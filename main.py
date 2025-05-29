import asyncio
import random
import pandas as pd
import io
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import requests

# Импорты для бота
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.filters import CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramForbiddenError
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

# Импорты для базы данных PostgreSQL
import asyncpg

# --- НАСТРОЙКИ ЛОГИРОВАНИЯ ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- НАСТРОЙКИ БОТА (Чтение из переменных окружения) ---
API_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not API_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN не установлен в переменных окружения. Бот не сможет запуститься.")
    exit(1)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL не установлен в переменных окружения. Бот не сможет подключиться к базе данных.")
    exit(1)

ADMIN_IDS_STR = os.environ.get("ADMIN_IDS")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',')] if ADMIN_IDS_STR else []
if not ADMIN_IDS:
    logger.warning("ADMIN_IDS не установлены или пусты. Некоторые функции админ-панели могут быть недоступны.")

REFERRAL_BONUS_PERCENT = float(os.environ.get("REFERRAL_BONUS_PERCENT", 0.05)) # Процент реферального бонуса (по умолчанию 5%)

# Инициализация бота с DefaultBotProperties для HTML-парсинга по умолчанию
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=types.ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# Глобальная переменная для режима технического обслуживания
maintenance_mode = False

# =====================
# ФЕЙКОВЫЙ ВЕБ-СЕРВЕР ДЛЯ RENDER.COM
# =====================
# Этот сервер нужен для того, чтобы Render не "усыплял" веб-сервис
# Если вы используете "Background Worker", этот код можно удалить
def run_fake_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'Bot is running!')

    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), Handler)
    logger.info(f"Fake web server running on 0.0.0.0:{port}")
    server.serve_forever()

# Запуск фейкового сервера в отдельном потоке
server_thread = threading.Thread(target=run_fake_server)
server_thread.daemon = True # Позволяет потоку завершиться при завершении основной программы
server_thread.start()


# =====================
# НАСТРОЙКИ БАЗЫ ДАННЫХ
# =====================

# Вспомогательная функция для получения соединения с БД
async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)

async def init_db():
    """Инициализирует таблицы базы данных, удаляя старые и создавая новые."""
    conn = None
    try:
        conn = await get_db_connection()
        logger.info("Подключение к базе данных установлено.")

        # --- УДАЛЕНИЕ СУЩЕСТВУЮЩИХ ТАБЛИЦ (для чистого старта) ---
        # ВНИМАНИЕ: Это приведет к ПОЛНОЙ потере всех данных в этих таблицах!
        # Удаляем в обратном порядке зависимостей
        await conn.execute('DROP TABLE IF EXISTS ref_bonuses_log CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS referral_bonuses CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS bonus_tasks CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS withdrawals CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS mine_sessions CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS investments CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS transactions CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS user_tasks CASCADE;') # Бывшая task_proofs
        await conn.execute('DROP TABLE IF EXISTS referrals CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS blocked_users CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS tasks CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS users CASCADE;')
        logger.info("Все существующие таблицы удалены (если были).")


        # --- СОЗДАНИЕ ТАБЛИЦ (В ПРАВИЛЬНОМ ПОРЯДКЕ ЗАВИСИМОСТЕЙ) ---

        # 1. Таблица 'users' (нет внешних ключей)
        await conn.execute('''
            CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                balance DECIMAL(10, 2) DEFAULT 0.00,
                registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                referrer_id BIGINT,
                status TEXT DEFAULT 'active',
                last_mine_time TIMESTAMP,
                referral_count INTEGER DEFAULT 0,
                mining_level INTEGER DEFAULT 1,
                mining_power DECIMAL(10, 2) DEFAULT 0.00,
                mining_profit_multiplier DECIMAL(10, 2) DEFAULT 1.00
            )
        ''')
        logger.info("Таблица 'users' создана.")

        # 2. Таблица 'tasks' (нет внешних ключей)
        await conn.execute('''
            CREATE TABLE tasks (
                id SERIAL PRIMARY KEY,
                description TEXT NOT NULL,
                reward DECIMAL(10, 2) NOT NULL,
                max_performers INTEGER,
                current_performers INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                photo_file_id TEXT
            )
        ''')
        logger.info("Таблица 'tasks' создана.")

        # 3. Таблица 'referrals' (зависит от users)
        await conn.execute('''
            CREATE TABLE referrals (
                referrer_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                referred_user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE
            )
        ''')
        logger.info("Таблица 'referrals' создана.")

        # 4. Таблица 'blocked_users' (зависит от users)
        await conn.execute('''
            CREATE TABLE blocked_users (
                user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE
            )
        ''')
        logger.info("Таблица 'blocked_users' создана.")

        # 5. Таблица 'user_tasks' (зависит от users и tasks)
        await conn.execute('''
            CREATE TABLE user_tasks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'completed', 'rejected'
                submission_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                proof_photo_file_id TEXT,
                UNIQUE(user_id, task_id)
            )
        ''')
        logger.info("Таблица 'user_tasks' создана.")

        # 6. Таблица 'transactions' (зависит от users и tasks)
        await conn.execute('''
            CREATE TABLE transactions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                type TEXT NOT NULL, -- 'deposit', 'withdrawal', 'task_reward', 'referral_bonus', 'mining_reward', 'investment_profit'
                amount DECIMAL(10, 2) NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                task_id INTEGER REFERENCES tasks(id) ON DELETE SET NULL, -- Для привязки к задаче
                description TEXT
            )
        ''')
        logger.info("Таблица 'transactions' создана.")

        # 7. Таблица 'investments' (зависит от users)
        await conn.execute('''
            CREATE TABLE investments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                amount DECIMAL(10, 2) NOT NULL,
                investment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_date TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'active', -- 'active', 'completed', 'cancelled'
                daily_profit_percent DECIMAL(5, 2) NOT NULL,
                total_profit_accrued DECIMAL(10, 2) DEFAULT 0.00,
                last_profit_accrual TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logger.info("Таблица 'investments' создана.")

        # 8. Таблица 'mine_sessions' (зависит от users)
        await conn.execute('''
            CREATE TABLE mine_sessions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                mined_amount DECIMAL(10, 2) DEFAULT 0.00,
                is_active BOOLEAN DEFAULT TRUE
            )
        ''')
        logger.info("Таблица 'mine_sessions' создана.")

        # 9. Таблица 'withdrawals' (зависит от users)
        await conn.execute('''
            CREATE TABLE withdrawals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                amount DECIMAL(10, 2) NOT NULL,
                currency TEXT NOT NULL,
                wallet TEXT NOT NULL,
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'pending' -- 'pending', 'approved', 'rejected'
            )
        ''')
        logger.info("Таблица 'withdrawals' создана.")

        # 10. Таблица 'bonus_tasks' (нет внешних ключей)
        await conn.execute('''
            CREATE TABLE bonus_tasks (
                id SERIAL PRIMARY KEY,
                task_name TEXT NOT NULL,
                reward_amount DECIMAL(10, 2) NOT NULL,
                status TEXT DEFAULT 'active'
            )
        ''')
        logger.info("Таблица 'bonus_tasks' создана.")

        # 11. Таблица 'referral_bonuses' (нет внешних ключей)
        await conn.execute('''
            CREATE TABLE referral_bonuses (
                id SERIAL PRIMARY KEY,
                level INTEGER NOT NULL UNIQUE,
                bonus_percent DECIMAL(5, 2) NOT NULL
            )
        ''')
        logger.info("Таблица 'referral_bonuses' создана.")

        # 12. Таблица 'ref_bonuses_log' (зависит от users и tasks)
        await conn.execute('''
            CREATE TABLE ref_bonuses_log (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                task_id INTEGER REFERENCES tasks(id) ON DELETE SET NULL,
                bonus_amount DECIMAL(10, 2) NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logger.info("Таблица 'ref_bonuses_log' создана.")


        logger.info("Все таблицы базы данных успешно инициализированы.")
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}", exc_info=True)
        # Важно: если инициализация БД не удалась, бот не может работать
        exit(1)
    finally:
        if conn:
            await conn.close()


# =====================
# ФУНКЦИИ ВЗАИМОДЕЙСТВИЯ С БД
# =====================

# Базовые функции для работы с данными
async def get_user_data(user_id):
    conn = await get_db_connection()
    try:
        user = await conn.fetchrow('SELECT * FROM users WHERE id = $1', user_id)
        return user
    finally:
        await conn.close()

async def create_user(user_id, username, full_name, referrer_id=None):
    conn = await get_db_connection()
    try:
        user_exists = await conn.fetchval('SELECT id FROM users WHERE id = $1', user_id)
        if not user_exists:
            await conn.execute('''
                INSERT INTO users (id, username, full_name, referrer_id)
                VALUES ($1, $2, $3, $4)
            ''', user_id, username, full_name, referrer_id)
            logger.info(f"Пользователь {user_id} создан.")

            if referrer_id:
                # Обновить счетчик рефералов у реферера
                await conn.execute('UPDATE users SET referral_count = referral_count + 1 WHERE id = $1', referrer_id)
                # Добавить запись в таблицу referrals
                await conn.execute('''
                    INSERT INTO referrals (referrer_id, referred_user_id)
                    VALUES ($1, $2) ON CONFLICT (referred_user_id) DO NOTHING
                ''', referrer_id, user_id)
                logger.info(f"Реферальная связь: {referrer_id} -> {user_id} добавлена.")
        else:
            # Обновляем username и full_name, если пользователь уже существует
            await conn.execute('''
                UPDATE users SET username = $1, full_name = $2 WHERE id = $3
            ''', username, full_name, user_id)
            logger.info(f"Пользователь {user_id} обновлен (username/full_name).")

    finally:
        await conn.close()

async def update_user_balance(user_id, amount):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE users SET balance = balance + $1 WHERE id = $2
        ''', amount, user_id)
        logger.info(f"Баланс пользователя {user_id} обновлен на {amount}.")
    finally:
        await conn.close()

async def add_transaction(user_id, type, amount, description=None, task_id=None):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            INSERT INTO transactions (user_id, type, amount, description, task_id)
            VALUES ($1, $2, $3, $4, $5)
        ''', user_id, type, amount, description, task_id)
        logger.info(f"Добавлена транзакция для пользователя {user_id}: тип {type}, сумма {amount}.")
    finally:
        await conn.close()

async def get_all_active_tasks():
    conn = await get_db_connection()
    try:
        tasks = await conn.fetch('SELECT * FROM tasks WHERE status = $1', 'active')
        return tasks
    finally:
        await conn.close()

async def get_task_by_id(task_id):
    conn = await get_db_connection()
    try:
        task = await conn.fetchrow('SELECT * FROM tasks WHERE id = $1', task_id)
        return task
    finally:
        await conn.close()

async def add_task(description, reward, max_performers=None, photo_file_id=None):
    conn = await get_db_connection()
    try:
        result = await conn.fetchrow('''
            INSERT INTO tasks (description, reward, max_performers, photo_file_id)
            VALUES ($1, $2, $3, $4) RETURNING id
        ''', description, reward, max_performers, photo_file_id)
        logger.info(f"Задача '{description}' добавлена с ID {result['id']}.")
        return result['id']
    finally:
        await conn.close()

async def increment_task_performers(task_id):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE tasks SET current_performers = current_performers + 1 WHERE id = $1
        ''', task_id)
        logger.info(f"Счетчик исполнителей задачи {task_id} увеличен.")
    finally:
        await conn.close()

async def set_task_status(task_id, status):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE tasks SET status = $1 WHERE id = $2
        ''', status, task_id)
        logger.info(f"Статус задачи {task_id} изменен на '{status}'.")
    finally:
        await conn.close()

async def get_user_task_status(user_id, task_id):
    conn = await get_db_connection()
    try:
        status = await conn.fetchval('SELECT status FROM user_tasks WHERE user_id = $1 AND task_id = $2', user_id, task_id)
        return status
    finally:
        await conn.close()

async def add_user_task_entry(user_id, task_id, status='pending', proof_photo_file_id=None):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            INSERT INTO user_tasks (user_id, task_id, status, proof_photo_file_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, task_id) DO UPDATE SET status = EXCLUDED.status, submission_time = CURRENT_TIMESTAMP, proof_photo_file_id = EXCLUDED.proof_photo_file_id
        ''', user_id, task_id, status, proof_photo_file_id)
        logger.info(f"Запись user_task для пользователя {user_id}, задачи {task_id} добавлена/обновлена со статусом '{status}'.")
    finally:
        await conn.close()

async def get_pending_user_tasks():
    conn = await get_db_connection()
    try:
        pending_tasks = await conn.fetch('''
            SELECT ut.id, ut.user_id, ut.task_id, ut.proof_photo_file_id, u.username, t.description
            FROM user_tasks ut
            JOIN users u ON ut.user_id = u.id
            JOIN tasks t ON ut.task_id = t.id
            WHERE ut.status = 'pending'
        ''')
        return pending_tasks
    finally:
        await conn.close()

async def update_user_task_status(user_task_id, status):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE user_tasks SET status = $1 WHERE id = $2
        ''', status, user_task_id)
        logger.info(f"Статус user_task {user_task_id} обновлен на '{status}'.")
    finally:
        await conn.close()

async def get_user_task_id_by_user_task(user_id, task_id):
    conn = await get_db_connection()
    try:
        ut_id = await conn.fetchval('SELECT id FROM user_tasks WHERE user_id = $1 AND task_id = $2 ORDER BY submission_time DESC LIMIT 1', user_id, task_id)
        return ut_id
    finally:
        await conn.close()

async def get_all_users_count():
    conn = await get_db_connection()
    try:
        count = await conn.fetchval('SELECT COUNT(*) FROM users')
        return count
    finally:
        await conn.close()

async def get_all_users_data():
    conn = await get_db_connection()
    try:
        users = await conn.fetch('SELECT * FROM users')
        return users
    finally:
        await conn.close()

async def get_total_balance():
    conn = await get_db_connection()
    try:
        total_balance = await conn.fetchval('SELECT SUM(balance) FROM users')
        return total_balance if total_balance else 0.00
    finally:
        await conn.close()

# Функции для бонусных заданий
async def add_bonus_task(task_name, reward_amount):
    conn = await get_db_connection()
    try:
        result = await conn.fetchrow('''
            INSERT INTO bonus_tasks (task_name, reward_amount)
            VALUES ($1, $2) RETURNING id
        ''', task_name, reward_amount)
        logger.info(f"Бонусная задача '{task_name}' добавлена с ID {result['id']}.")
        return result['id']
    finally:
        await conn.close()

async def set_bonus_task_status(bonus_task_id, status):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE bonus_tasks SET status = $1 WHERE id = $2
        ''', status, bonus_task_id)
        logger.info(f"Статус бонусной задачи {bonus_task_id} изменен на '{status}'.")
    finally:
        await conn.close()

async def get_bonus_task_by_id(bonus_task_id):
    conn = await get_db_connection()
    try:
        task = await conn.fetchrow('SELECT * FROM bonus_tasks WHERE id = $1', bonus_task_id)
        return task
    finally:
        await conn.close()

async def get_all_bonus_tasks():
    conn = await get_db_connection()
    try:
        tasks = await conn.fetch('SELECT * FROM bonus_tasks')
        return tasks
    finally:
        await conn.close()

async def get_pending_bonus_tasks():
    conn = await get_db_connection()
    try:
        tasks = await conn.fetch('SELECT * FROM bonus_tasks WHERE status = $1', 'pending_approval')
        return tasks
    finally:
        await conn.close()

async def get_user_completed_tasks(user_id):
    conn = await get_db_connection()
    try:
        completed_count = await conn.fetchval('SELECT COUNT(*) FROM user_tasks WHERE user_id = $1 AND status = \'completed\'', user_id)
        return completed_count
    finally:
        await conn.close()

# Функции для инвестиций
async def add_user_investment(user_id, amount, daily_profit_percent, end_date):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            INSERT INTO investments (user_id, amount, daily_profit_percent, end_date)
            VALUES ($1, $2, $3, $4)
        ''', user_id, amount, daily_profit_percent, end_date)
        logger.info(f"Инвестиция на {amount} RUB добавлена для пользователя {user_id}.")
    finally:
        await conn.close()

async def update_investment_status(investment_id, status):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE investments SET status = $1 WHERE id = $2
        ''', status, investment_id)
        logger.info(f"Статус инвестиции {investment_id} обновлен на '{status}'.")
    finally:
        await conn.close()

async def get_active_investments_for_user(user_id):
    conn = await get_db_connection()
    try:
        investments = await conn.fetch('SELECT * FROM investments WHERE user_id = $1 AND status = \'active\' AND end_date > CURRENT_TIMESTAMP', user_id)
        return investments
    finally:
        await conn.close()

async def get_all_active_investments():
    conn = await get_db_connection()
    try:
        investments = await conn.fetch('SELECT * FROM investments WHERE status = \'active\' AND end_date > CURRENT_TIMESTAMP')
        return investments
    finally:
        await conn.close()

async def update_investment_profit(investment_id, profit_amount):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE investments SET total_profit_accrued = total_profit_accrued + $1, last_profit_accrual = CURRENT_TIMESTAMP WHERE id = $2
        ''', profit_amount, investment_id)
        logger.info(f"Прибыль по инвестиции {investment_id} обновлена на {profit_amount}.")
    finally:
        await conn.close()

# Функции для майнинга
async def mine_ore(user_id, mine_amount):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE users SET balance = balance + $1 WHERE id = $2
        ''', mine_amount, user_id)
        await conn.execute('''
            UPDATE users SET last_mine_time = CURRENT_TIMESTAMP WHERE id = $1
        ''', user_id)
        await add_transaction(user_id, 'mining_reward', mine_amount, "Награда за майнинг")
        logger.info(f"Пользователь {user_id} намайнил {mine_amount} RUB.")
    finally:
        await conn.close()

async def start_mine_session(user_id):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            INSERT INTO mine_sessions (user_id, start_time)
            VALUES ($1, CURRENT_TIMESTAMP)
        ''', user_id)
        await conn.execute('UPDATE users SET last_mine_time = CURRENT_TIMESTAMP WHERE id = $1', user_id)
        logger.info(f"Начата майнинг-сессия для пользователя {user_id}.")
    finally:
        await conn.close()

async def end_mine_session(user_id, session_id, mined_amount):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE mine_sessions SET end_time = CURRENT_TIMESTAMP, mined_amount = $1, is_active = FALSE WHERE id = $2
        ''', mined_amount, session_id)
        logger.info(f"Завершена майнинг-сессия {session_id} для пользователя {user_id} с намайненной суммой {mined_amount}.")
    finally:
        await conn.close()

async def get_active_mine_session(user_id):
    conn = await get_db_connection()
    try:
        session = await conn.fetchrow('SELECT * FROM mine_sessions WHERE user_id = $1 AND is_active = TRUE', user_id)
        return session
    finally:
        await conn.close()

# Функции для вывода средств
async def add_withdrawal_request(user_id, amount, currency, wallet):
    conn = await get_db_connection()
    try:
        result = await conn.fetchrow('''
            INSERT INTO withdrawals (user_id, amount, currency, wallet)
            VALUES ($1, $2, $3, $4) RETURNING id
        ''', user_id, amount, currency, wallet)
        logger.info(f"Запрос на вывод {amount} {currency} на {wallet} от пользователя {user_id} добавлен. ID запроса: {result['id']}.")
        return result['id']
    finally:
        await conn.close()

async def get_pending_withdrawal_requests():
    conn = await get_db_connection()
    try:
        requests = await conn.fetch('SELECT * FROM withdrawals WHERE status = \'pending\'')
        return requests
    finally:
        await conn.close()

async def update_withdrawal_status(withdrawal_id, status):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE withdrawals SET status = $1 WHERE id = $2
        ''', status, withdrawal_id)
        logger.info(f"Статус запроса на вывод {withdrawal_id} обновлен на '{status}'.")
    finally:
        await conn.close()

async def get_withdrawal_request_by_id(withdrawal_id):
    conn = await get_db_connection()
    try:
        request = await conn.fetchrow('SELECT * FROM withdrawals WHERE id = $1', withdrawal_id)
        return request
    finally:
        await conn.close()


# Функции для реферальных бонусов
async def add_referral_bonus_setting(level, bonus_percent):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            INSERT INTO referral_bonuses (level, bonus_percent)
            VALUES ($1, $2) ON CONFLICT (level) DO UPDATE SET bonus_percent = EXCLUDED.bonus_percent
        ''', level, bonus_percent)
        logger.info(f"Настройка реферального бонуса для уровня {level} установлена на {bonus_percent}%.")
    finally:
        await conn.close()

async def get_referral_bonus_setting(level):
    conn = await get_db_connection()
    try:
        setting = await conn.fetchval('SELECT bonus_percent FROM referral_bonuses WHERE level = $1', level)
        return setting
    finally:
        await conn.close()

async def log_referral_bonus(user_id, task_id, bonus_amount):
    conn = await get_db_connection()
    try:
        await conn.execute('''
            INSERT INTO ref_bonuses_log (user_id, task_id, bonus_amount)
            VALUES ($1, $2, $3)
        ''', user_id, task_id, bonus_amount)
        logger.info(f"Залогирован реферальный бонус {bonus_amount} для пользователя {user_id} за задачу {task_id}.")
    finally:
        await conn.close()

async def get_all_referrals(referrer_id):
    conn = await get_db_connection()
    try:
        referrals = await conn.fetch('SELECT * FROM referrals WHERE referrer_id = $1', referrer_id)
        return referrals
    finally:
        await conn.close()

async def get_sum_referrals_bonus(user_id):
    conn = await get_db_connection()
    try:
        total_bonus = await conn.fetchval('SELECT SUM(bonus_amount) FROM ref_bonuses_log WHERE user_id = $1', user_id)
        return total_bonus if total_bonus else 0.00
    finally:
        await conn.close()

async def get_all_ref_bonuses_log():
    conn = await get_db_connection()
    try:
        log = await conn.fetch('SELECT * FROM ref_bonuses_log')
        return log
    finally:
        await conn.close()

async def get_ref_bonus_log_by_user_task_id(user_task_id):
    # Эта функция возможно требует доработки, если task_id в log - это task.id, а не user_task.id
    # Предполагаем, что ref_bonuses_log.task_id ссылается на tasks.id, а не user_tasks.id
    # Если нужно связать с user_task_id, то нужна будет другая логика или дополнительное поле в ref_bonuses_log
    conn = await get_db_connection()
    try:
        log_entry = await conn.fetchrow('SELECT * FROM ref_bonuses_log WHERE task_id = (SELECT task_id FROM user_tasks WHERE id = $1) LIMIT 1', user_task_id)
        return log_entry
    finally:
        await conn.close()

async def get_active_referral_bonus(user_id):
    conn = await get_db_connection()
    try:
        # Здесь логика зависит от того, как вы определяете "активный" реферальный бонус
        # Возможно, это просто получение текущего процента для пользователя
        # Или, если у вас несколько уровней, получить уровень пользователя и соответствующий бонус
        # Пока вернем константу или настройку
        return await get_referral_bonus_setting(1) # Пример: берем бонус для 1-го уровня
    finally:
        await conn.close()

# =====================
# КЛАВИАТУРЫ
# =====================

def get_main_kb():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="🚀 Задачи")],
            [KeyboardButton(text="🤝 Рефералы"), KeyboardButton(text="⛏️ Майнинг")],
            [KeyboardButton(text="📈 Инвестиции"), KeyboardButton(text="💼 Вывод")],
            [KeyboardButton(text="🎁 Бонусы"), KeyboardButton(text="❓ Помощь")],
            [KeyboardButton(text="⚙️ Админ-панель")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )
    return kb

def get_admin_kb():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить задачу"), KeyboardButton(text="📄 Проверить задачи")],
            [KeyboardButton(text="✏️ Изменить задачу"), KeyboardButton(text="🗑️ Удалить задачу")],
            [KeyboardButton(text="💰 Изменить баланс")],
            [KeyboardButton(text="📈 Статистика"), KeyboardButton(text="📢 Рассылка")],
            [KeyboardButton(text="📤 Вывод средств"), KeyboardButton(text="➕ Бонусная задача")],
            [KeyboardButton(text="🔧 Техперерыв Вкл"), KeyboardButton(text="🔧 Техперерыв Выкл")],
            [KeyboardButton(text="⬅️ В главное меню")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие в админ-панели..."
    )
    return kb

def get_back_to_main_kb():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⬅️ В главное меню")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )
    return kb

def get_task_action_kb(task_id):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Выполнить", callback_data=f"perform_task_{task_id}")
    builder.button(text="⬅️ Назад к задачам", callback_data="show_tasks")
    return builder.as_markup()

def get_approve_decline_kb(user_task_id):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Одобрить", callback_data=f"approve_ut_{user_task_id}")
    builder.button(text="❌ Отклонить", callback_data=f"decline_ut_{user_task_id}")
    return builder.as_markup()

def get_withdrawal_action_kb(withdrawal_id):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить", callback_data=f"confirm_withdrawal_{withdrawal_id}")
    builder.button(text="❌ Отклонить", callback_data=f"reject_withdrawal_{withdrawal_id}")
    return builder.as_markup()

def get_investment_menu_kb():
    builder = InlineKeyboardBuilder()
    builder.button(text="💸 Мои инвестиции", callback_data="my_investments")
    builder.button(text="📈 Инвестировать", callback_data="invest_now")
    return builder.as_markup()

def get_mining_menu_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Начать майнинг", callback_data="start_mining")],
        [InlineKeyboardButton(text="⏳ Завершить майнинг", callback_data="end_mining")],
        [InlineKeyboardButton(text="⬅️ В главное меню", callback_data="main_menu")]
    ])
    return kb

def get_mine_more_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Продолжить майнинг", callback_data="start_mining")],
        [InlineKeyboardButton(text="⬅️ В главное меню", callback_data="main_menu")]
    ])
    return kb

def get_confirm_mine_reset_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, начать новый", callback_data="confirm_new_mine")],
        [InlineKeyboardButton(text="Нет, отмена", callback_data="cancel_mine_reset")]
    ])
    return kb

def get_confirm_mining_start_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, начать", callback_data="start_mining_confirmed")],
        [InlineKeyboardButton(text="Нет, отмена", callback_data="cancel_mining_start")]
    ])
    return kb


# =====================
# СОСТОЯНИЯ FSM
# =====================

class TaskStates(StatesGroup):
    waiting_for_description = State()
    waiting_for_reward = State()
    waiting_for_max_performers = State()
    waiting_for_task_photo = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast_message = State()
    waiting_for_task_to_edit_id = State()
    waiting_for_new_task_description = State()
    waiting_for_new_task_reward = State()
    waiting_for_new_task_max_performers = State()
    waiting_for_new_task_photo = State()
    waiting_for_task_to_delete_id = State()
    waiting_for_user_balance_change_id = State()
    waiting_for_balance_change_amount = State()
    waiting_for_bonus_task_name = State()
    waiting_for_bonus_task_reward = State()

class WithdrawStates(StatesGroup):
    waiting_for_amount = State()
    waiting_for_currency = State()
    waiting_for_wallet = State()
    waiting_for_withdrawal_confirmation = State()

class InvestStates(StatesGroup):
    waiting_for_invest_amount = State()
    waiting_for_invest_duration = State() # В днях, например
    waiting_for_invest_percent = State() # Ежедневный процент

# =====================
# ХЕНДЛЕРЫ
# =====================

# Декоратор для обработки ошибок
def error_handler_decorator(func):
    async def wrapper(message_or_callback, *args, **kwargs):
        try:
            return await func(message_or_callback, *args, **kwargs)
        except Exception as e:
            user_id = message_or_callback.from_user.id
            logger.error(f"Ошибка в хендлере для пользователя {user_id}: {e}", exc_info=True)
            if isinstance(message_or_callback, types.Message):
                await message_or_callback.answer("Произошла внутренняя ошибка. Мы уже работаем над её устранением. Попробуйте позже.")
            elif isinstance(message_or_callback, types.CallbackQuery):
                await message_or_callback.answer("Произошла внутренняя ошибка. Попробуйте позже.", show_alert=True)
            # Для админов можно отправить более подробное сообщение
            if user_id in ADMIN_IDS:
                admin_error_message = (
                    f"**⚠️ Ошибка в боте!**\n\n"
                    f"Функция: `{func.__name__}`\n"
                    f"Пользователь: `{user_id}`\n"
                    f"Ошибка: `{type(e).__name__}: {e}`\n\n"
                    f"Пожалуйста, проверьте логи Render/сервера."
                )
                for admin_id in ADMIN_IDS:
                    if admin_id != user_id: # Не отправлять тому, кто уже получил ошибку
                        try:
                            await bot.send_message(admin_id, admin_error_message)
                        except TelegramForbiddenError:
                            logger.warning(f"Не удалось отправить уведомление администратору {admin_id}: бот заблокирован.")
    return wrapper


# Хендлер на команду /start
@dp.message(CommandStart())
@error_handler_decorator
async def handle_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    full_name = message.from_user.full_name

    referrer_id = None
    if message.text:
        command_obj = CommandStart().parse_args(message.text)
        if command_obj and command_obj.args:
            try:
                referrer_id = int(command_obj.args)
                # Проверить существование реферера и не быть реферером для самого себя
                if not await get_user_data(referrer_id) or referrer_id == user_id:
                    referrer_id = None
            except ValueError:
                referrer_id = None

    await create_user(user_id, username, full_name, referrer_id)

    # Приветственное сообщение
    welcome_message = f"Привет, {full_name}! 👋\n\nЯ твой бот для заработка и выполнения заданий. Здесь ты можешь:\n\n" \
                      "💰 Зарабатывать, выполняя простые задания.\n" \
                      "🤝 Приглашать друзей и получать бонусы.\n" \
                      "📊 Отслеживать свой баланс.\n\n" \
                      "Начнем!"
    await message.answer(welcome_message, reply_markup=get_main_kb())
    logger.info(f"Пользователь {user_id} ({full_name}) начал взаимодействие с ботом. Реферер: {referrer_id}")

# Хендлер на кнопку "⬅️ В главное меню"
@dp.message(F.text == "⬅️ В главное меню")
@error_handler_decorator
async def back_to_main_menu(message: types.Message, state: FSMContext):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    await state.clear() # Очистить текущее состояние FSM
    await message.answer("Вы вернулись в главное меню.", reply_markup=get_main_kb())
    logger.info(f"Пользователь {message.from_user.id} вернулся в главное меню.")


# Хендлер на кнопку "💰 Баланс"
@dp.message(F.text == "💰 Баланс")
@error_handler_decorator
async def show_balance(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user = await get_user_data(message.from_user.id)
    if user:
        balance_message = f"Ваш текущий баланс: **{user['balance']:.2f} RUB**"
    else:
        balance_message = "Пожалуйста, начните взаимодействие с ботом с команды /start."
    await message.answer(balance_message, reply_markup=get_main_kb())
    logger.info(f"Пользователь {message.from_user.id} запросил баланс.")

# Хендлер на кнопку "🚀 Задачи"
@dp.message(F.text == "🚀 Задачи")
@error_handler_decorator
async def show_tasks(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    tasks = await get_all_active_tasks()

    available_tasks_found = False
    for task in tasks:
        # Проверить, выполнил ли пользователь уже эту задачу
        user_task_status = await get_user_task_status(message.from_user.id, task['id'])
        if user_task_status in ['pending', 'completed']:
            continue # Пропустить, если уже выполняется или выполнена

        # Проверить, не превышен ли лимит исполнителей
        if task['max_performers'] is not None and task['current_performers'] >= task['max_performers']:
            continue # Пропустить, если лимит достигнут

        task_text = (
            f"**Задача ID: {task['id']}**\n"
            f"Описание: {task['description']}\n"
            f"Вознаграждение: {task['reward']:.2f} RUB\n"
            f"Исполнителей: {task['current_performers']}/{task['max_performers'] if task['max_performers'] else '∞'}\n\n"
        )
        if task['photo_file_id']:
            try:
                await bot.send_photo(message.chat.id, photo=task['photo_file_id'], caption=task_text, reply_markup=get_task_action_kb(task['id']))
            except Exception as e:
                logger.error(f"Не удалось отправить фото для задачи {task['id']}: {e}", exc_info=True)
                await message.answer(task_text, reply_markup=get_task_action_kb(task['id']))
        else:
            await message.answer(task_text, reply_markup=get_task_action_kb(task['id']))
        
        logger.info(f"Пользователь {message.from_user.id} увидел задачу {task['id']}.")
        available_tasks_found = True

    if not available_tasks_found:
         await message.answer("На данный момент доступных задач нет, которые вы еще не выполняли или лимит исполнителей которых не достигнут. Попробуйте позже.", reply_markup=get_main_kb())


# Обработка колбэка для выполнения задачи
@dp.callback_query(F.data.startswith("perform_task_"))
@error_handler_decorator
async def process_perform_task(callback_query: types.CallbackQuery):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    task_id = int(callback_query.data.split("_")[2])
    user_id = callback_query.from_user.id

    # Проверить, существует ли задача и активна ли она
    task = await get_task_by_id(task_id)
    if not task or task['status'] != 'active':
        await callback_query.answer("Эта задача больше недоступна.", show_alert=True)
        await callback_query.message.delete_reply_markup() # Удалить только кнопки
        return

    # Проверить, выполнил ли пользователь уже эту задачу
    user_task_status = await get_user_task_status(user_id, task_id)
    if user_task_status == 'pending':
        await callback_query.answer("Вы уже выполняете эту задачу. Дождитесь проверки.", show_alert=True)
        return
    elif user_task_status == 'completed':
        await callback_query.answer("Вы уже выполнили эту задачу.", show_alert=True)
        return

    # Проверка лимита исполнителей перед принятием задачи
    if task['max_performers'] is not None and task['current_performers'] >= task['max_performers']:
        await callback_query.answer("Лимит исполнителей для этой задачи достигнут.", show_alert=True)
        await callback_query.message.delete_reply_markup() # Удалить только кнопки
        return

    # Добавить запись о начале выполнения задачи
    await add_user_task_entry(user_id, task_id, status='pending')
    await callback_query.answer("Пришлите фото/скриншот подтверждения выполнения задачи.", show_alert=False)
    await callback_query.message.answer("Теперь отправьте мне фотографию или скриншот, подтверждающий выполнение задачи.", reply_markup=get_back_to_main_kb())
    await dp.fsm.get_context(user_id).set_state(TaskStates.waiting_for_task_photo)
    await dp.fsm.get_context(user_id).update_data(current_task_id=task_id)
    logger.info(f"Пользователь {user_id} начал выполнять задачу {task_id}.")


# Хендлер для получения фото подтверждения выполнения задачи
@dp.message(TaskStates.waiting_for_task_photo, F.photo)
@error_handler_decorator
async def process_task_proof_photo(message: types.Message, state: FSMContext):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    data = await state.get_data()
    task_id = data.get("current_task_id")
    user_id = message.from_user.id
    photo_file_id = message.photo[-1].file_id # Получаем ID самого большого фото

    user_task_id = await get_user_task_id_by_user_task(user_id, task_id)
    if user_task_id:
        # Обновляем запись о выполнении задачи, добавляя photo_file_id
        await add_user_task_entry(user_id, task_id, status='pending', proof_photo_file_id=photo_file_id)
        await message.answer("Спасибо! Ваше подтверждение отправлено на проверку администраторам. Как только оно будет одобрено, вы получите вознаграждение.", reply_markup=get_main_kb())
        
        # Уведомить админов
        for admin_id in ADMIN_IDS:
            try:
                task = await get_task_by_id(task_id)
                user = await get_user_data(user_id)
                admin_notification_text = (
                    f"🔔 **НОВАЯ ЗАЯВКА НА ПРОВЕРКУ ЗАДАЧИ!**\n\n"
                    f"Пользователь: [{user.get('full_name', 'N/A')}]({message.from_user.url})\n"
                    f"ID пользователя: `{user_id}`\n"
                    f"Username: @{user.get('username', 'N/A')}\n"
                    f"Задача: `{task.get('description', 'N/A')}` (ID: `{task_id}`)\n"
                    f"Вознаграждение: `{task.get('reward', 0):.2f} RUB`"
                )
                await bot.send_photo(admin_id, photo=photo_file_id, caption=admin_notification_text, reply_markup=get_approve_decline_kb(user_task_id))
                logger.info(f"Админу {admin_id} отправлено уведомление о задаче {user_task_id}.")
            except TelegramForbiddenError:
                logger.warning(f"Не удалось отправить уведомление администратору {admin_id}: бот заблокирован.")
            except Exception as e:
                logger.error(f"Ошибка при отправке уведомления админу о задаче {user_task_id}: {e}", exc_info=True)
    else:
        await message.answer("Что-то пошло не так. Пожалуйста, попробуйте снова начать выполнение задачи.", reply_markup=get_main_kb())
    
    await state.clear()
    logger.info(f"Пользователь {user_id} отправил фото для задачи {task_id}.")


# Хендлер, если отправлено что-то другое вместо фото для подтверждения задачи
@dp.message(TaskStates.waiting_for_task_photo, ~F.photo)
@error_handler_decorator
async def process_task_proof_wrong_type(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    await message.answer("Пожалуйста, отправьте именно **фотографию** или **скриншот** подтверждения. Если вы хотите отменить, нажмите '⬅️ В главное меню'.")
    logger.warning(f"Пользователь {message.from_user.id} отправил не фото, находясь в состоянии ожидания фото подтверждения.")


# Хендлер на кнопку "🤝 Рефералы"
@dp.message(F.text == "🤝 Рефералы")
@error_handler_decorator
async def show_referrals(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user_id = message.from_user.id
    user = await get_user_data(user_id)
    if not user:
        await message.answer("Пожалуйста, начните взаимодействие с ботом с команды /start.")
        return

    referral_link = f"https://t.me/{bot.me.username}?start={user_id}"
    referral_count = user['referral_count']
    total_referral_bonus = await get_sum_referrals_bonus(user_id)
    
    referral_message = (
        f"**Ваша реферальная ссылка:**\n`{referral_link}`\n\n"
        f"👥 **Количество рефералов:** {referral_count}\n"
        f"💰 **Общий реферальный бонус:** {total_referral_bonus:.2f} RUB\n\n"
        "Приглашайте друзей и получайте процент от их заработка на заданиях!"
    )
    await message.answer(referral_message, reply_markup=get_main_kb())
    logger.info(f"Пользователь {user_id} запросил информацию о рефералах.")


# Хендлер на кнопку "⛏️ Майнинг"
@dp.message(F.text == "⛏️ Майнинг")
@error_handler_decorator
async def show_mining_menu(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user = await get_user_data(message.from_user.id)
    if not user:
        await message.answer("Пожалуйста, начните взаимодействие с ботом с команды /start.")
        return

    mining_message = (
        f"**Ваш уровень майнинга:** {user['mining_level']}\n"
        f"**Мощность майнинга:** {user['mining_power']:.2f} RUB/час\n"
        f"**Множитель прибыли:** x{user['mining_profit_multiplier']:.2f}\n\n"
        "Нажмите 'Начать майнинг', чтобы начать зарабатывать пассивный доход."
    )
    
    active_session = await get_active_mine_session(message.from_user.id)
    if active_session:
        start_time_utc = active_session['start_time'].astimezone(timezone.utc)
        current_time_utc = datetime.now(timezone.utc)
        elapsed_time = current_time_utc - start_time_utc
        
        hours_elapsed = elapsed_time.total_seconds() / 3600
        potential_mined_amount = user['mining_power'] * user['mining_profit_multiplier'] * hours_elapsed
        
        mining_message += (
            f"\n\n⚡ **Активная сессия майнинга:**\n"
            f"Начало: {active_session['start_time'].strftime('%Y-%m-%d %H:%M:%S')} (UTC)\n"
            f"Предполагаемый доход: {potential_mined_amount:.2f} RUB"
        )
        await message.answer(mining_message, reply_markup=get_mining_menu_kb())
    else:
        await message.answer(mining_message, reply_markup=get_mining_menu_kb())
    logger.info(f"Пользователь {message.from_user.id} открыл меню майнинга.")


@dp.callback_query(F.data == "start_mining")
@error_handler_decorator
async def start_mining_callback(callback_query: types.CallbackQuery):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user_id = callback_query.from_user.id
    user = await get_user_data(user_id)

    if not user:
        await callback_query.answer("Пожалуйста, начните взаимодействие с ботом с команды /start.", show_alert=True)
        return

    active_session = await get_active_mine_session(user_id)
    if active_session:
        await callback_query.answer("У вас уже активна сессия майнинга. Хотите завершить текущую и начать новую?", show_alert=True, reply_markup=get_confirm_mine_reset_kb())
    else:
        await start_mine_session(user_id)
        await callback_query.message.edit_text("⚡ Майнинг начат! Вы будете получать пассивный доход.", reply_markup=get_mining_menu_kb())
        await callback_query.answer()
        logger.info(f"Пользователь {user_id} начал майнинг.")

@dp.callback_query(F.data == "confirm_new_mine")
@error_handler_decorator
async def confirm_new_mine_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    active_session = await get_active_mine_session(user_id)
    if active_session:
        # Завершаем текущую сессию
        start_time_utc = active_session['start_time'].astimezone(timezone.utc)
        current_time_utc = datetime.now(timezone.utc)
        elapsed_time = current_time_utc - start_time_utc
        user = await get_user_data(user_id)
        
        hours_elapsed = elapsed_time.total_seconds() / 3600
        mined_amount = user['mining_power'] * user['mining_profit_multiplier'] * hours_elapsed
        
        await end_mine_session(user_id, active_session['id'], mined_amount)
        await update_user_balance(user_id, mined_amount)
        await add_transaction(user_id, 'mining_reward', mined_amount, f"Завершение сессии майнинга. Продолжительность: {hours_elapsed:.2f} ч.")
        await callback_query.answer(f"Предыдущая сессия завершена. Намайнено: {mined_amount:.2f} RUB.", show_alert=True)

    # Начинаем новую
    await start_mine_session(user_id)
    await callback_query.message.edit_text("⚡ Майнинг начат! Вы будете получать пассивный доход.", reply_markup=get_mining_menu_kb())
    await callback_query.answer()
    logger.info(f"Пользователь {user_id} завершил старую и начал новую майнинг-сессию.")

@dp.callback_query(F.data == "cancel_mine_reset")
@error_handler_decorator
async def cancel_mine_reset_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Отменено. Ваша текущая сессия майнинга продолжается.", show_alert=False)
    await callback_query.message.delete_reply_markup()
    await show_mining_menu(callback_query.message) # Обновить меню майнинга
    logger.info(f"Пользователь {callback_query.from_user.id} отменил сброс майнинг-сессии.")

@dp.callback_query(F.data == "end_mining")
@error_handler_decorator
async def end_mining_callback(callback_query: types.CallbackQuery):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user_id = callback_query.from_user.id
    active_session = await get_active_mine_session(user_id)

    if not active_session:
        await callback_query.answer("У вас нет активных сессий майнинга.", show_alert=True)
        return

    start_time_utc = active_session['start_time'].astimezone(timezone.utc)
    current_time_utc = datetime.now(timezone.utc)
    elapsed_time = current_time_utc - start_time_utc
    
    user = await get_user_data(user_id)
    
    hours_elapsed = elapsed_time.total_seconds() / 3600
    mined_amount = user['mining_power'] * user['mining_profit_multiplier'] * hours_elapsed
    
    await end_mine_session(user_id, active_session['id'], mined_amount)
    await update_user_balance(user_id, mined_amount)
    await add_transaction(user_id, 'mining_reward', mined_amount, f"Завершение сессии майнинга. Продолжительность: {hours_elapsed:.2f} ч.")
    
    await callback_query.message.edit_text(f"⏳ Майнинг завершен! Вы намайнили **{mined_amount:.2f} RUB**, эти средства добавлены на ваш баланс. Хотите начать новую сессию?", reply_markup=get_mine_more_kb())
    await callback_query.answer()
    logger.info(f"Пользователь {user_id} завершил майнинг и получил {mined_amount:.2f} RUB.")


# Хендлер на кнопку "📈 Инвестиции"
@dp.message(F.text == "📈 Инвестиции")
@error_handler_decorator
async def show_investment_menu(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    await message.answer("Добро пожаловать в раздел инвестиций! Здесь вы можете увеличить свой капитал.", reply_markup=get_investment_menu_kb())
    logger.info(f"Пользователь {message.from_user.id} открыл меню инвестиций.")

@dp.callback_query(F.data == "my_investments")
@error_handler_decorator
async def show_my_investments(callback_query: types.CallbackQuery):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user_id = callback_query.from_user.id
    investments = await get_active_investments_for_user(user_id)

    if not investments:
        await callback_query.answer("У вас пока нет активных инвестиций.", show_alert=True)
        return

    response_text = "📊 **Ваши активные инвестиции:**\n\n"
    for inv in investments:
        response_text += (
            f"ID: `{inv['id']}`\n"
            f"Сумма: `{inv['amount']:.2f} RUB`\n"
            f"Ежедневный %: `{inv['daily_profit_percent']:.2f}%`\n"
            f"Накопленная прибыль: `{inv['total_profit_accrued']:.2f} RUB`\n"
            f"Дата начала: `{inv['investment_date'].strftime('%Y-%m-%d %H:%M')}`\n"
            f"Дата окончания: `{inv['end_date'].strftime('%Y-%m-%d %H:%M')}`\n"
            f"Последний начисление: `{inv['last_profit_accrual'].strftime('%Y-%m-%d %H:%M')}`\n\n"
        )
    await callback_query.message.edit_text(response_text, reply_markup=get_investment_menu_kb())
    await callback_query.answer()
    logger.info(f"Пользователь {user_id} запросил свои инвестиции.")

@dp.callback_query(F.data == "invest_now")
@error_handler_decorator
async def invest_now_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    await callback_query.message.edit_text("Введите сумму, которую хотите инвестировать (минимум 100 RUB):", reply_markup=get_back_to_main_kb())
    await state.set_state(InvestStates.waiting_for_invest_amount)
    await callback_query.answer()
    logger.info(f"Пользователь {callback_query.from_user.id} начал процесс инвестирования.")


@dp.message(InvestStates.waiting_for_invest_amount)
@error_handler_decorator
async def process_invest_amount(message: types.Message, state: FSMContext):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технительного обслуживания. Попробуйте позже.")
        return
    try:
        amount = float(message.text)
        if amount < 100:
            await message.answer("Сумма инвестиции должна быть не менее 100 RUB. Попробуйте еще раз.")
            return

        user = await get_user_data(message.from_user.id)
        if user['balance'] < amount:
            await message.answer(f"Недостаточно средств на балансе. Ваш баланс: {user['balance']:.2f} RUB.", reply_markup=get_main_kb())
            await state.clear()
            return
        
        await state.update_data(invest_amount=amount)
        await message.answer("Введите срок инвестиции в днях (например, 7, 30, 90):", reply_markup=get_back_to_main_kb())
        await state.set_state(InvestStates.waiting_for_invest_duration)
        logger.info(f"Пользователь {message.from_user.id} ввел сумму инвестиции: {amount}.")
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число для суммы.")


@dp.message(InvestStates.waiting_for_invest_duration)
@error_handler_decorator
async def process_invest_duration(message: types.Message, state: FSMContext):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    try:
        duration_days = int(message.text)
        if duration_days <= 0:
            await message.answer("Срок инвестиции должен быть положительным числом дней. Попробуйте еще раз.")
            return
        
        # Определяем процент в зависимости от срока
        daily_profit_percent = 0.0
        if duration_days <= 7:
            daily_profit_percent = 0.5 # 0.5% в день до 7 дней
        elif duration_days <= 30:
            daily_profit_percent = 0.7 # 0.7% в день до 30 дней
        elif duration_days <= 90:
            daily_profit_percent = 1.0 # 1.0% в день до 90 дней
        else:
            daily_profit_percent = 1.2 # 1.2% в день для более длительных сроков

        await state.update_data(invest_duration=duration_days, invest_percent=daily_profit_percent)
        data = await state.get_data()
        amount = data['invest_amount']

        confirmation_text = (
            f"Вы хотите инвестировать **{amount:.2f} RUB**\n"
            f"На срок **{duration_days} дней**\n"
            f"Под **{daily_profit_percent:.2f}%** в день.\n\n"
            "Подтверждаете?"
        )
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Подтвердить", callback_data="confirm_investment")
        builder.button(text="❌ Отмена", callback_data="cancel_investment")
        await message.answer(confirmation_text, reply_markup=builder.as_markup())
        logger.info(f"Пользователь {message.from_user.id} ввел срок инвестиции: {duration_days} дней.")
        
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число дней.")

@dp.callback_query(F.data == "confirm_investment")
@error_handler_decorator
async def confirm_investment_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user_id = callback_query.from_user.id
    data = await state.get_data()
    amount = data['invest_amount']
    duration_days = data['invest_duration']
    daily_profit_percent = data['invest_percent']

    user = await get_user_data(user_id)
    if user['balance'] < amount:
        await callback_query.answer("Недостаточно средств на балансе. Пожалуйста, пополните.", show_alert=True)
        await state.clear()
        return

    # Вычисляем дату окончания
    end_date = datetime.now() + timedelta(days=duration_days)

    await update_user_balance(user_id, -amount) # Списываем средства с баланса
    await add_transaction(user_id, 'investment_start', -amount, f"Инвестиция на {duration_days} дней")
    await add_user_investment(user_id, amount, daily_profit_percent, end_date)

    await callback_query.message.edit_text(f"✅ Ваша инвестиция в размере **{amount:.2f} RUB** на **{duration_days} дней** успешно оформлена! Ежедневная прибыль составит **{daily_profit_percent:.2f}%**.", reply_markup=get_investment_menu_kb())
    await callback_query.answer()
    await state.clear()
    logger.info(f"Пользователь {user_id} подтвердил инвестицию: {amount} RUB на {duration_days} дней.")

@dp.callback_query(F.data == "cancel_investment")
@error_handler_decorator
async def cancel_investment_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    await callback_query.message.edit_text("❌ Инвестиция отменена.", reply_markup=get_investment_menu_kb())
    await callback_query.answer()
    await state.clear()
    logger.info(f"Пользователь {callback_query.from_user.id} отменил инвестицию.")

# Фоновая задача для начисления прибыли по инвестициям
async def accrue_investment_profit():
    while True:
        await asyncio.sleep(60 * 60) # Проверяем каждый час
        # logger.info("Запущена фоновая задача начисления прибыли по инвестициям.")
        active_investments = await get_all_active_investments()
        current_time = datetime.now(timezone.utc)

        for inv in active_investments:
            user_id = inv['user_id']
            investment_id = inv['id']
            daily_profit_percent = inv['daily_profit_percent']
            amount = inv['amount']
            last_accrual_time = inv['last_profit_accrual'].astimezone(timezone.utc) if inv['last_profit_accrual'] else inv['investment_date'].astimezone(timezone.utc)
            end_date = inv['end_date'].astimezone(timezone.utc)

            # Проверяем, не закончилась ли инвестиция
            if current_time >= end_date:
                # Если инвестиция закончилась, начисляем последнюю прибыль и закрываем ее
                hours_since_last_accrual = (end_date - last_accrual_time).total_seconds() / 3600
                if hours_since_last_accrual > 0:
                    profit_amount = amount * (daily_profit_percent / 100) * (hours_since_last_accrual / 24)
                    await update_user_balance(user_id, profit_amount)
                    await add_transaction(user_id, 'investment_profit', profit_amount, f"Прибыль по инвестиции {investment_id} (завершение)")
                    await update_investment_profit(investment_id, profit_amount)
                    logger.info(f"Инвестиция {investment_id} завершена, начислена последняя прибыль {profit_amount:.2f} RUB.")
                await update_investment_status(investment_id, 'completed')
                # Уведомление пользователя о завершении инвестиции
                try:
                    await bot.send_message(user_id, f"🎉 Ваша инвестиция ID `{investment_id}` завершена! Общая накопленная прибыль: **{inv['total_profit_accrued'] + profit_amount:.2f} RUB**.")
                except TelegramForbiddenError:
                    logger.warning(f"Не удалось уведомить пользователя {user_id} о завершении инвестиции: бот заблокирован.")
                continue

            # Начисляем прибыль, если прошел хотя бы 1 час с последнего начисления
            if (current_time - last_accrual_time).total_seconds() >= 3600: # Прошел 1 час
                hours_to_accrue = (current_time - last_accrual_time).total_seconds() / 3600
                profit_amount = amount * (daily_profit_percent / 100) * (hours_to_accrue / 24)

                await update_user_balance(user_id, profit_amount)
                await add_transaction(user_id, 'investment_profit', profit_amount, f"Прибыль по инвестиции {investment_id}")
                await update_investment_profit(investment_id, profit_amount)
                logger.info(f"Начислена прибыль {profit_amount:.2f} RUB по инвестиции {investment_id} для пользователя {user_id}.")


# Хендлер на кнопку "💼 Вывод"
@dp.message(F.text == "💼 Вывод")
@error_handler_decorator
async def withdraw_funds_start(message: types.Message, state: FSMContext):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user = await get_user_data(message.from_user.id)
    if not user or user['balance'] == 0:
        await message.answer("У вас нет средств для вывода или ваш баланс равен 0.", reply_markup=get_main_kb())
        return

    await message.answer(f"Ваш баланс: {user['balance']:.2f} RUB.\n\nВведите сумму, которую хотите вывести (минимум 100 RUB):", reply_markup=get_back_to_main_kb())
    await state.set_state(WithdrawStates.waiting_for_amount)
    logger.info(f"Пользователь {message.from_user.id} начал процесс вывода средств.")

@dp.message(WithdrawStates.waiting_for_amount)
@error_handler_decorator
async def process_withdraw_amount(message: types.Message, state: FSMContext):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технительного обслуживания. Попробуйте позже.")
        return
    try:
        amount = float(message.text)
        if amount < 100:
            await message.answer("Сумма вывода должна быть не менее 100 RUB. Попробуйте еще раз.")
            return
        
        user = await get_user_data(message.from_user.id)
        if user['balance'] < amount:
            await message.answer(f"Недостаточно средств на балансе. Ваш баланс: {user['balance']:.2f} RUB. Попробуйте другую сумму или пополните баланс.", reply_markup=get_main_kb())
            await state.clear()
            return
        
        await state.update_data(withdrawal_amount=amount)
        await message.answer("Введите валюту (например, RUB, BTC, USDT):", reply_markup=get_back_to_main_kb())
        await state.set_state(WithdrawStates.waiting_for_currency)
        logger.info(f"Пользователь {message.from_user.id} ввел сумму для вывода: {amount}.")
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число для суммы.")


@dp.message(WithdrawStates.waiting_for_currency)
@error_handler_decorator
async def process_withdraw_currency(message: types.Message, state: FSMContext):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    currency = message.text.upper() # Переводим в верхний регистр для унификации
    if currency not in ['RUB', 'BTC', 'USDT']: # Пример поддерживаемых валют
        await message.answer("Поддерживаются только RUB, BTC, USDT. Пожалуйста, введите одну из них.")
        return

    await state.update_data(withdrawal_currency=currency)
    await message.answer("Введите номер кошелька для вывода (например, номер Qiwi, адрес BTC, адрес USDT):", reply_markup=get_back_to_main_kb())
    await state.set_state(WithdrawStates.waiting_for_wallet)
    logger.info(f"Пользователь {message.from_user.id} ввел валюту для вывода: {currency}.")


@dp.message(WithdrawStates.waiting_for_wallet)
@error_handler_decorator
async def process_withdraw_wallet(message: types.Message, state: FSMContext):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    wallet = message.text
    await state.update_data(withdrawal_wallet=wallet)

    data = await state.get_data()
    amount = data['withdrawal_amount']
    currency = data['withdrawal_currency']

    confirmation_text = (
        f"Вы собираетесь вывести:\n"
        f"Сумма: **{amount:.2f} {currency}**\n"
        f"На кошелек: `{wallet}`\n\n"
        f"Подтверждаете?"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить вывод", callback_data="confirm_withdrawal")
    builder.button(text="❌ Отмена", callback_data="cancel_withdrawal")
    
    await message.answer(confirmation_text, reply_markup=builder.as_markup())
    await state.set_state(WithdrawStates.waiting_for_withdrawal_confirmation)
    logger.info(f"Пользователь {message.from_user.id} ввел кошелек для вывода.")


@dp.callback_query(F.data == "confirm_withdrawal")
@error_handler_decorator
async def confirm_withdrawal_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    user_id = callback_query.from_user.id
    data = await state.get_data()
    amount = data['withdrawal_amount']
    currency = data['withdrawal_currency']
    wallet = data['withdrawal_wallet']

    # Проверить баланс еще раз, чтобы избежать двойных списаний или некорректных запросов
    user = await get_user_data(user_id)
    if user['balance'] < amount:
        await callback_query.answer("Недостаточно средств на балансе. Пожалуйста, пополните или уменьшите сумму.", show_alert=True)
        await state.clear()
        return

    await update_user_balance(user_id, -amount) # Списываем средства
    withdrawal_id = await add_withdrawal_request(user_id, amount, currency, wallet)
    await add_transaction(user_id, 'withdrawal_request', -amount, f"Запрос на вывод {amount} {currency} на {wallet}")

    await callback_query.message.edit_text("✅ Запрос на вывод средств отправлен на проверку администраторам. Ожидайте обработки.", reply_markup=get_main_kb())
    await callback_query.answer()
    await state.clear()

    # Уведомить админов о новом запросе на вывод
    for admin_id in ADMIN_IDS:
        try:
            user_info = await get_user_data(user_id)
            admin_notification_text = (
                f"🚨 **НОВЫЙ ЗАПРОС НА ВЫВОД СРЕДСТВ!**\n\n"
                f"От пользователя: [{user_info.get('full_name', 'N/A')}]({callback_query.from_user.url})\n"
                f"ID пользователя: `{user_id}`\n"
                f"Username: @{user_info.get('username', 'N/A')}\n"
                f"Сумма: `{amount:.2f} {currency}`\n"
                f"Кошелек: `{wallet}`"
            )
            await bot.send_message(admin_id, admin_notification_text, reply_markup=get_withdrawal_action_kb(withdrawal_id))
            logger.info(f"Админу {admin_id} отправлено уведомление о запросе на вывод {withdrawal_id}.")
        except TelegramForbiddenError:
            logger.warning(f"Не удалось отправить уведомление администратору {admin_id}: бот заблокирован.")
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления админу о запросе на вывод {withdrawal_id}: {e}", exc_info=True)
    logger.info(f"Пользователь {user_id} подтвердил запрос на вывод средств.")


@dp.callback_query(F.data == "cancel_withdrawal")
@error_handler_decorator
async def cancel_withdrawal_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if maintenance_mode and callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    await callback_query.message.edit_text("❌ Запрос на вывод средств отменен.", reply_markup=get_main_kb())
    await callback_query.answer()
    await state.clear()
    logger.info(f"Пользователь {callback_query.from_user.id} отменил запрос на вывод средств.")


# Хендлер на кнопку "🎁 Бонусы"
@dp.message(F.text == "🎁 Бонусы")
@error_handler_decorator
async def show_bonus_tasks(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    # TODO: Реализовать логику для бонусных заданий
    # Это может быть список задач, которые пользователь может выполнить для получения бонуса
    # Например, подписаться на канал, оставить отзыв и т.д.
    await message.answer("Раздел 'Бонусы' находится в разработке. Скоро здесь появятся интересные предложения!", reply_markup=get_main_kb())
    logger.info(f"Пользователь {message.from_user.id} запросил информацию о бонусах.")


# Хендлер на кнопку "❓ Помощь"
@dp.message(F.text == "❓ Помощь")
@error_handler_decorator
async def show_help(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.answer("Бот находится в режиме технического обслуживания. Попробуйте позже.")
        return
    help_text = (
        "**Центр поддержки**\n\n"
        "Если у вас возникли вопросы или проблемы, вы можете:\n"
        "1. Проверить раздел F.A.Q. (скоро)\n"
        "2. Написать в нашу службу поддержки: [Ссылка на поддержку](https://t.me/your_support_channel)\n\n"
        "Мы всегда готовы помочь!"
    )
    await message.answer(help_text, reply_markup=get_main_kb())
    logger.info(f"Пользователь {message.from_user.id} запросил помощь.")


# =====================
# АДМИН-ПАНЕЛЬ
# =====================

# Хендлер на кнопку "⚙️ Админ-панель"
@dp.message(F.text == "⚙️ Админ-панель")
@error_handler_decorator
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав доступа к админ-панели.")
        logger.warning(f"Несанкционированный доступ к админ-панели от пользователя {message.from_user.id}.")
        return
    await message.answer("Добро пожаловать в админ-панель!", reply_markup=get_admin_kb())
    logger.info(f"Админ {message.from_user.id} вошел в админ-панель.")


# --- Добавление задачи ---
@dp.message(F.text == "➕ Добавить задачу")
@error_handler_decorator
async def add_task_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите описание задачи:", reply_markup=get_back_to_main_kb())
    await state.set_state(TaskStates.waiting_for_description)
    logger.info(f"Админ {message.from_user.id} начал добавление задачи.")

@dp.message(TaskStates.waiting_for_description)
@error_handler_decorator
async def process_description(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.update_data(description=message.text)
    await message.answer("Введите вознаграждение за задачу (число):")
    await state.set_state(TaskStates.waiting_for_reward)
    logger.info(f"Админ {message.from_user.id} ввел описание задачи.")

@dp.message(TaskStates.waiting_for_reward)
@error_handler_decorator
async def process_reward(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        reward = float(message.text)
        await state.update_data(reward=reward)
        await message.answer("Введите максимальное количество исполнителей (число, или 0 если нет лимита):")
        await state.set_state(TaskStates.waiting_for_max_performers)
        logger.info(f"Админ {message.from_user.id} ввел вознаграждение задачи.")
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число для вознаграждения.")

@dp.message(TaskStates.waiting_for_max_performers)
@error_handler_decorator
async def process_max_performers(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        max_performers = int(message.text)
        if max_performers < 0:
            await message.answer("Максимальное количество исполнителей не может быть отрицательным. Введите 0 для безлимита или положительное число.")
            return

        # Если 0, то записываем None для отсутствия лимита
        if max_performers == 0:
            max_performers = None

        await state.update_data(max_performers=max_performers)
        await message.answer("Теперь отправьте фотографию или скриншот для задачи (если нет, отправьте '-' ):")
        await state.set_state(TaskStates.waiting_for_task_photo)
        logger.info(f"Админ {message.from_user.id} ввел максимальное количество исполнителей.")
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число для максимального количества исполнителей.")

@dp.message(TaskStates.waiting_for_task_photo)
@error_handler_decorator
async def process_task_photo(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    photo_file_id = None
    if message.photo:
        photo_file_id = message.photo[-1].file_id # ID самого большого фото
    elif message.text == '-':
        photo_file_id = None
    else:
        await message.answer("Пожалуйста, отправьте фотографию/скриншот или символ '-'.")
        return

    data = await state.get_data()
    description = data['description']
    reward = data['reward']
    max_performers = data['max_performers']

    await add_task(description, reward, max_performers, photo_file_id)
    await message.answer("✅ Задача успешно добавлена!", reply_markup=get_admin_kb())
    await state.clear()
    logger.info(f"Админ {message.from_user.id} завершил добавление задачи.")


# --- Проверка задач на выполнение ---
@dp.message(F.text == "📄 Проверить задачи")
@error_handler_decorator
async def check_pending_tasks(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    pending_tasks = await get_pending_user_tasks()
    
    if not pending_tasks:
        await message.answer("На данный момент нет задач, ожидающих проверки.", reply_markup=get_admin_kb())
        return

    for ut in pending_tasks:
        try:
            user_info = await get_user_data(ut['user_id'])
            task_info = await get_task_by_id(ut['task_id'])
            
            caption_text = (
                f"**Заявка ID: {ut['id']}**\n"
                f'От пользователя: [{user_info.get("full_name", "N/A")}]({f"tg://user?id={ut['user_id']}"})\n'
                f"ID пользователя: `{ut['user_id']}`\n"
                f"Username: @{user_info.get('username', 'N/A')}\n"
                f"Задача: `{task_info.get('description', 'N/A')}` (ID: `{ut['task_id']}`)\n"
                f"Вознаграждение: `{task_info.get('reward', 0):.2f} RUB`"
            )
            
            if ut['proof_photo_file_id']:
                await bot.send_photo(message.chat.id, photo=ut['proof_photo_file_id'], caption=caption_text, reply_markup=get_approve_decline_kb(ut['id']))
            else:
                await message.answer(caption_text + "\n(Нет фото подтверждения)", reply_markup=get_approve_decline_kb(ut['id']))
            logger.info(f"Админ {message.from_user.id} просматривает заявку {ut['id']}.")
        except Exception as e:
            logger.error(f"Ошибка при отправке задачи на проверку админу {message.from_user.id} (user_task_id: {ut['id']}): {e}", exc_info=True)
            await message.answer(f"Произошла ошибка при получении данных для заявки {ut['id']}. Проверьте логи.")
    
    await message.answer("Все заявки на проверку отправлены выше.", reply_markup=get_admin_kb())


# Обработка одобрения задачи
@dp.callback_query(F.data.startswith("approve_ut_"))
@error_handler_decorator
async def approve_user_task(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет прав доступа.")
        return

    user_task_id = int(callback_query.data.split("_")[2])
    
    conn = await get_db_connection()
    try:
        # Получаем данные user_task
        ut = await conn.fetchrow('SELECT * FROM user_tasks WHERE id = $1', user_task_id)
        if not ut:
            await callback_query.answer("Заявка не найдена или уже обработана.", show_alert=True)
            await callback_query.message.delete_reply_markup()
            return
        
        if ut['status'] != 'pending':
            await callback_query.answer(f"Эта заявка уже имеет статус '{ut['status']}'.", show_alert=True)
            await callback_query.message.delete_reply_markup()
            return

        task_id = ut['task_id']
        user_id = ut['user_id']

        # Получаем данные задачи и пользователя
        task = await conn.fetchrow('SELECT * FROM tasks WHERE id = $1', task_id)
        user = await conn.fetchrow('SELECT * FROM users WHERE id = $1', user_id)

        if not task or not user:
            await callback_query.answer("Ошибка: не удалось получить данные задачи или пользователя.", show_alert=True)
            return

        # Обновить статус user_task
        await conn.execute('UPDATE user_tasks SET status = $1 WHERE id = $2', 'completed', user_task_id)

        # Начислить вознаграждение пользователю
        reward = task['reward']
        await conn.execute('UPDATE users SET balance = balance + $1 WHERE id = $2', reward, user_id)
        await add_transaction(user_id, 'task_reward', reward, f"Вознаграждение за задачу {task_id}")

        # Увеличить счетчик исполнителей задачи
        await conn.execute('UPDATE tasks SET current_performers = current_performers + 1 WHERE id = $1', task_id)

        # Проверить лимит исполнителей после увеличения
        if task['max_performers'] is not None and task['current_performers'] + 1 >= task['max_performers']:
            await set_task_status(task_id, 'completed') # Завершить задачу, если достигнут лимит
            logger.info(f"Задача {task_id} помечена как 'completed' из-за достижения лимита исполнителей.")

        # Начислить реферальный бонус рефереру (если есть)
        referrer_id = user['referrer_id']
        if referrer_id:
            referral_bonus_amount = reward * REFERRAL_BONUS_PERCENT
            if referral_bonus_amount > 0:
                await update_user_balance(referrer_id, referral_bonus_amount)
                await add_transaction(referrer_id, 'referral_bonus', referral_bonus_amount, f"Бонус за реферала {user_id} (за задачу {task_id})")
                await log_referral_bonus(referrer_id, task_id, referral_bonus_amount)
                logger.info(f"Реферальный бонус {referral_bonus_amount:.2f} RUB начислен {referrer_id} за {user_id}.")
                try:
                    await bot.send_message(referrer_id, f"🎉 Вы получили реферальный бонус **{referral_bonus_amount:.2f} RUB** за то, что ваш реферал выполнил задачу '{task['description']}'!")
                except TelegramForbiddenError:
                    logger.warning(f"Не удалось отправить реферальный бонус рефереру {referrer_id}: бот заблокирован.")


        await callback_query.answer("Заявка одобрена! Вознаграждение начислено.", show_alert=True)
        # Изменить сообщение, удалив инлайн-кнопки
        await callback_query.message.edit_reply_markup(reply_markup=None)
        await callback_query.message.edit_caption(callback_query.message.caption + "\n\n✅ **ОДОБРЕНО!**", parse_mode='Markdown')

        # Уведомить пользователя
        try:
            await bot.send_message(user_id, f"🎉 Ваша задача '{task['description']}' (ID: {task_id}) одобрена! На ваш баланс зачислено **{reward:.2f} RUB**.")
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {user_id} заблокировал бота.")
        logger.info(f"Админ {callback_query.from_user.id} одобрил заявку {user_task_id}.")
    finally:
        if conn:
            await conn.close()


# Обработка отклонения задачи
@dp.callback_query(F.data.startswith("decline_ut_"))
@error_handler_decorator
async def decline_user_task(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет прав доступа.")
        return

    user_task_id = int(callback_query.data.split("_")[2])
    
    conn = await get_db_connection()
    try:
        ut = await conn.fetchrow('SELECT * FROM user_tasks WHERE id = $1', user_task_id)
        if not ut:
            await callback_query.answer("Заявка не найдена или уже обработана.", show_alert=True)
            await callback_query.message.delete_reply_markup()
            return
        
        if ut['status'] != 'pending':
            await callback_query.answer(f"Эта заявка уже имеет статус '{ut['status']}'.", show_alert=True)
            await callback_query.message.delete_reply_markup()
            return

        task_id = ut['task_id']
        user_id = ut['user_id']
        task = await conn.fetchrow('SELECT * FROM tasks WHERE id = $1', task_id)

        # Обновить статус user_task
        await conn.execute('UPDATE user_tasks SET status = $1 WHERE id = $2', 'rejected', user_task_id)

        await callback_query.answer("Заявка отклонена.", show_alert=True)
        # Изменить сообщение, удалив инлайн-кнопки
        await callback_query.message.edit_reply_markup(reply_markup=None)
        await callback_query.message.edit_caption(callback_query.message.caption + "\n\n❌ **ОТКЛОНЕНО!**", parse_mode='Markdown')

        # Уведомить пользователя
        try:
            await bot.send_message(user_id, f"❌ Ваша задача '{task['description']}' (ID: {task_id}) была отклонена. Пожалуйста, проверьте выполнение и попробуйте еще раз.")
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {user_id} заблокировал бота.")
        logger.info(f"Админ {callback_query.from_user.id} отклонил заявку {user_task_id}.")
    finally:
        if conn:
            await conn.close()


# --- Изменение задачи ---
@dp.message(F.text == "✏️ Изменить задачу")
@error_handler_decorator
async def edit_task_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите ID задачи, которую хотите изменить:", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminStates.waiting_for_task_to_edit_id)
    logger.info(f"Админ {message.from_user.id} начал изменение задачи.")

@dp.message(AdminStates.waiting_for_task_to_edit_id)
@error_handler_decorator
async def process_task_to_edit_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        task_id = int(message.text)
        task = await get_task_by_id(task_id)
        if not task:
            await message.answer("Задача с таким ID не найдена. Попробуйте еще раз или вернитесь в меню.", reply_markup=get_admin_kb())
            await state.clear()
            return
        
        await state.update_data(editing_task_id=task_id)
        await message.answer(f"Задача ID `{task_id}` выбрана.\n"
                             f"Текущее описание: `{task['description']}`\n"
                             f"Введите новое описание задачи (или '-' чтобы оставить текущее):", reply_markup=get_back_to_main_kb())
        await state.set_state(AdminStates.waiting_for_new_task_description)
        logger.info(f"Админ {message.from_user.id} выбрал задачу {task_id} для изменения.")
    except ValueError:
        await message.answer("Пожалуйста, введите корректный ID задачи.")


@dp.message(AdminStates.waiting_for_new_task_description)
@error_handler_decorator
async def process_new_task_description(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    task_id = data['editing_task_id']
    task = await get_task_by_id(task_id)

    new_description = message.text if message.text != '-' else task['description']
    await state.update_data(new_description=new_description)

    await message.answer(f"Текущее вознаграждение: `{task['reward']:.2f}`\n"
                         f"Введите новое вознаграждение (число, или '-' чтобы оставить текущее):")
    await state.set_state(AdminStates.waiting_for_new_task_reward)
    logger.info(f"Админ {message.from_user.id} ввел новое описание задачи {task_id}.")

@dp.message(AdminStates.waiting_for_new_task_reward)
@error_handler_decorator
async def process_new_task_reward(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    task_id = data['editing_task_id']
    task = await get_task_by_id(task_id)

    new_reward = task['reward']
    if message.text != '-':
        try:
            new_reward = float(message.text)
        except ValueError:
            await message.answer("Пожалуйста, введите корректное число для вознаграждения.")
            return
    
    await state.update_data(new_reward=new_reward)
    
    max_performers_display = task['max_performers'] if task['max_performers'] is not None else 0
    await message.answer(f"Текущее максимальное количество исполнителей: `{max_performers_display}`\n"
                         f"Введите новое максимальное количество исполнителей (число, 0 для безлимита, или '-' чтобы оставить текущее):")
    await state.set_state(AdminStates.waiting_for_new_task_max_performers)
    logger.info(f"Админ {message.from_user.id} ввел новое вознаграждение задачи {task_id}.")

@dp.message(AdminStates.waiting_for_new_task_max_performers)
@error_handler_decorator
async def process_new_task_max_performers(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    task_id = data['editing_task_id']
    task = await get_task_by_id(task_id)

    new_max_performers = task['max_performers']
    if message.text != '-':
        try:
            input_val = int(message.text)
            if input_val < 0:
                await message.answer("Максимальное количество исполнителей не может быть отрицательным. Введите 0 для безлимита или положительное число.")
                return
            new_max_performers = None if input_val == 0 else input_val
        except ValueError:
            await message.answer("Пожалуйста, введите корректное число для максимального количества исполнителей.")
            return

    await state.update_data(new_max_performers=new_max_performers)

    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE tasks SET description = $1, reward = $2, max_performers = $3 WHERE id = $4
        ''', data['new_description'], data['new_reward'], new_max_performers, task_id)
        await message.answer("✅ Задача успешно обновлена!", reply_markup=get_admin_kb())
        logger.info(f"Админ {message.from_user.id} обновил задачу {task_id}.")
    except Exception as e:
        logger.error(f"Ошибка при обновлении задачи {task_id}: {e}", exc_info=True)
        await message.answer("Произошла ошибка при обновлении задачи. Попробуйте еще раз.")
    finally:
        if conn:
            await conn.close()
    await state.clear()


# --- Удаление задачи ---
@dp.message(F.text == "🗑️ Удалить задачу")
@error_handler_decorator
async def delete_task_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите ID задачи, которую хотите удалить:", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminStates.waiting_for_task_to_delete_id)
    logger.info(f"Админ {message.from_user.id} начал удаление задачи.")

@dp.message(AdminStates.waiting_for_task_to_delete_id)
@error_handler_decorator
async def process_task_to_delete_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        task_id = int(message.text)
        task = await get_task_by_id(task_id)
        if not task:
            await message.answer("Задача с таким ID не найдена. Попробуйте еще раз или вернитесь в меню.", reply_markup=get_admin_kb())
            await state.clear()
            return
        
        conn = await get_db_connection()
        try:
            # Удаляем связанные записи из user_tasks и ref_bonuses_log
            await conn.execute('DELETE FROM user_tasks WHERE task_id = $1', task_id)
            await conn.execute('DELETE FROM ref_bonuses_log WHERE task_id = $1', task_id)
            # Устанавливаем NULL для task_id в transactions, где это возможно
            await conn.execute('UPDATE transactions SET task_id = NULL WHERE task_id = $1', task_id)
            # Теперь удаляем саму задачу
            await conn.execute('DELETE FROM tasks WHERE id = $1', task_id)
            await message.answer(f"✅ Задача ID `{task_id}` успешно удалена!", reply_markup=get_admin_kb())
            logger.info(f"Админ {message.from_user.id} удалил задачу {task_id}.")
        except Exception as e:
            logger.error(f"Ошибка при удалении задачи {task_id}: {e}", exc_info=True)
            await message.answer("Произошла ошибка при удалении задачи. Возможно, есть связанные данные. Проверьте логи.")
        finally:
            if conn:
                await conn.close()
        await state.clear()
    except ValueError:
        await message.answer("Пожалуйста, введите корректный ID задачи.")


# --- Изменение баланса пользователя ---
@dp.message(F.text == "💰 Изменить баланс")
@error_handler_decorator
async def change_balance_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите ID пользователя, баланс которого хотите изменить:", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminStates.waiting_for_user_balance_change_id)
    logger.info(f"Админ {message.from_user.id} начал изменение баланса пользователя.")

@dp.message(AdminStates.waiting_for_user_balance_change_id)
@error_handler_decorator
async def process_user_balance_change_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        user_id = int(message.text)
        user = await get_user_data(user_id)
        if not user:
            await message.answer("Пользователь с таким ID не найден. Попробуйте еще раз или вернитесь в меню.", reply_markup=get_admin_kb())
            await state.clear()
            return
        
        await state.update_data(target_user_id=user_id)
        await message.answer(f"Баланс пользователя `{user_id}` (@{user['username'] if user['username'] else 'нет'}) сейчас составляет: **{user['balance']:.2f} RUB**.\n"
                             "Введите сумму для изменения баланса. Используйте '+' для добавления, '-' для вычитания (например, +100 или -50):")
        await state.set_state(AdminStates.waiting_for_balance_change_amount)
        logger.info(f"Админ {message.from_user.id} выбрал пользователя {user_id} для изменения баланса.")
    except ValueError:
        await message.answer("Пожалуйста, введите корректный ID пользователя.")

@dp.message(AdminStates.waiting_for_balance_change_amount)
@error_handler_decorator
async def process_balance_change_amount(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        amount_str = message.text
        if not (amount_str.startswith('+') or amount_str.startswith('-')):
            await message.answer("Сумма должна начинаться с '+' или '-' (например, +100 или -50). Попробуйте еще раз.")
            return

        amount = float(amount_str)
        data = await state.get_data()
        user_id = data['target_user_id']
        
        await update_user_balance(user_id, amount)
        await add_transaction(user_id, 'admin_change', amount, f"Изменение баланса администратором (admin_id: {message.from_user.id})")
        
        updated_user = await get_user_data(user_id)
        await message.answer(f"✅ Баланс пользователя `{user_id}` успешно изменен. Новый баланс: **{updated_user['balance']:.2f} RUB**.", reply_markup=get_admin_kb())
        
        # Уведомить пользователя о изменении баланса
        try:
            await bot.send_message(user_id, f"💰 Ваш баланс был изменен администратором на **{amount:.2f} RUB**. Текущий баланс: **{updated_user['balance']:.2f} RUB**.")
        except TelegramForbiddenError:
            logger.warning(f"Не удалось уведомить пользователя {user_id} об изменении баланса: бот заблокирован.")
        
        await state.clear()
        logger.info(f"Админ {message.from_user.id} изменил баланс пользователя {user_id} на {amount}.")
    except ValueError:
        await message.answer("Пожалуйста, введите корректное числовое значение для суммы.")


# --- Статистика ---
@dp.message(F.text == "📈 Статистика")
@error_handler_decorator
async def show_statistics(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    total_users = await get_all_users_count()
    total_balance = await get_total_balance()

    stats_message = (
        f"📊 **Общая статистика:**\n\n"
        f"👥 **Всего пользователей:** {total_users}\n"
        f"💰 **Общий баланс пользователей:** {total_balance:.2f} RUB\n"
        # Можно добавить больше статистики: количество выполненных задач, выплат и т.д.
    )
    await message.answer(stats_message, reply_markup=get_admin_kb())
    logger.info(f"Админ {message.from_user.id} запросил статистику.")

    # Можно также сгенерировать файл Excel с данными пользователей
    conn = await get_db_connection()
    try:
        users_data = await conn.fetch('SELECT id, username, full_name, balance, registration_date, referrer_id, referral_count, mining_level FROM users')
        if users_data:
            df = pd.DataFrame(users_data)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Users')
            output.seek(0)
            await message.answer_document(BufferedInputFile(output.getvalue(), filename="users_data.xlsx"), caption="Полные данные о пользователях:")
            logger.info(f"Админ {message.from_user.id} скачал данные пользователей.")
        else:
            await message.answer("Нет данных о пользователях для экспорта.")
    except Exception as e:
        logger.error(f"Ошибка при экспорте данных пользователей: {e}", exc_info=True)
        await message.answer("Произошла ошибка при экспорте данных пользователей.")
    finally:
        if conn:
            await conn.close()


# --- Рассылка ---
@dp.message(F.text == "📢 Рассылка")
@error_handler_decorator
async def start_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите текст для рассылки всем пользователям:", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminStates.waiting_for_broadcast_message)
    logger.info(f"Админ {message.from_user.id} начал рассылку.")

@dp.message(AdminStates.waiting_for_broadcast_message)
@error_handler_decorator
async def process_broadcast_message(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    broadcast_text = message.text
    all_users = await get_all_users_data() # Получить всех пользователей

    sent_count = 0
    blocked_count = 0

    await message.answer("Начинаю рассылку. Это может занять некоторое время...", reply_markup=get_admin_kb())

    for user in all_users:
        try:
            await bot.send_message(user['id'], broadcast_text)
            sent_count += 1
            await asyncio.sleep(0.05) # Небольшая задержка, чтобы избежать лимитов Telegram
        except TelegramForbiddenError:
            # Пользователь заблокировал бота
            blocked_count += 1
            logger.warning(f"Пользователь {user['id']} заблокировал бота. Сообщение не доставлено.")
            # Можно пометить пользователя как неактивного/заблокированного в БД
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение пользователю {user['id']}: {e}", exc_info=True)

    await message.answer(f"✅ Рассылка завершена!\n"
                         f"Отправлено сообщений: {sent_count}\n"
                         f"Заблокировали бота: {blocked_count}", reply_markup=get_admin_kb())
    await state.clear()
    logger.info(f"Админ {message.from_user.id} завершил рассылку. Отправлено: {sent_count}, заблокировано: {blocked_count}.")


# --- Вывод средств (админ-панель) ---
@dp.message(F.text == "📤 Вывод средств")
@error_handler_decorator
async def check_withdrawal_requests(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    pending_requests = await get_pending_withdrawal_requests()

    if not pending_requests:
        await message.answer("На данный момент нет запросов на вывод средств.", reply_markup=get_admin_kb())
        return

    for req in pending_requests:
        try:
            user_info = await get_user_data(req['user_id'])
            caption_text = (
                f"**Запрос на вывод ID: {req['id']}**\n"
                f"От пользователя: [{user_info.get('full_name', 'N/A')}]({f'tg://user?id={req["user_id"]}'})\n"
                f"ID пользователя: `{req['user_id']}`\n"
                f"Username: @{user_info.get('username', 'N/A')}\n"
                f"Сумма: `{req['amount']:.2f} {req['currency']}`\n"
                f"Кошелек: `{req['wallet']}`\n"
                f"Время запроса: `{req['request_time'].strftime('%Y-%m-%d %H:%M:%S')}`"
            )
            await message.answer(caption_text, reply_markup=get_withdrawal_action_kb(req['id']))
            logger.info(f"Админ {message.from_user.id} просматривает запрос на вывод {req['id']}.")
        except Exception as e:
            logger.error(f"Ошибка при отправке запроса на вывод админу {message.from_user.id} (withdrawal_id: {req['id']}): {e}", exc_info=True)
            await message.answer(f"Произошла ошибка при получении данных для запроса на вывод {req['id']}. Проверьте логи.")
    
    await message.answer("Все запросы на вывод отправлены выше.", reply_markup=get_admin_kb())


@dp.callback_query(F.data.startswith("confirm_withdrawal_"))
@error_handler_decorator
async def confirm_withdrawal_admin(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет прав доступа.")
        return

    withdrawal_id = int(callback_query.data.split("_")[2])
    request = await get_withdrawal_request_by_id(withdrawal_id)

    if not request or request['status'] != 'pending':
        await callback_query.answer("Запрос не найден или уже обработан.", show_alert=True)
        await callback_query.message.delete_reply_markup()
        return
    
    user_id = request['user_id']
    amount = request['amount']
    currency = request['currency']

    await update_withdrawal_status(withdrawal_id, 'approved')
    # Здесь можно было бы добавить логику взаимодействия с Qiwi API, если она была бы реализована
    # Например: await qiwi_api.send_payment(request['wallet'], amount, currency)

    await callback_query.answer("Запрос на вывод подтвержден!", show_alert=True)
    await callback_query.message.edit_reply_markup(reply_markup=None)
    await callback_query.message.edit_text(callback_query.message.text + "\n\n✅ **ВЫВЕДЕНО!**")

    # Уведомить пользователя
    try:
        await bot.send_message(user_id, f"✅ Ваш запрос на вывод **{amount:.2f} {currency}** на кошелек `{request['wallet']}` одобрен и средства отправлены!")
    except TelegramForbiddenError:
        logger.warning(f"Пользователь {user_id} заблокировал бота.")
    
    await state.clear()
    await bot.send_message(callback_query.from_user.id, "Выберите следующее действие:", reply_markup=get_admin_kb())
    logger.info(f"Админ {callback_query.from_user.id} подтвердил вывод средств {withdrawal_id}.")


@dp.callback_query(F.data.startswith("reject_withdrawal_"))
@error_handler_decorator
async def reject_withdrawal_admin(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет прав доступа.")
        return

    withdrawal_id = int(callback_query.data.split("_")[2])
    request = await get_withdrawal_request_by_id(withdrawal_id)

    if not request or request['status'] != 'pending':
        await callback_query.answer("Запрос не найден или уже обработан.", show_alert=True)
        await callback_query.message.delete_reply_markup()
        return
    
    user_id = request['user_id']
    amount = request['amount']
    currency = request['currency']

    await update_withdrawal_status(withdrawal_id, 'rejected')
    await update_user_balance(user_id, amount) # Возвращаем средства на баланс
    await add_transaction(user_id, 'withdrawal_rejected', amount, f"Возврат средств за отклоненный вывод {withdrawal_id}")

    await callback_query.answer("Запрос на вывод отклонен. Средства возвращены на баланс пользователя.", show_alert=True)
    await callback_query.message.edit_reply_markup(reply_markup=None)
    await callback_query.message.edit_text(callback_query.message.text + "\n\n❌ **ОТКЛОНЕНО!**")

    # Уведомить пользователя
    try:
        await bot.send_message(user_id, f"❌ Ваш запрос на вывод {amount:.2f} {currency} был отклонен. Средства возвращены на ваш баланс. Пожалуйста, свяжитесь с администратором для уточнения причин.")
    except TelegramForbiddenError:
        logger.warning(f"Пользователь {user_id} заблокировал бота.")

    await state.clear()
    await bot.send_message(callback_query.from_user.id, "Выберите следующее действие:", reply_markup=get_admin_kb())
    logger.info(f"Админ {callback_query.from_user.id} отклонил вывод средств {withdrawal_id}.")


# --- Добавление бонусной задачи (для админа) ---
@dp.message(F.text == "➕ Бонусная задача")
@error_handler_decorator
async def add_bonus_task_admin_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите название бонусной задачи (например, 'Подписка на канал'):", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminStates.waiting_for_bonus_task_name)
    logger.info(f"Админ {message.from_user.id} начал добавление бонусной задачи.")

@dp.message(AdminStates.waiting_for_bonus_task_name)
@error_handler_decorator
async def process_bonus_task_name(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.update_data(bonus_task_name=message.text)
    await message.answer("Введите вознаграждение за эту бонусную задачу (число):")
    await state.set_state(AdminStates.waiting_for_bonus_task_reward)
    logger.info(f"Админ {message.from_user.id} ввел название бонусной задачи.")

@dp.message(AdminStates.waiting_for_bonus_task_reward)
@error_handler_decorator
async def process_bonus_task_reward(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        reward = float(message.text)
        if reward <= 0:
            await message.answer("Вознаграждение должно быть положительным числом. Попробуйте еще раз.")
            return

        data = await state.get_data()
        task_name = data['bonus_task_name']
        
        await add_bonus_task(task_name, reward)
        await message.answer(f"✅ Бонусная задача '{task_name}' с вознаграждением {reward:.2f} RUB успешно добавлена!", reply_markup=get_admin_kb())
        await state.clear()
        logger.info(f"Админ {message.from_user.id} добавил бонусную задачу.")
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число для вознаграждения.")


# --- Режим технического обслуживания ---
@dp.message(F.text == "🔧 Техперерыв Вкл")
@error_handler_decorator
async def enable_maintenance(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    global maintenance_mode
    maintenance_mode = True
    await message.answer("✅ Режим технического обслуживания включен. Бот будет отвечать только администраторам.", reply_markup=get_admin_kb())
    logger.info(f"Админ {message.from_user.id} включил режим технического обслуживания.")

@dp.message(F.text == "🔧 Техперерыв Выкл")
@error_handler_decorator
async def disable_maintenance(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    global maintenance_mode
    maintenance_mode = False
    await message.answer("✅ Режим технического обслуживания выключен. Бот снова доступен всем пользователям.", reply_markup=get_admin_kb())
    logger.info(f"Админ {message.from_user.id} выключил режим технического обслуживания.")


# Обработчик для всех остальных текстовых сообщений
@dp.message(F.text)
@error_handler_decorator
async def handle_invalid_command(message: types.Message):
    # Этот обработчик должен быть в конце, чтобы не перехватывать другие команды
    await message.answer("❌ Неверный формат команды. Пожалуйста, используйте кнопки меню.", reply_markup=get_main_kb())

# =====================
# ЗАПУСК БОТА
# =====================

async def main():
    await init_db()
    # Запускаем фоновую задачу для начисления инвестиционной прибыли
    asyncio.create_task(accrue_investment_profit())
    # Удалите или закомментируйте эту строку, если вы хотите получать все обновления
    # await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, skip_updates=True) # skip_updates=True пропустит все старые необработанные обновления

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}", exc_info=True)
