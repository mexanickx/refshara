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
    exit(1) # Завершаем программу, если токен не установлен

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL не установлен в переменных окружения. База данных недоступна.")
    exit(1)

ADMIN_IDS_STR = os.environ.get("ADMIN_IDS")
# Улучшенный парсинг ADMIN_IDS: фильтруем пустые строки
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',') if x.strip()] if ADMIN_IDS_STR else []
if not ADMIN_IDS:
    logger.warning("ADMIN_IDS не установлены или пусты. Некоторые функции администрирования могут быть недоступны.")

CRYPTO_BOT_TOKEN = os.environ.get("CRYPTO_BOT_API_TOKEN")
if not CRYPTO_BOT_TOKEN:
    logger.warning("CRYPTO_BOT_API_TOKEN не установлен. Функции пополнения/вывода через Crypto Bot будут отключены.")


# --- ИНИЦИАЛИЗАЦИЯ БОТА И ДИСПЕТЧЕРА ---
bot = Bot(API_TOKEN, parse_mode='HTML') # Используем HTML для форматирования
dp = Dispatcher(storage=MemoryStorage())

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ И НАСТРОЙКИ ---
maintenance_mode = False # Режим технического обслуживания
min_withdrawal_amount_btc = 0.0001
min_withdrawal_amount_usdt = 5.0 # Минимальная сумма вывода USDT
min_withdrawal_amount_trc20 = 5.0 # Минимальная сумма вывода TRC20 (если TRC20 не USDT)

# --- СОСТОЯНИЯ FSM (Finite State Machine) ---
class Form(StatesGroup):
    task_id = State()
    proof_text = State()
    proof_photo = State()
    withdrawal_amount = State()
    withdrawal_address = State()
    withdrawal_currency = State()
    admin_send_message = State()
    admin_send_photo = State()
    admin_send_text = State()
    waiting_for_task_name = State()
    waiting_for_task_description = State()
    waiting_for_task_reward = State()
    waiting_for_delete_task_number = State()
    waiting_for_task_id_for_approve = State()
    waiting_for_proof_photo_for_approve = State()


class WithdrawState(StatesGroup):
    waiting_for_amount = State()
    waiting_for_address = State()
    waiting_for_currency = State()

class AdminAddTaskState(StatesGroup):
    waiting_for_name = State()
    waiting_for_description = State()
    waiting_for_reward = State()

class DeleteTaskState(StatesGroup):
    waiting_for_task_number = State()

class ApproveTaskState(StatesGroup):
    waiting_for_proof_id = State()
    waiting_for_admin_decision = State()

class AdminBroadcastState(StatesGroup):
    waiting_for_message = State()


# --- ФЕЙКОВЫЙ ВЕБ-СЕРВЕР ДЛЯ RENDER.COM ---
# запускаем фейковый сервер, чтобы Render не ругался
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
# Это важно, чтобы основной поток оставался свободным для работы бота
server_thread = threading.Thread(target=run_fake_server)
server_thread.daemon = True # Позволяет потоку завершиться при завершении основной программы
server_thread.start()


# --- ДЕКОРАТОР ДЛЯ ОБРАБОТКИ ОШИБОК И РЕЖИМА ТЕХ. ОБСЛУЖИВАНИЯ ---
def error_handler_decorator(func):
    async def wrapper(message: types.Message, *args, **kwargs):
        if maintenance_mode and message.from_user.id not in ADMIN_IDS:
            await message.answer("🛠️ Бот находится на техническом обслуживании. Пожалуйста, попробуйте позже.")
            return

        try:
            return await func(message, *args, **kwargs)
        except TelegramForbiddenError:
            logger.error(f"Бот заблокирован пользователем {message.from_user.id}.")
            # Здесь можно добавить логику для удаления пользователя из БД или отметки его как неактивного
        except Exception as e:
            logger.error(f"Произошла ошибка в обработчике {func.__name__}: {e}", exc_info=True)
            if message.from_user.id in ADMIN_IDS:
                await message.answer(f"⚠️ Произошла ошибка: {e}\n\nПожалуйста, проверьте логи.")
            else:
                await message.answer("⚠️ Произошла непредвиденная ошибка. Пожалуйста, попробуйте еще раз позже.")
    return wrapper

# --- ФУНКЦИИ БАЗЫ ДАННЫХ ---
async def get_db_pool():
    if not hasattr(get_db_pool, 'pool'):
        get_db_pool.pool = await asyncpg.create_pool(DATABASE_URL)
    return get_db_pool.pool

async def init_db():
    conn = None
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    reg_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    balance REAL DEFAULT 0.0,
                    last_mine_time TIMESTAMP WITH TIME ZONE,
                    referral_count INTEGER DEFAULT 0, -- Убедитесь, что этот столбец есть!
                    referrer_id BIGINT,
                    invited_users TEXT DEFAULT '[]',
                    mining_level INTEGER DEFAULT 1,
                    mining_income REAL DEFAULT 0.001,
                    last_bonus_time TIMESTAMP WITH TIME ZONE,
                    invited_by_link TEXT,
                    btc_address TEXT,
                    trc20_address TEXT,
                    usdt_balance REAL DEFAULT 0.0
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS mining_upgrades (
                    user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                    level INTEGER DEFAULT 1,
                    price REAL DEFAULT 0.0,
                    income REAL DEFAULT 0.001,
                    last_upgrade_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id SERIAL PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    task_description TEXT NOT NULL,
                    reward REAL NOT NULL,
                    status TEXT DEFAULT 'active'
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS completed_tasks (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    task_id INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                    completion_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE (user_id, task_id)
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS task_proofs (
                    proof_id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    task_id INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                    proof_text TEXT,
                    proof_photo_id TEXT,
                    submission_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    status TEXT DEFAULT 'pending'
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS withdrawals (
                    withdrawal_id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL, -- BTC, USDT, TRC20
                    address TEXT NOT NULL,
                    request_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    status TEXT DEFAULT 'pending' -- pending, approved, rejected
                );
            ''')
            logger.info("База данных и таблицы инициализированы/проверены.")
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}", exc_info=True)
        exit(1)

async def db_add_user(user_id: int, username: str, referrer_id: int = None, invited_by_link: str = None):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO users(user_id, username, reg_date, balance, last_mine_time, referrer_id, invited_by_link)
            VALUES($1, $2, NOW(), 0.0, NULL, $3, $4)
            ON CONFLICT (user_id) DO NOTHING
        ''', user_id, username, referrer_id, invited_by_link)
        logger.info(f"Добавлен или обновлен пользователь: {user_id}, username: {username}, referrer: {referrer_id}")

        if referrer_id:
            # Для надежности, убедитесь, что referrer_id существует
            referrer_exists = await conn.fetchval("SELECT user_id FROM users WHERE user_id = $1", referrer_id)
            if referrer_exists:
                await conn.execute('''
                    UPDATE users
                    SET referral_count = referral_count + 1,
                        invited_users = invited_users || $1::jsonb
                    WHERE user_id = $2
                ''', f'[{user_id}]', referrer_id)
                logger.info(f"Увеличено referral_count для реферера {referrer_id}.")
            else:
                logger.warning(f"Реферер {referrer_id} не найден в базе данных.")


async def db_get_user(user_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Добавлен referral_count в SELECT запрос
        row = await conn.fetchrow("SELECT user_id, username, reg_date, balance, last_mine_time, referral_count, referrer_id, invited_users, mining_level, mining_income, last_bonus_time, invited_by_link, btc_address, trc20_address, usdt_balance FROM users WHERE user_id = $1", user_id)
        if row:
            # Преобразуем row в словарь для удобства доступа
            user_data = dict(row)
            # Обработка last_mine_time для обратной совместимости (если еще есть старые записи)
            if 'last_mine_time' in user_data and isinstance(user_data['last_mine_time'], str):
                try:
                    user_data['last_mine_time'] = datetime.strptime(user_data['last_mine_time'], '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
                except ValueError:
                    user_data['last_mine_time'] = datetime.strptime(user_data['last_mine_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            # Обработка last_bonus_time
            if 'last_bonus_time' in user_data and isinstance(user_data['last_bonus_time'], str):
                try:
                    user_data['last_bonus_time'] = datetime.strptime(user_data['last_bonus_time'], '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
                except ValueError:
                    user_data['last_bonus_time'] = datetime.strptime(user_data['last_bonus_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            return user_data
        return None

async def db_update_user_balance(user_id: int, amount: float):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        new_balance = await conn.fetchval("UPDATE users SET balance = balance + $1 WHERE user_id = $2 RETURNING balance", amount, user_id)
        logger.info(f"Баланс пользователя {user_id} обновлен на {amount}. Новый баланс: {new_balance}")
        return new_balance

async def db_update_user_last_mine_time(user_id: int, mine_time: datetime):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_mine_time = $1 WHERE user_id = $2", mine_time.astimezone(timezone.utc), user_id)
        logger.info(f"Время последнего майнинга для пользователя {user_id} обновлено на {mine_time.isoformat()}")

async def db_update_user_username(user_id: int, username: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET username = $1 WHERE user_id = $2", username, user_id)
        logger.info(f"Имя пользователя {user_id} обновлено на {username}.")

async def db_get_all_users_ids():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [row['user_id'] for row in rows]

async def db_get_referrals_count(user_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Теперь получаем из `referral_count` в `users`
        count = await conn.fetchval("SELECT referral_count FROM users WHERE user_id = $1", user_id)
        return count if count is not None else 0

async def db_get_user_mining_upgrade(user_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT level, income, price, last_upgrade_time FROM mining_upgrades WHERE user_id = $1", user_id)
        if row:
            upgrade_data = dict(row)
            if 'last_upgrade_time' in upgrade_data and isinstance(upgrade_data['last_upgrade_time'], str):
                try:
                    upgrade_data['last_upgrade_time'] = datetime.strptime(upgrade_data['last_upgrade_time'], '%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
                except ValueError:
                    upgrade_data['last_upgrade_time'] = datetime.strptime(upgrade_data['last_upgrade_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            return upgrade_data
        return None

async def db_update_user_mining_upgrade(user_id: int, level: int, income: float, price: float):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Используем INSERT ... ON CONFLICT UPDATE для удобства
        await conn.execute('''
            INSERT INTO mining_upgrades(user_id, level, income, price, last_upgrade_time)
            VALUES($1, $2, $3, $4, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET level = $2, income = $3, price = $4, last_upgrade_time = NOW()
        ''', user_id, level, income, price)
        logger.info(f"Уровень майнинга пользователя {user_id} обновлен до уровня {level}.")

async def db_update_user_btc_address(user_id: int, address: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET btc_address = $1 WHERE user_id = $2", address, user_id)
        logger.info(f"BTC адрес пользователя {user_id} обновлен: {address}")

async def db_update_user_trc20_address(user_id: int, address: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET trc20_address = $1 WHERE user_id = $2", address, user_id)
        logger.info(f"TRC20 адрес пользователя {user_id} обновлен: {address}")

async def db_update_user_usdt_balance(user_id: int, amount: float):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        new_balance = await conn.fetchval("UPDATE users SET usdt_balance = usdt_balance + $1 WHERE user_id = $2 RETURNING usdt_balance", amount, user_id)
        logger.info(f"USDT баланс пользователя {user_id} обновлен на {amount}. Новый баланс: {new_balance}")
        return new_balance

async def db_add_task(task_name: str, task_description: str, reward: float):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        task_id = await conn.fetchval('''
            INSERT INTO tasks(task_name, task_description, reward)
            VALUES($1, $2, $3)
            RETURNING task_id
        ''', task_name, task_description, reward)
        logger.info(f"Добавлено новое задание: {task_name} с ID {task_id}")
        return task_id

async def db_get_active_tasks():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT task_id, task_name, task_description, reward FROM tasks WHERE status = 'active'")
        return [dict(row) for row in rows]

async def db_get_task(task_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT task_id, task_name, task_description, reward, status FROM tasks WHERE task_id = $1", task_id)
        return dict(row) if row else None

async def db_delete_task(task_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tasks WHERE task_id = $1", task_id)
        logger.info(f"Задание с ID {task_id} удалено.")

async def db_set_task_status(task_id: int, status: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET status = $1 WHERE task_id = $2", status, task_id)
        logger.info(f"Статус задания {task_id} изменен на {status}.")

async def db_add_completed_task(user_id: int, task_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute('''
                INSERT INTO completed_tasks(user_id, task_id, completion_date)
                VALUES($1, $2, NOW())
            ''', user_id, task_id)
            logger.info(f"Пользователь {user_id} отметил задание {task_id} как выполненное.")
            return True
        except asyncpg.exceptions.UniqueViolationError:
            logger.warning(f"Пользователь {user_id} уже выполнил задание {task_id}.")
            return False

async def db_has_completed_task(user_id: int, task_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM completed_tasks WHERE user_id = $1 AND task_id = $2", user_id, task_id)
        return count > 0

async def db_get_user_completed_tasks(user_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT task_id, completion_date FROM completed_tasks WHERE user_id = $1")
        return [dict(row) for row in rows]

async def db_add_task_proof(user_id: int, task_id: int, proof_text: str = None, proof_photo_id: str = None):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        proof_id = await conn.fetchval('''
            INSERT INTO task_proofs(user_id, task_id, proof_text, proof_photo_id, submission_date, status)
            VALUES($1, $2, $3, $4, NOW(), 'pending')
            RETURNING proof_id
        ''', user_id, task_id, proof_text, proof_photo_id)
        logger.info(f"Добавлено подтверждение для задания {task_id} от пользователя {user_id}. Proof ID: {proof_id}")
        return proof_id

async def db_get_pending_proofs():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT proof_id, user_id, task_id, proof_text, proof_photo_id, submission_date FROM task_proofs WHERE status = 'pending'")
        return [dict(row) for row in rows]

async def db_set_proof_status(proof_id: int, status: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE task_proofs SET status = $1 WHERE proof_id = $2", status, proof_id)
        logger.info(f"Статус подтверждения {proof_id} изменен на {status}.")

async def db_get_proof(proof_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT proof_id, user_id, task_id, proof_text, proof_photo_id, submission_date, status FROM task_proofs WHERE proof_id = $1", proof_id)
        return dict(row) if row else None

async def db_add_withdrawal_request(user_id: int, amount: float, currency: str, address: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        withdrawal_id = await conn.fetchval('''
            INSERT INTO withdrawals(user_id, amount, currency, address, request_date, status)
            VALUES($1, $2, $3, $4, NOW(), 'pending')
            RETURNING withdrawal_id
        ''', user_id, amount, currency, address)
        logger.info(f"Запрос на вывод {amount} {currency} от пользователя {user_id} на адрес {address}. ID: {withdrawal_id}")
        return withdrawal_id

async def db_get_pending_withdrawals():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT withdrawal_id, user_id, amount, currency, address, request_date FROM withdrawals WHERE status = 'pending'")
        return [dict(row) for row in rows]

async def db_set_withdrawal_status(withdrawal_id: int, status: str):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE withdrawals SET status = $1 WHERE withdrawal_id = $2", status, withdrawal_id)
        logger.info(f"Статус вывода {withdrawal_id} изменен на {status}.")

async def db_get_total_completed_tasks_count(user_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM task_proofs WHERE user_id = $1 AND status = 'approved'", user_id)
        return count if count is not None else 0

async def db_get_all_users_data():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, username, reg_date, balance, last_mine_time, referral_count, referrer_id, invited_users, mining_level, mining_income, last_bonus_time, invited_by_link, btc_address, trc20_address, usdt_balance FROM users")
        return [dict(row) for row in rows]

async def db_get_all_tasks_data():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT task_id, task_name, task_description, reward, status FROM tasks")
        return [dict(row) for row in rows]

async def db_get_all_task_proofs_data():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT proof_id, user_id, task_id, proof_text, proof_photo_id, submission_date, status FROM task_proofs")
        return [dict(row) for row in rows]

async def db_get_all_withdrawals_data():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT withdrawal_id, user_id, amount, currency, address, request_date, status FROM withdrawals")
        return [dict(row) for row in rows]

async def db_update_user_bonus_time(user_id: int, bonus_time: datetime):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_bonus_time = $1 WHERE user_id = $2", bonus_time.astimezone(timezone.utc), user_id)
        logger.info(f"Время последнего бонуса для пользователя {user_id} обновлено на {bonus_time.isoformat()}")

# --- Вспомогательные функции для Crypto Bot API ---
async def crypto_bot_create_invoice(asset: str, amount: float, user_id: int, description: str):
    if not CRYPTO_BOT_TOKEN:
        logger.error("CRYPTO_BOT_API_TOKEN не установлен, невозможно создать инвойс.")
        return None

    url = "https://pay.crypt.bot/api/createInvoice"
    headers = {
        "Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN
    }
    payload = {
        "asset": asset,
        "amount": str(amount),
        "description": description,
        "payload": str(user_id),
        "allow_anonymous": True,
        "allow_comments": True,
        "expires_in": 3600 # 1 час
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status() # Вызывает исключение для HTTP ошибок
        json_response = response.json()
        logger.info(f"Crypto Bot API Response (createInvoice): {json_response}")
        if json_response.get("ok"):
            return json_response["result"]
        else:
            logger.error(f"Ошибка создания инвойса Crypto Bot: {json_response.get('error')}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса к Crypto Bot API (createInvoice): {e}")
        return None

async def crypto_bot_get_invoices(invoice_ids: list):
    if not CRYPTO_BOT_TOKEN:
        logger.error("CRYPTO_BOT_API_TOKEN не установлен, невозможно получить инвойсы.")
        return []

    url = "https://pay.crypt.bot/api/getInvoices"
    headers = {
        "Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN
    }
    params = {
        "invoice_ids": ",".join(map(str, invoice_ids))
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        json_response = response.json()
        logger.info(f"Crypto Bot API Response (getInvoices for {invoice_ids}): {json_response}")
        if json_response.get("ok"):
            return json_response["result"]["items"]
        else:
            logger.error(f"Ошибка получения инвойсов Crypto Bot: {json_response.get('error')}")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса к Crypto Bot API (getInvoices): {e}")
        return []

async def crypto_bot_get_balance():
    if not CRYPTO_BOT_TOKEN:
        logger.error("CRYPTO_BOT_API_TOKEN не установлен, невозможно получить баланс.")
        return None
    url = "https://pay.crypt.bot/api/getBalance"
    headers = {
        "Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        json_response = response.json()
        if json_response.get("ok"):
            return {item['asset']: item['available'] for item in json_response['result']}
        else:
            logger.error(f"Ошибка получения баланса Crypto Bot: {json_response.get('error')}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса к Crypto Bot API (getBalance): {e}")
        return None


# --- КЛАВИАТУРЫ ---
def get_main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💎 Майнинг"), KeyboardButton(text="🚀 Задания")],
            [KeyboardButton(text="👥 Рефералы"), KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="ℹ️ Информация"), KeyboardButton(text="⚙️ Настройки")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def get_balance_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Пополнить"), KeyboardButton(text="➖ Вывести")],
            [KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def get_admin_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="✍️ Объявление")],
            [KeyboardButton(text="➕ Добавить задание"), KeyboardButton(text="❌ Удалить задание")],
            [KeyboardButton(text="✅ Проверить задания"), KeyboardButton(text="💰 Проверить вывод")],
            [KeyboardButton(text="⚙️ Администраторские настройки"), KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def get_tasks_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➡️ Выполнить задание"), KeyboardButton(text="Мои выполненные задания")],
            [KeyboardButton(text="🏆 Топ заданий")],
            [KeyboardButton(text:"⬅️ Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def get_tasks_admin_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить задание"), KeyboardButton(text="❌ Удалить задание")],
            [KeyboardButton(text="⬅️ Назад в админ-панель")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def get_mining_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⛏️ Начать майнинг"), KeyboardButton(text="📈 Улучшить майнер")],
            [KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def get_settings_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔗 Привязать BTC кошелек")],
            [KeyboardButton(text="🔗 Привязать TRC20 кошелек")],
            [KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def get_admin_settings_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔧 Техперерыв Вкл"), KeyboardButton(text="🔧 Техперерыв Выкл")],
            [KeyboardButton(text="⬅️ Назад в админ-панель")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def get_withdrawal_currency_kb():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="USDT", callback_data="withdraw_usdt"))
    builder.add(InlineKeyboardButton(text="BTC", callback_data="withdraw_btc"))
    builder.add(InlineKeyboardButton(text="TRC20", callback_data="withdraw_trc20"))
    return builder.as_markup()

def get_admin_decision_kb(proof_id: int):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_proof_{proof_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_proof_{proof_id}")
    )
    return builder.as_markup()

def get_admin_withdrawal_decision_kb(withdrawal_id: int):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_withdrawal_{withdrawal_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_withdrawal_{withdrawal_id}")
    )
    return builder.as_markup()

def get_back_to_main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⬅️ Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True # Часто удобно, чтобы она скрывалась после использования
    )

# --- ОБРАБОТЧИКИ СООБЩЕНИЙ ---

@dp.message(CommandStart(deep_link=True))
@error_handler_decorator
async def cmd_start_deep_link(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    referrer_id = None
    invited_by_link = None

    if command.args:
        try:
            referrer_id = int(command.args)
            if referrer_id == user_id: # Пользователь не может быть реферером для самого себя
                referrer_id = None
            else:
                logger.info(f"Пользователь {user_id} пришел по реферальной ссылке от {referrer_id}")
        except ValueError:
            invited_by_link = command.args
            logger.info(f"Пользователь {user_id} пришел по кастомной ссылке: {invited_by_link}")

    await db_add_user(user_id, username, referrer_id, invited_by_link)

    start_message = (
        f"👋 Добро пожаловать, {message.from_user.full_name}!\n\n"
        "Я ваш Crypto Bot для майнинга и заработка криптовалюты.\n"
        "Выберите действие в меню ниже."
    )
    await message.answer(start_message, reply_markup=get_main_kb())

@dp.message(CommandStart())
@error_handler_decorator
async def cmd_start_no_deep_link(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.full_name
    await db_add_user(user_id, username)

    start_message = (
        f"👋 Добро пожаловать, {message.from_user.full_name}!\n\n"
        "Я ваш Crypto Bot для майнинга и заработка криптовалюты.\n"
        "Выберите действие в меню ниже."
    )
    await message.answer(start_message, reply_markup=get_main_kb())


@dp.message(F.text == "⬅️ Назад")
@error_handler_decorator
async def go_back(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.clear()
    await message.answer("Вы вернулись в главное меню.", reply_markup=get_main_kb())

@dp.message(F.text == "⬅️ Назад в админ-панель")
@error_handler_decorator
async def go_back_to_admin(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    await message.answer("Вы вернулись в админ-панель.", reply_markup=get_admin_kb())

# --- ОБРАБОТЧИКИ ГЛАВНОГО МЕНЮ ---

@dp.message(F.text == "💎 Майнинг")
@error_handler_decorator
async def mining_menu(message: types.Message):
    user = await db_get_user(message.from_user.id)
    if not user:
        await message.answer("Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        return

    mining_info = await db_get_user_mining_upgrade(message.from_user.id)
    current_level = mining_info['level'] if mining_info else user['mining_level']
    current_income = mining_info['income'] if mining_info else user['mining_income']

    text = (
        "<b>💎 Майнинг</b>\n\n"
        f"Ваш текущий уровень майнинга: <b>{current_level}</b>\n"
        f"Доход в час: <b>{current_income:.6f} BTC</b>\n\n"
        "Выберите действие:"
    )
    await message.answer(text, reply_markup=get_mining_kb())

@dp.message(F.text == "⛏️ Начать майнинг")
@error_handler_decorator
async def start_mining(message: types.Message):
    user_id = message.from_user.id
    user = await db_get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        return

    last_mine_time = user.get('last_mine_time')
    current_income = user.get('mining_income', 0.001)

    now_utc = datetime.now(timezone.utc)

    if last_mine_time:
        # Если last_mine_time - наивная дата, сделаем её осведомленной о UTC
        if last_mine_time.tzinfo is None:
            last_mine_time = last_mine_time.replace(tzinfo=timezone.utc)
        
        time_since_last_mine = now_utc - last_mine_time
        if time_since_last_mine < timedelta(hours=1):
            remaining_time = timedelta(hours=1) - time_since_last_mine
            minutes = int(remaining_time.total_seconds() // 60)
            seconds = int(remaining_time.total_seconds() % 60)
            await message.answer(f"⏳ Вы сможете майнить снова через {minutes} мин. {seconds} сек.")
            return

    # Рассчитываем доход
    # Доход за час * количество часов (или 1 час, если прошло больше часа)
    earned_amount = current_income # Сейчас всегда за 1 час
    await db_update_user_balance(user_id, earned_amount)
    await db_update_user_last_mine_time(user_id, now_utc)

    await message.answer(f"✅ Вы успешно намайнили <b>{earned_amount:.6f} BTC</b>. Ваш баланс обновлен.", reply_markup=get_mining_kb())


@dp.message(F.text == "📈 Улучшить майнер")
@error_handler_decorator
async def upgrade_miner(message: types.Message):
    user_id = message.from_user.id
    user = await db_get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        return

    current_level = user.get('mining_level', 1)
    current_income = user.get('mining_income', 0.001)
    user_balance = user.get('balance', 0.0)

    # Логика для стоимости и нового дохода
    next_level = current_level + 1
    upgrade_cost = current_level * 0.0005 # Пример: 1 -> 0.0005, 2 -> 0.001, 3 -> 0.0015
    new_income = current_income + 0.0001 # Пример: 0.001 -> 0.0011 -> 0.0012

    if user_balance < upgrade_cost:
        await message.answer(f"❌ У вас недостаточно BTC для улучшения майнера. Для уровня {next_level} требуется {upgrade_cost:.6f} BTC.")
        return

    # Вычитаем стоимость и обновляем уровень
    await db_update_user_balance(user_id, -upgrade_cost) # Вычитаем стоимость
    await db_update_user_mining_upgrade(user_id, next_level, new_income, upgrade_cost)
    # Также обновляем в users таблице для консистентности, хотя mining_upgrades - основной источник
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET mining_level = $1, mining_income = $2 WHERE user_id = $3", next_level, new_income, user_id)

    await message.answer(
        f"✅ Ваш майнер улучшен до <b>уровня {next_level}</b>!\n"
        f"Новый доход в час: <b>{new_income:.6f} BTC</b>.\n"
        f"С вашего баланса списано: <b>{upgrade_cost:.6f} BTC</b>."
    )


@dp.message(F.text == "👥 Рефералы")
@error_handler_decorator
async def referrals_menu(message: types.Message):
    user_id = message.from_user.id
    user = await db_get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        return

    referral_count = await db_get_referrals_count(user_id)
    referral_link = f"https://t.me/{bot.me.username}?start={user_id}"

    text = (
        "<b>👥 Рефералы</b>\n\n"
        f"Ваша реферальная ссылка: <code>{referral_link}</code>\n\n"
        f"Приглашенных пользователей: <b>{referral_count}</b>\n\n"
        "Делитесь своей ссылкой и получайте бонусы за каждого нового пользователя!"
    )
    await message.answer(text, reply_markup=get_main_kb())


@dp.message(F.text == "💰 Баланс")
@error_handler_decorator
async def balance_menu(message: types.Message):
    user_id = message.from_user.id
    user = await db_get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        return

    btc_balance = user.get('balance', 0.0)
    usdt_balance = user.get('usdt_balance', 0.0)

    text = (
        "<b>💰 Ваш баланс:</b>\n\n"
        f"BTC: <b>{btc_balance:.6f}</b>\n"
        f"USDT (TRC20): <b>{usdt_balance:.2f}</b>\n\n"
        "Выберите действие:"
    )
    await message.answer(text, reply_markup=get_balance_kb())


@dp.message(F.text == "➕ Пополнить")
@error_handler_decorator
async def deposit_menu(message: types.Message):
    if not CRYPTO_BOT_TOKEN:
        await message.answer("Функция пополнения временно недоступна. Пожалуйста, свяжитесь с администратором.", reply_markup=get_balance_kb())
        return

    user_id = message.from_user.id
    description = f"Пополнение баланса пользователя {user_id}"

    # Пример создания инвойса на 0.1 USDT
    invoice = await crypto_bot_create_invoice(asset="USDT", amount=0.1, user_id=user_id, description=description)

    if invoice and 'mini_app_invoice_url' in invoice: # Используем mini_app_invoice_url
        mini_app_url = invoice['mini_app_invoice_url']
        keyboard = InlineKeyboardBuilder()
        keyboard.add(InlineKeyboardButton(text="Перейти к оплате", url=mini_app_url))
        await message.answer(
            f"Для пополнения баланса USDT (TRC20) на 0.1 USDT, перейдите по ссылке:\n\n"
            f"🔗 <a href='{mini_app_url}'>Оплатить 0.1 USDT</a>\n\n"
            "После успешной оплаты ваш баланс будет автоматически обновлен.",
            reply_markup=keyboard.as_markup(),
            disable_web_page_preview=True
        )
        logger.info(f"Сгенерирован инвойс {invoice['invoice_id']} для пользователя {user_id}.")
    else:
        await message.answer("❌ Не удалось создать инвойс для пополнения. Пожалуйста, попробуйте позже.", reply_markup=get_balance_kb())


@dp.message(F.text == "➖ Вывести")
@error_handler_decorator
async def withdraw_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user = await db_get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        return

    btc_balance = user.get('balance', 0.0)
    usdt_balance = user.get('usdt_balance', 0.0)
    btc_address = user.get('btc_address')
    trc20_address = user.get('trc20_address') # TRC20_address для USDT TRC20

    text = (
        "<b>➖ Вывод средств</b>\n\n"
        f"Ваш текущий баланс BTC: <b>{btc_balance:.6f}</b>\n"
        f"Ваш текущий баланс USDT (TRC20): <b>{usdt_balance:.2f}</b>\n\n"
        f"Привязанный BTC кошелек: <code>{btc_address if btc_address else 'Не привязан'}</code>\n"
        f"Привязанный TRC20 кошелек: <code>{trc20_address if trc20_address else 'Не привязан'}</code>\n\n"
        "Выберите, в какой валюте хотите вывести средства:"
    )
    await message.answer(text, reply_markup=get_withdrawal_currency_kb())
    await state.set_state(WithdrawState.waiting_for_currency)

@dp.callback_query(WithdrawState.waiting_for_currency, F.data.startswith("withdraw_"))
@error_handler_decorator
async def withdraw_currency_selected(callback_query: types.CallbackQuery, state: FSMContext):
    currency = callback_query.data.split('_')[1].upper()
    user_id = callback_query.from_user.id
    user = await db_get_user(user_id)
    await callback_query.answer() # Отвечаем на callback_query, чтобы убрать "часики"

    if not user:
        await bot.send_message(user_id, "Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        await state.clear()
        return

    min_amount = 0
    balance = 0
    address = None

    if currency == "BTC":
        balance = user.get('balance', 0.0)
        min_amount = min_withdrawal_amount_btc
        address = user.get('btc_address')
        currency_display = "BTC"
    elif currency == "USDT" or currency == "TRC20": # USDT TRC20
        balance = user.get('usdt_balance', 0.0)
        min_amount = min_withdrawal_amount_usdt if currency == "USDT" else min_withdrawal_amount_trc20 # Если у вас разные лимиты для USDT и TRC20
        address = user.get('trc20_address') # Адрес TRC20 для вывода USDT/TRC20
        currency_display = "USDT (TRC20)"
    else:
        await bot.send_message(user_id, "Неизвестная валюта для вывода. Пожалуйста, выберите из предложенных.", reply_markup=get_balance_kb())
        await state.clear()
        return

    if not address:
        await bot.send_message(user_id,
            f"❌ У вас не привязан кошелек для вывода {currency_display}. "
            "Пожалуйста, привяжите его в разделе '⚙️ Настройки'.",
            reply_markup=get_settings_kb() # Возможно, лучше main_kb, если пользователь не в состоянии
        )
        await state.clear()
        return

    await state.update_data(withdrawal_currency=currency, withdrawal_address=address)
    await state.set_state(WithdrawState.waiting_for_amount)
    await bot.send_message(
        user_id,
        f"Введите сумму для вывода в {currency_display}. Ваш баланс: <b>{balance:.6f if currency == 'BTC' else balance:.2f}</b>.\n"
        f"Минимальная сумма вывода: <b>{min_amount:.6f if currency == 'BTC' else min_amount:.2f}</b>.",
        reply_markup=get_back_to_main_kb() # Можно дать кнопку "Назад"
    )

@dp.message(WithdrawState.waiting_for_amount)
@error_handler_decorator
async def withdraw_amount_received(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_data = await state.get_data()
    currency = user_data['withdrawal_currency']
    address = user_data['withdrawal_address']

    try:
        amount = float(message.text)
    except ValueError:
        await message.answer("❌ Сумма должна быть числом. Попробуйте еще раз.")
        return

    user = await db_get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        await state.clear()
        return

    balance = user.get('balance', 0.0) if currency == "BTC" else user.get('usdt_balance', 0.0)
    min_amount = min_withdrawal_amount_btc if currency == "BTC" else (min_withdrawal_amount_usdt if currency == "USDT" else min_withdrawal_amount_trc20)

    if amount <= 0:
        await message.answer("❌ Сумма вывода должна быть положительной. Попробуйте еще раз.")
        return
    if amount < min_amount:
        await message.answer(f"❌ Сумма вывода должна быть не менее {min_amount:.6f if currency == 'BTC' else min_amount:.2f} {currency}. Попробуйте еще раз.")
        return
    if amount > balance:
        await message.answer(f"❌ Недостаточно средств на балансе. У вас: {balance:.6f if currency == 'BTC' else balance:.2f} {currency}. Попробуйте еще раз.")
        return

    # Записываем запрос на вывод
    withdrawal_id = await db_add_withdrawal_request(user_id, amount, currency, address)

    # Вычитаем из баланса пользователя сразу
    if currency == "BTC":
        await db_update_user_balance(user_id, -amount)
    elif currency == "USDT" or currency == "TRC20":
        await db_update_user_usdt_balance(user_id, -amount)

    await message.answer(
        f"✅ Запрос на вывод {amount:.6f if currency == 'BTC' else amount:.2f} {currency} на адрес <code>{address}</code> создан (ID: {withdrawal_id}).\n"
        "Администратор рассмотрит вашу заявку в ближайшее время.",
        reply_markup=get_balance_kb()
    )
    await state.clear()

    # Уведомление админов
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"🔔 НОВЫЙ ЗАПРОС НА ВЫВОД!\n"
                f"Пользователь: {message.from_user.full_name} (ID: {user_id})\n"
                f"Сумма: {amount:.6f if currency == 'BTC' else amount:.2f} {currency}\n"
                f"Адрес: <code>{address}</code>\n"
                f"ID запроса: {withdrawal_id}",
                reply_markup=get_admin_withdrawal_decision_kb(withdrawal_id)
            )
        except TelegramForbiddenError:
            logger.warning(f"Админ {admin_id} заблокировал бота.")
        except Exception as e:
            logger.error(f"Ошибка при уведомлении админа {admin_id} о выводе: {e}")

@dp.message(F.text == "ℹ️ Информация")
@error_handler_decorator
async def info_menu(message: types.Message):
    info_text = (
        "<b>ℹ️ Информация о боте</b>\n\n"
        "Этот бот позволяет вам 'майнить' виртуальную криптовалюту, выполнять задания и приглашать друзей для получения бонусов.\n\n"
        "💎 <b>Майнинг</b>: Автоматически генерирует доход с течением времени. Вы можете улучшать свой майнер для увеличения прибыли.\n"
        "🚀 <b>Задания</b>: Выполняйте простые задачи и получайте награды.\n"
        "👥 <b>Рефералы</b>: Приглашайте новых пользователей по своей уникальной ссылке и получайте бонусы.\n"
        "💰 <b>Баланс</b>: Отслеживайте свой баланс и управляйте пополнениями/выводами.\n\n"
        "<b>Правила и условия:</b>\n"
        "1. Запрещено использовать любые виды накрутки или обмана.\n"
        "2. Все выплаты обрабатываются вручную администратором. Сроки обработки могут варьироваться.\n"
        "3. В случае обнаружения нарушений, администрация оставляет за собой право заблокировать аккаунт без объяснения причин.\n\n"
        "По всем вопросам обращайтесь к администратору: @UsernameAdmin" # Замените на реальное имя админа
    )
    await message.answer(info_text, reply_markup=get_main_kb())

@dp.message(F.text == "⚙️ Настройки")
@error_handler_decorator
async def settings_menu(message: types.Message):
    user_id = message.from_user.id
    user = await db_get_user(user_id)
    if not user:
        await message.answer("Пользователь не найден. Пожалуйста, перезапустите бота /start.", reply_markup=get_main_kb())
        return

    btc_address = user.get('btc_address')
    trc20_address = user.get('trc20_address')

    text = (
        "<b>⚙️ Настройки</b>\n\n"
        f"Ваш привязанный BTC кошелек: <code>{btc_address if btc_address else 'Не привязан'}</code>\n"
        f"Ваш привязанный TRC20 кошелек: <code>{trc20_address if trc20_address else 'Не привязан'}</code>\n\n"
        "Выберите действие:"
    )
    await message.answer(text, reply_markup=get_settings_kb())

@dp.message(F.text == "🔗 Привязать BTC кошелек")
@error_handler_decorator
async def link_btc_wallet(message: types.Message, state: FSMContext):
    await message.answer("Пожалуйста, отправьте ваш BTC адрес (для вывода средств):", reply_markup=get_back_to_main_kb())
    await state.set_state(Form.btc_address)

@dp.message(Form.btc_address)
@error_handler_decorator
async def process_btc_address(message: types.Message, state: FSMContext):
    btc_address = message.text.strip()
    # Простая валидация (можно улучшить)
    if len(btc_address) < 26 or len(btc_address) > 35: # Примерная длина BTC адреса
        await message.answer("❌ Похоже на неверный BTC адрес. Пожалуйста, попробуйте еще раз.")
        return

    await db_update_user_btc_address(message.from_user.id, btc_address)
    await message.answer(f"✅ Ваш BTC кошелек <code>{btc_address}</code> успешно привязан!", reply_markup=get_settings_kb())
    await state.clear()

@dp.message(F.text == "🔗 Привязать TRC20 кошелек")
@error_handler_decorator
async def link_trc20_wallet(message: types.Message, state: FSMContext):
    await message.answer("Пожалуйста, отправьте ваш TRC20 адрес (для вывода средств USDT TRC20):", reply_markup=get_back_to_main_kb())
    await state.set_state(Form.trc20_address)

@dp.message(Form.trc20_address)
@error_handler_decorator
async def process_trc20_address(message: types.Message, state: FSMContext):
    trc20_address = message.text.strip()
    # TRC20 адреса начинаются с 'T' и имеют длину 34 символа
    if not trc20_address.startswith('T') or len(trc20_address) != 34:
        await message.answer("❌ Похоже на неверный TRC20 адрес (должен начинаться с 'T' и быть 34 символа). Пожалуйста, попробуйте еще раз.")
        return

    await db_update_user_trc20_address(message.from_user.id, trc20_address)
    await message.answer(f"✅ Ваш TRC20 кошелек <code>{trc20_address}</code> успешно привязан!", reply_markup=get_settings_kb())
    await state.clear()


# --- ОБРАБОТЧИКИ ЗАДАНИЙ ---

@dp.message(F.text == "🚀 Задания")
@error_handler_decorator
async def tasks_menu(message: types.Message):
    await message.answer("Добро пожаловать в раздел заданий! Выберите действие:", reply_markup=get_tasks_kb())

@dp.message(F.text == "➡️ Выполнить задание") # Исправлено для точного совпадения с текстом кнопки
@error_handler_decorator
async def start_task_completion(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    active_tasks = await db_get_active_tasks()
    user_completed_tasks = await db_get_user_completed_tasks(user_id)
    completed_task_ids = {task['task_id'] for task in user_completed_tasks}

    available_tasks = [
        task for task in active_tasks
        if task['task_id'] not in completed_task_ids
    ]

    if not available_tasks:
        await message.answer("Пока нет доступных заданий для выполнения. Пожалуйста, попробуйте позже.", reply_markup=get_tasks_kb())
        await state.clear()
        return

    text = "Выберите номер задания, которое хотите выполнить:\n\n"
    keyboard_builder = InlineKeyboardBuilder()

    for task in available_tasks:
        text += (
            f"<b>Задание #{task['task_id']}: {task['task_name']}</b>\n"
            f"Описание: {task['task_description']}\n"
            f"Награда: <b>{task['reward']:.2f} BTC</b>\n\n"
        )
        keyboard_builder.add(InlineKeyboardButton(text=f"Выбрать #{task['task_id']}", callback_data=f"select_task_{task['task_id']}"))

    keyboard_builder.adjust(2) # Размещаем кнопки по 2 в ряд
    await message.answer(text, reply_markup=keyboard_builder.as_markup())
    await state.set_state(Form.task_id)


@dp.callback_query(Form.task_id, F.data.startswith("select_task_"))
@error_handler_decorator
async def process_selected_task(callback_query: types.CallbackQuery, state: FSMContext):
    task_id = int(callback_query.data.split('_')[2])
    user_id = callback_query.from_user.id
    await callback_query.answer() # Отвечаем на callback_query

    task = await db_get_task(task_id)
    if not task or task['status'] != 'active':
        await bot.send_message(user_id, "❌ Это задание неактивно или не существует.")
        await state.clear()
        return

    if await db_has_completed_task(user_id, task_id):
        await bot.send_message(user_id, "Вы уже выполнили это задание ранее.")
        await state.clear()
        return

    await state.update_data(task_id=task_id)
    await bot.send_message(
        user_id,
        f"Вы выбрали задание #{task_id}: <b>{task['task_name']}</b>.\n"
        "Отправьте подтверждение выполнения (текст и/или фото):",
        reply_markup=get_back_to_main_kb()
    )
    await state.set_state(Form.proof_text)


@dp.message(Form.proof_text)
@error_handler_decorator
async def process_proof_text(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer("Действие отменено. Выберите действие:", reply_markup=get_tasks_kb())
        return

    proof_text = message.text
    user_data = await state.get_data()
    task_id = user_data['task_id']

    await state.update_data(proof_text=proof_text)
    await message.answer("Теперь, если требуется, отправьте фото-доказательство выполнения (или нажмите '⬅️ Назад', если фото не нужно):", reply_markup=get_back_to_main_kb())
    await state.set_state(Form.proof_photo)

@dp.message(Form.proof_photo, F.photo | F.text == "⬅️ Назад") # Можно принимать фото ИЛИ команду "Назад"
@error_handler_decorator
async def process_proof_photo(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "⬅️ Назад":
        user_data = await state.get_data()
        task_id = user_data['task_id']
        proof_text = user_data.get('proof_text')
        proof_photo_id = None # Нет фото

        proof_id = await db_add_task_proof(user_id, task_id, proof_text=proof_text, proof_photo_id=proof_photo_id)
        await message.answer(
            f"✅ Ваше подтверждение для задания #{task_id} (ID: {proof_id}) отправлено на проверку.\n"
            "Вы получите уведомление после проверки.",
            reply_markup=get_tasks_kb()
        )
        await state.clear()
        # Уведомление админов
        await notify_admins_new_proof(proof_id, user_id, task_id)
        return

    if message.photo:
        proof_photo_id = message.photo[-1].file_id # Берем самое большое фото
        user_data = await state.get_data()
        task_id = user_data['task_id']
        proof_text = user_data.get('proof_text')

        proof_id = await db_add_task_proof(user_id, task_id, proof_text=proof_text, proof_photo_id=proof_photo_id)
        await message.answer(
            f"✅ Ваше подтверждение для задания #{task_id} (ID: {proof_id}) отправлено на проверку.\n"
            "Вы получите уведомление после проверки.",
            reply_markup=get_tasks_kb()
        )
        await state.clear()
        # Уведомление админов
        await notify_admins_new_proof(proof_id, user_id, task_id)
    else:
        await message.answer("Пожалуйста, отправьте фото или нажмите '⬅️ Назад', если фото не требуется.")


@dp.message(F.text == "Мои выполненные задания")
@error_handler_decorator
async def show_my_completed_tasks(message: types.Message):
    user_id = message.from_user.id
    completed_tasks = await db_get_user_completed_tasks(user_id)

    if not completed_tasks:
        await message.answer("Вы еще не выполнили ни одного задания.", reply_markup=get_tasks_kb())
        return

    text = "<b>Ваши выполненные задания:</b>\n\n"
    for comp_task in completed_tasks:
        task_info = await db_get_task(comp_task['task_id'])
        if task_info:
            completion_date_str = comp_task['completion_date'].strftime('%d.%m.%Y %H:%M') if comp_task['completion_date'] else 'Неизвестно'
            text += f"▪️ <b>{task_info['task_name']}</b> (ID: {task_info['task_id']})\n"
            text += f"   <i>Выполнено: {completion_date_str}</i>\n"
            text += f"   <i>Награда: {task_info['reward']:.2f} BTC</i>\n\n"
        else:
            text += f"▪️ Задание с ID {comp_task['task_id']} (информация не найдена)\n"

    await message.answer(text, reply_markup=get_tasks_kb())

@dp.message(F.text == "🏆 Топ заданий")
@error_handler_decorator
async def top_tasks_menu(message: types.Message):
    # Временное решение - можно сделать отдельную функцию для расчета топа
    # Пока что просто заглушка или вывод общей инфы
    text = (
        "<b>🏆 Топ заданий</b>\n\n"
        "Эта функция пока в разработке. Здесь будет отображаться рейтинг пользователей по количеству выполненных заданий."
    )
    await message.answer(text, reply_markup=get_tasks_kb())


# --- АДМИН-ФУНКЦИИ ---

@dp.message(F.text == "⚙️ Администраторские настройки")
@error_handler_decorator
async def admin_settings_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Администраторские настройки:", reply_markup=get_admin_settings_kb())


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

@dp.message(F.text == "📊 Статистика")
@error_handler_decorator
async def send_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    users_data = await db_get_all_users_data()
    tasks_data = await db_get_all_tasks_data()
    proofs_data = await db_get_all_task_proofs_data()
    withdrawals_data = await db_get_all_withdrawals_data()

    total_users = len(users_data)
    total_balance_btc = sum(u['balance'] for u in users_data)
    total_balance_usdt = sum(u['usdt_balance'] for u in users_data)
    
    pending_proofs_count = sum(1 for p in proofs_data if p['status'] == 'pending')
    pending_withdrawals_count = sum(1 for w in withdrawals_data if w['status'] == 'pending')

    stats_text = (
        "<b>📊 Статистика бота:</b>\n\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"💰 Общий баланс BTC: {total_balance_btc:.6f}\n"
        f"💰 Общий баланс USDT (TRC20): {total_balance_usdt:.2f}\n"
        f"🚀 Всего активных заданий: {len([t for t in tasks_data if t['status'] == 'active'])}\n"
        f"✅ Ожидают проверки заданий: {pending_proofs_count}\n"
        f"💸 Ожидают вывода средств: {pending_withdrawals_count}\n"
    )

    # Optional: Generate Excel reports
    output_xlsx = io.BytesIO()
    with pd.ExcelWriter(output_xlsx, engine='xlsxwriter') as writer:
        pd.DataFrame(users_data).to_excel(writer, sheet_name='Users', index=False)
        pd.DataFrame(tasks_data).to_excel(writer, sheet_name='Tasks', index=False)
        pd.DataFrame(proofs_data).to_excel(writer, sheet_name='Proofs', index=False)
        pd.DataFrame(withdrawals_data).to_excel(writer, sheet_name='Withdrawals', index=False)

    output_xlsx.seek(0)
    
    # Get Crypto Bot balance if token is set
    crypto_balances = None
    if CRYPTO_BOT_TOKEN:
        crypto_balances = await crypto_bot_get_balance()
        if crypto_balances:
            stats_text += "\n<b>Баланс Crypto Bot:</b>\n"
            for asset, amount in crypto_balances.items():
                stats_text += f"- {asset}: {amount}\n"
        else:
            stats_text += "\n<i>Не удалось получить баланс Crypto Bot.</i>\n"


    await message.answer(stats_text, reply_markup=get_admin_kb())
    await bot.send_document(
        message.from_user.id,
        BufferedInputFile(output_xlsx.getvalue(), filename="bot_data.xlsx"),
        caption="Полная статистика в Excel файле."
    )


@dp.message(F.text == "✍️ Объявление")
@error_handler_decorator
async def start_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите текст объявления, который будет отправлен всем пользователям:", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminBroadcastState.waiting_for_message)

@dp.message(AdminBroadcastState.waiting_for_message)
@error_handler_decorator
async def process_broadcast_message(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer("Отправка объявления отменена.", reply_markup=get_admin_kb())
        return

    broadcast_text = message.html_text # Используем html_text для сохранения форматирования
    user_ids = await db_get_all_users_ids()
    sent_count = 0
    blocked_count = 0

    for user_id in user_ids:
        try:
            await bot.send_message(user_id, broadcast_text)
            sent_count += 1
            await asyncio.sleep(0.05) # Небольшая задержка, чтобы избежать лимитов Telegram
        except TelegramForbiddenError:
            blocked_count += 1
            logger.warning(f"Пользователь {user_id} заблокировал бота.")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")

    await message.answer(
        f"✅ Объявление отправлено {sent_count} пользователям.\n"
        f"❌ Не удалось отправить {blocked_count} пользователям (вероятно, заблокировали бота).",
        reply_markup=get_admin_kb()
    )
    await state.clear()


@dp.message(F.text == "➕ Добавить задание")
@error_handler_decorator
async def admin_add_task_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите название нового задания:", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminAddTaskState.waiting_for_name)

@dp.message(AdminAddTaskState.waiting_for_name)
@error_handler_decorator
async def admin_add_task_name(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer("Добавление задания отменено.", reply_markup=get_admin_kb())
        return
    await state.update_data(task_name=message.text)
    await message.answer("Введите описание задания:", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminAddTaskState.waiting_for_description)

@dp.message(AdminAddTaskState.waiting_for_description)
@error_handler_decorator
async def admin_add_task_description(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer("Добавление задания отменено.", reply_markup=get_admin_kb())
        return
    await state.update_data(task_description=message.text)
    await message.answer("Введите награду за задание (число, например, 0.0001 BTC):", reply_markup=get_back_to_main_kb())
    await state.set_state(AdminAddTaskState.waiting_for_reward)

@dp.message(AdminAddTaskState.waiting_for_reward)
@error_handler_decorator
async def admin_add_task_reward(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer("Добавление задания отменено.", reply_markup=get_admin_kb())
        return
    try:
        reward = float(message.text)
        if reward <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Награда должна быть положительным числом. Попробуйте еще раз.")
        return

    user_data = await state.get_data()
    task_name = user_data['task_name']
    task_description = user_data['task_description']

    task_id = await db_add_task(task_name, task_description, reward)
    await message.answer(f"✅ Задание '{task_name}' (ID: {task_id}) успешно добавлено с наградой {reward:.6f} BTC.", reply_markup=get_admin_kb())
    await state.clear()


@dp.message(F.text == "❌ Удалить задание")
@error_handler_decorator
async def admin_delete_task_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    active_tasks = await db_get_active_tasks()
    if not active_tasks:
        await message.answer("Активных заданий для удаления нет.", reply_markup=get_admin_kb())
        return

    text = "Введите ID задания, которое хотите удалить:\n\n"
    for task in active_tasks:
        text += f"ID: {task['task_id']} | Название: {task['task_name']}\n"
    await message.answer(text, reply_markup=get_back_to_main_kb())
    await state.set_state(DeleteTaskState.waiting_for_task_number)

@dp.message(DeleteTaskState.waiting_for_task_number)
@error_handler_decorator
async def admin_delete_task_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer("Удаление задания отменено.", reply_markup=get_admin_kb())
        return
    try:
        task_id = int(message.text)
        task = await db_get_task(task_id)
        if not task:
            await message.answer("❌ Задание с таким ID не найдено. Попробуйте еще раз.")
            return
        await db_delete_task(task_id)
        await message.answer(f"✅ Задание с ID {task_id} успешно удалено.", reply_markup=get_admin_kb())
    except ValueError:
        await message.answer("❌ ID задания должен быть числом. Попробуйте еще раз.")
    finally:
        await state.clear()

@dp.message(F.text == "✅ Проверить задания")
@error_handler_decorator
async def admin_check_proofs(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    pending_proofs = await db_get_pending_proofs()
    if not pending_proofs:
        await message.answer("Нет заданий, ожидающих проверки.", reply_markup=get_admin_kb())
        return

    proof_list_text = "<b>Задания, ожидающие проверки:</b>\n\n"
    for proof in pending_proofs:
        task = await db_get_task(proof['task_id'])
        user = await db_get_user(proof['user_id'])
        username = user['username'] if user and user['username'] else 'Неизвестный пользователь'
        task_name = task['task_name'] if task else 'Неизвестное задание'
        submission_date = proof['submission_date'].strftime('%d.%m.%Y %H:%M') if proof['submission_date'] else 'Неизвестно'

        proof_list_text += (
            f"<b>ID подтверждения:</b> {proof['proof_id']}\n"
            f"<b>От пользователя:</b> @{username} (ID: {proof['user_id']})\n"
            f"<b>Задание:</b> {task_name} (ID: {proof['task_id']})\n"
            f"<b>Текст:</b> {proof['proof_text'] if proof['proof_text'] else 'Нет текста'}\n"
            f"<b>Дата:</b> {submission_date}\n"
        )
        if proof['proof_photo_id']:
            proof_list_text += "Фото: [см. следующее сообщение]\n"
        proof_list_text += "-----------------------------------\n\n"

    await message.answer(proof_list_text)
    await asyncio.sleep(0.5) # Небольшая пауза, чтобы сообщения не сливались

    for proof in pending_proofs:
        if proof['proof_photo_id']:
            try:
                await bot.send_photo(message.from_user.id, proof['proof_photo_id'], caption=f"Фото для подтверждения ID: {proof['proof_id']}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Не удалось отправить фото {proof['proof_photo_id']} админу {message.from_user.id}: {e}")
                await bot.send_message(message.from_user.id, f"Ошибка при отправке фото для подтверждения ID {proof['proof_id']}. Возможно, фото удалено или недоступно.")

    await message.answer("Введите ID подтверждения, которое хотите проверить:", reply_markup=get_back_to_main_kb())
    await state.set_state(ApproveTaskState.waiting_for_proof_id)

@dp.message(ApproveTaskState.waiting_for_proof_id)
@error_handler_decorator
async def admin_process_proof_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer("Проверка заданий отменена.", reply_markup=get_admin_kb())
        return
    try:
        proof_id = int(message.text)
        proof = await db_get_proof(proof_id)
        if not proof or proof['status'] != 'pending':
            await message.answer("❌ Подтверждение с таким ID не найдено или уже обработано. Попробуйте еще раз.")
            return

        user = await db_get_user(proof['user_id'])
        task = await db_get_task(proof['task_id'])
        username = user['username'] if user and user['username'] else 'Неизвестный пользователь'
        task_name = task['task_name'] if task else 'Неизвестное задание'
        reward = task['reward'] if task else 0.0

        confirmation_text = (
            f"<b>Проверка подтверждения ID: {proof_id}</b>\n\n"
            f"От пользователя: @{username} (ID: {proof['user_id']})\n"
            f"Задание: {task_name} (ID: {proof['task_id']})\n"
            f"Награда за задание: <b>{reward:.6f} BTC</b>\n"
            f"Текст: {proof['proof_text'] if proof['proof_text'] else 'Нет текста'}\n"
        )
        if proof['proof_photo_id']:
            await bot.send_photo(message.from_user.id, proof['proof_photo_id'], caption=confirmation_text, reply_markup=get_admin_decision_kb(proof_id))
        else:
            await message.answer(confirmation_text, reply_markup=get_admin_decision_kb(proof_id))

        await state.set_state(ApproveTaskState.waiting_for_admin_decision)
    except ValueError:
        await message.answer("❌ ID подтверждения должен быть числом. Попробуйте еще раз.")


@dp.callback_query(ApproveTaskState.waiting_for_admin_decision, F.data.startswith("approve_proof_") | F.data.startswith("reject_proof_"))
@error_handler_decorator
async def admin_decision_proof(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет прав для этого действия.")
        return

    action = callback_query.data.split('_')[0]
    proof_id = int(callback_query.data.split('_')[2])
    await callback_query.answer() # Отвечаем на callback_query

    proof = await db_get_proof(proof_id)
    if not proof or proof['status'] != 'pending':
        await bot.send_message(callback_query.from_user.id, "Это подтверждение уже обработано или не существует.")
        await state.clear()
        return

    user_id = proof['user_id']
    task_id = proof['task_id']
    task = await db_get_task(task_id)
    reward = task['reward'] if task else 0.0

    if action == "approve":
        # Проверяем, не выполнил ли пользователь это задание уже одобренное
        if await db_has_completed_task(user_id, task_id):
            await bot.send_message(callback_query.from_user.id, f"⚠️ Пользователь {user_id} уже имеет одобренное выполнение задания {task_id}. Не начислен повторно.")
            await db_set_proof_status(proof_id, 'rejected') # Помечаем текущее как отклоненное, чтобы не висело
            await bot.send_message(user_id, f"❌ Ваше подтверждение для задания #{task_id} было отклонено, так как это задание уже было вами успешно выполнено.")
            await state.clear()
            return

        await db_set_proof_status(proof_id, 'approved')
        await db_update_user_balance(user_id, reward)
        await db_add_completed_task(user_id, task_id) # Добавляем запись о выполненном задании

        await bot.send_message(callback_query.from_user.id, f"✅ Подтверждение ID {proof_id} одобрено. Награда {reward:.6f} BTC начислена пользователю {user_id}.")
        try:
            await bot.send_message(user_id, f"🎉 Ваше подтверждение для задания #{task_id} одобрено! На ваш баланс начислено <b>{reward:.6f} BTC</b>.")
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {user_id} заблокировал бота.")
    else: # reject
        await db_set_proof_status(proof_id, 'rejected')
        await bot.send_message(callback_query.from_user.id, f"❌ Подтверждение ID {proof_id} отклонено.")
        try:
            await bot.send_message(user_id, f"❌ Ваше подтверждение для задания #{task_id} было отклонено. Пожалуйста, внимательно ознакомьтесь с условиями.")
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {user_id} заблокировал бота.")

    await state.clear()
    await bot.send_message(callback_query.from_user.id, "Выберите следующее действие:", reply_markup=get_admin_kb())


async def notify_admins_new_proof(proof_id: int, user_id: int, task_id: int):
    user = await db_get_user(user_id)
    task = await db_get_task(task_id)
    username = user['username'] if user and user['username'] else 'Неизвестный пользователь'
    task_name = task['task_name'] if task else 'Неизвестное задание'

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"🔔 НОВОЕ ПОДТВЕРЖДЕНИЕ ЗАДАНИЯ!\n"
                f"ID подтверждения: {proof_id}\n"
                f"От пользователя: @{username} (ID: {user_id})\n"
                f"Задание: {task_name} (ID: {task_id})\n"
                f"Нажмите '✅ Проверить задания' в админ-панели, чтобы рассмотреть.",
                reply_markup=get_admin_kb()
            )
        except TelegramForbiddenError:
            logger.warning(f"Админ {admin_id} заблокировал бота.")
        except Exception as e:
            logger.error(f"Ошибка при уведомлении админа {admin_id} о подтверждении: {e}")


@dp.message(F.text == "💰 Проверить вывод")
@error_handler_decorator
async def admin_check_withdrawals(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    pending_withdrawals = await db_get_pending_withdrawals()
    if not pending_withdrawals:
        await message.answer("Нет запросов на вывод, ожидающих проверки.", reply_markup=get_admin_kb())
        return

    text = "<b>Запросы на вывод, ожидающие проверки:</b>\n\n"
    for withdraw_req in pending_withdrawals:
        user = await db_get_user(withdraw_req['user_id'])
        username = user['username'] if user and user['username'] else 'Неизвестный пользователь'
        request_date = withdraw_req['request_date'].strftime('%d.%m.%Y %H:%M') if withdraw_req['request_date'] else 'Неизвестно'
        
        text += (
            f"<b>ID запроса:</b> {withdraw_req['withdrawal_id']}\n"
            f"<b>От пользователя:</b> @{username} (ID: {withdraw_req['user_id']})\n"
            f"<b>Сумма:</b> {withdraw_req['amount']:.6f if withdraw_req['currency'] == 'BTC' else withdraw_req['amount']:.2f} {withdraw_req['currency']}\n"
            f"<b>Адрес:</b> <code>{withdraw_req['address']}</code>\n"
            f"<b>Дата:</b> {request_date}\n"
            "-----------------------------------\n\n"
        )
        
    await message.answer(text, reply_markup=get_admin_kb())
    await message.answer("Нажмите на ID запроса для одобрения/отклонения:", reply_markup=get_admin_withdrawal_decision_kb_list(pending_withdrawals))


def get_admin_withdrawal_decision_kb_list(pending_withdrawals):
    builder = InlineKeyboardBuilder()
    for req in pending_withdrawals:
        builder.row(InlineKeyboardButton(text=f"Вывод #{req['withdrawal_id']}", callback_data=f"review_withdrawal_{req['withdrawal_id']}"))
    builder.adjust(2)
    return builder.as_markup()

@dp.callback_query(F.data.startswith("review_withdrawal_"))
@error_handler_decorator
async def admin_review_withdrawal(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет прав для этого действия.")
        return
    
    withdrawal_id = int(callback_query.data.split('_')[2])
    await callback_query.answer()

    withdrawal = await db_get_proof(withdrawal_id) # Это была ошибка, должно быть db_get_withdrawal
    withdrawal = await db_get_withdrawal_request(withdrawal_id) # Исправлено

    if not withdrawal or withdrawal['status'] != 'pending':
        await bot.send_message(callback_query.from_user.id, "Этот запрос уже обработан или не существует.")
        await state.clear()
        return

    user = await db_get_user(withdrawal['user_id'])
    username = user['username'] if user and user['username'] else 'Неизвестный пользователь'
    request_date = withdrawal['request_date'].strftime('%d.%m.%Y %H:%M') if withdrawal['request_date'] else 'Неизвестно'

    text = (
        f"<b>Подробности запроса на вывод #{withdrawal_id}</b>\n\n"
        f"От пользователя: @{username} (ID: {withdrawal['user_id']})\n"
        f"Сумма: {withdrawal['amount']:.6f if withdrawal['currency'] == 'BTC' else withdrawal['amount']:.2f} {withdrawal['currency']}\n"
        f"Адрес: <code>{withdrawal['address']}</code>\n"
        f"Дата запроса: {request_date}\n"
        "Выберите действие:"
    )
    await bot.send_message(callback_query.from_user.id, text, reply_markup=get_admin_withdrawal_decision_kb(withdrawal_id))

# Исправленная функция для получения запроса на вывод
async def db_get_withdrawal_request(withdrawal_id: int):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT withdrawal_id, user_id, amount, currency, address, request_date, status FROM withdrawals WHERE withdrawal_id = $1", withdrawal_id)
        return dict(row) if row else None


@dp.callback_query(F.data.startswith("approve_withdrawal_") | F.data.startswith("reject_withdrawal_"))
@error_handler_decorator
async def admin_decision_withdrawal(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет прав для этого действия.")
        return

    action = callback_query.data.split('_')[0]
    withdrawal_id = int(callback_query.data.split('_')[2])
    await callback_query.answer() # Отвечаем на callback_query

    withdrawal = await db_get_withdrawal_request(withdrawal_id)
    if not withdrawal or withdrawal['status'] != 'pending':
        await bot.send_message(callback_query.from_user.id, "Этот запрос на вывод уже обработан или не существует.")
        await state.clear()
        return

    user_id = withdrawal['user_id']
    amount = withdrawal['amount']
    currency = withdrawal['currency']

    if action == "approve":
        await db_set_withdrawal_status(withdrawal_id, 'approved')
        await bot.send_message(callback_query.from_user.id, f"✅ Запрос на вывод ID {withdrawal_id} одобрен. Пожалуйста, не забудьте произвести фактическую отправку средств.")
        try:
            await bot.send_message(user_id, f"🎉 Ваш запрос на вывод {amount:.6f if currency == 'BTC' else amount:.2f} {currency} одобрен! Ожидайте поступления средств на ваш кошелек.")
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {user_id} заблокировал бота.")
    else: # reject
        await db_set_withdrawal_status(withdrawal_id, 'rejected')
        # Возвращаем средства на баланс пользователя, если запрос отклонен
        if currency == "BTC":
            await db_update_user_balance(user_id, amount)
        elif currency == "USDT" or currency == "TRC20":
            await db_update_user_usdt_balance(user_id, amount)

        await bot.send_message(callback_query.from_user.id, f"❌ Запрос на вывод ID {withdrawal_id} отклонен. Средства возвращены на баланс пользователя.")
        try:
            await bot.send_message(user_id, f"❌ Ваш запрос на вывод {amount:.6f if currency == 'BTC' else amount:.2f} {currency} был отклонен. Средства возвращены на ваш баланс. Пожалуйста, свяжитесь с администратором для уточнения причин.")
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {user_id} заблокировал бота.")

    await state.clear()
    await bot.send_message(callback_query.from_user.id, "Выберите следующее действие:", reply_markup=get_admin_kb())


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
    # Удалите или закомментируйте эту строку, если вы хотите получать все обновления
    # await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, skip_updates=True) # skip_updates=True пропустит все старые необработанные обновления

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную.")
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {e}", exc_info=True)
