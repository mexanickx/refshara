import asyncio
import random
import pandas as pd
import io
import logging
from datetime import datetime, timedelta
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
    exit(1) # Завершаем выполнение, если токен не найден

ADMIN_IDS_STR = os.environ.get("ADMIN_IDS")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',')] if ADMIN_IDS_STR else []
if not ADMIN_IDS:
    logger.warning("ADMIN_IDS не установлены в переменных окружения. Админ-функции будут недоступны.")

CRYPTO_BOT_TOKEN = os.environ.get("CRYPTO_BOT_API_TOKEN")
if not CRYPTO_BOT_TOKEN:
    logger.warning("CRYPTO_BOT_API_TOKEN не установлен. Функции вывода/пополнения не будут работать.")

CRYPTO_BOT_API_URL = 'https://pay.crypt.bot/api/'

# Константы майнинга
MINING_COOLDOWN = 3600  # 1 час в секундах
MINING_REWARD_RANGE = (3, 3)  # Диапазон награды
TASK_REWARD_RANGE = (5, 10)  # Награда за задание
REFERRAL_REWARD = 3  # Награда за приглашенного реферала
MIN_WITHDRAWAL = 0.05  # Минимальная сумма вывода в USDT
ZB_EXCHANGE_RATE = 0.01  # Курс 1 Zebranium = 0.01 USDT

# Настройки базы данных PostgreSQL (Чтение из переменных окружения)
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL не установлен в переменных окружения. База данных не будет работать.")
    exit(1) # Завершаем выполнение, если URL базы данных не найден

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Временные хранилища в памяти
pending_approvals = {}
maintenance_mode = False # Глобальный флаг режима техобслуживания

# Запускаем фейковый HTTP-сервер, чтобы Render не завершал процесс
def run_fake_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'Bot is running!')

    port = int(os.environ.get("PORT", 10000)) # Используем порт из переменной окружения
    try:
        server = HTTPServer(('0.0.0.0', port), Handler)
        logger.info(f"Fake web server running on 0.0.0.0:{port}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start fake web server: {e}")

# Запускаем фейковый сервер в отдельном потоке
threading.Thread(target=run_fake_server, daemon=True).start()


# Классы состояний
class TaskStates(StatesGroup):
    waiting_for_proof = State()

class BroadcastState(StatesGroup):
    waiting_for_message = State()

class BlockState(StatesGroup):
    waiting_for_id = State()

class UnblockState(StatesGroup):
    waiting_for_id = State()

class AddTaskState(StatesGroup):
    waiting_for_task_number = State()
    waiting_for_task_text = State()
    waiting_for_task_photo = State()

class DeleteTaskState(StatesGroup):
    waiting_for_task_number = State()

class TopStates(StatesGroup):
    waiting_top_type = State()
    waiting_referral_period = State() # Не используется, но оставлено для единообразия
    waiting_task_period = State()

class EditUserState(StatesGroup):
    waiting_for_id = State()
    waiting_for_field = State()
    waiting_for_value = State()

class WithdrawState(StatesGroup):
    waiting_for_amount = State()

class DepositState(StatesGroup):
    waiting_for_amount = State()


# =====================
# ФУНКЦИИ БАЗЫ ДАННЫХ PostgreSQL
# =====================

async def get_db_connection():
    """Получает асинхронное подключение к базе данных PostgreSQL."""
    return await asyncpg.connect(DATABASE_URL)

async def init_db():
    """Инициализирует таблицы базы данных, если они не существуют."""
    conn = None
    try:
        conn = await get_db_connection()
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username VARCHAR(255),
                reg_date VARCHAR(255),
                balance DECIMAL(10, 2),
                last_mine_time VARCHAR(255)
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                referrer_id BIGINT,
                referred_user_id BIGINT PRIMARY KEY,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                task_num INT PRIMARY KEY,
                task_text TEXT,
                task_photo_file_id VARCHAR(255)
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS task_proofs (
                user_id BIGINT,
                task_num INT,
                proof_photo_file_id VARCHAR(255),
                completion_date VARCHAR(255),
                PRIMARY KEY (user_id, task_num),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (task_num) REFERENCES tasks(task_num) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS blocked_users (
                user_id BIGINT PRIMARY KEY
            );
        ''')
        logger.info("База данных PostgreSQL инициализирована.")
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")
    finally:
        if conn:
            await conn.close()


# --- CRUD операции для пользователей ---
async def db_add_user(user_id: int, username: str, reg_date: str, balance: float, last_mine_time: str = None):
    conn = await get_db_connection()
    try:
        await conn.execute(
            "INSERT INTO users (user_id, username, reg_date, balance, last_mine_time) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (user_id) DO NOTHING",
            user_id, username, reg_date, balance, last_mine_time
        )
    finally:
        await conn.close()

async def db_get_user(user_id: int):
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow("SELECT user_id, username, reg_date, balance, last_mine_time FROM users WHERE user_id = $1", user_id)
    finally:
        await conn.close()
    if row:
        return {
            'user_id': row['user_id'],
            'username': row['username'],
            'reg_date': row['reg_date'],
            'balance': float(row['balance']), # Преобразуем Decimal в float
            'last_mine_time': row['last_mine_time']
        }
    return None

async def db_update_user_balance(user_id: int, new_balance: float):
    conn = await get_db_connection()
    try:
        await conn.execute("UPDATE users SET balance = $1 WHERE user_id = $2", new_balance, user_id)
    finally:
        await conn.close()

async def db_update_user_last_mine_time(user_id: int, last_mine_time: str):
    conn = await get_db_connection()
    try:
        await conn.execute("UPDATE users SET last_mine_time = $1 WHERE user_id = $2", last_mine_time, user_id)
    finally:
        await conn.close()

async def db_update_username(user_id: int, username: str):
    conn = await get_db_connection()
    try:
        await conn.execute("UPDATE users SET username = $1 WHERE user_id = $2", username, user_id)
    finally:
        await conn.close()

# --- CRUD операции для рефералов ---
async def db_add_referral(referrer_id: int, referred_user_id: int):
    conn = await get_db_connection()
    try:
        await conn.execute(
            "INSERT INTO referrals (referrer_id, referred_user_id) VALUES ($1, $2) ON CONFLICT (referred_user_id) DO NOTHING",
            referrer_id, referred_user_id
        )
    finally:
        await conn.close()

async def db_get_referrals_count(user_id: int):
    conn = await get_db_connection()
    try:
        count = await conn.fetchval("SELECT COUNT(*) FROM referrals WHERE referrer_id = $1", user_id)
    finally:
        await conn.close()
    return count if count is not None else 0

async def db_get_all_users_with_referral_count():
    conn = await get_db_connection()
    try:
        rows = await conn.fetch('''
            SELECT u.user_id, u.username, COUNT(r.referred_user_id) AS referral_count
            FROM users u
            LEFT JOIN referrals r ON u.user_id = r.referrer_id
            GROUP BY u.user_id, u.username
            ORDER BY referral_count DESC
        ''')
    finally:
        await conn.close()
    return [(r['user_id'], r['username'], r['referral_count']) for r in rows]

# --- CRUD операции для заданий ---
async def db_add_task(task_num: int, text: str, photo_file_id: str = None):
    conn = await get_db_connection()
    try:
        await conn.execute(
            "INSERT INTO tasks (task_num, task_text, task_photo_file_id) VALUES ($1, $2, $3) ON CONFLICT (task_num) DO UPDATE SET task_text=$2, task_photo_file_id=$3",
            task_num, text, photo_file_id
        )
    finally:
        await conn.close()

async def db_get_task(task_num: int):
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow("SELECT task_num, task_text, task_photo_file_id FROM tasks WHERE task_num = $1", task_num)
    finally:
        await conn.close()
    if row:
        return {'task_num': row['task_num'], 'text': row['task_text'], 'photo': row['task_photo_file_id']}
    return None

async def db_get_all_tasks():
    conn = await get_db_connection()
    try:
        rows = await conn.fetch("SELECT task_num, task_text, task_photo_file_id FROM tasks ORDER BY task_num")
    finally:
        await conn.close()
    return [{'task_num': r['task_num'], 'text': r['task_text'], 'photo': r['task_photo_file_id']} for r in rows]

async def db_delete_task(task_num: int):
    conn = await get_db_connection()
    try:
        await conn.execute("DELETE FROM tasks WHERE task_num = $1", task_num)
    finally:
        await conn.close()
    
# --- CRUD операции для выполненных заданий ---
async def db_add_task_proof(user_id: int, task_num: int, proof_photo_file_id: str, completion_date: str):
    conn = await get_db_connection()
    try:
        await conn.execute(
            "INSERT INTO task_proofs (user_id, task_num, proof_photo_file_id, completion_date) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, task_num) DO UPDATE SET proof_photo_file_id=$3, completion_date=$4",
            user_id, task_num, proof_photo_file_id, completion_date
        )
    finally:
        await conn.close()

async def db_get_user_completed_tasks(user_id: int):
    conn = await get_db_connection()
    try:
        rows = await conn.fetch("SELECT task_num, completion_date FROM task_proofs WHERE user_id = $1", user_id)
    finally:
        await conn.close()
    return {r['task_num']: r['completion_date'] for r in rows} # {task_num: completion_date}

async def db_get_all_completed_tasks_with_dates():
    conn = await get_db_connection()
    try:
        rows = await conn.fetch("SELECT user_id, task_num, completion_date FROM task_proofs")
    finally:
        await conn.close()
    
    result = defaultdict(dict)
    for r in rows:
        result[r['user_id']][r['task_num']] = r['completion_date']
    return result

async def db_get_total_completed_tasks_count():
    conn = await get_db_connection()
    try:
        count = await conn.fetchval("SELECT COUNT(*) FROM task_proofs")
    finally:
        await conn.close()
    return count if count is not None else 0

# --- CRUD операции для заблокированных пользователей ---
async def db_block_user(user_id: int):
    conn = await get_db_connection()
    try:
        await conn.execute("INSERT INTO blocked_users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id)
    finally:
        await conn.close()

async def db_unblock_user(user_id: int):
    conn = await get_db_connection()
    try:
        await conn.execute("DELETE FROM blocked_users WHERE user_id = $1", user_id)
    finally:
        await conn.close()

async def db_is_user_blocked(user_id: int):
    conn = await get_db_connection()
    try:
        is_blocked = await conn.fetchval("SELECT 1 FROM blocked_users WHERE user_id = $1", user_id) is not None
    finally:
        await conn.close()
    return is_blocked

async def db_get_all_blocked_users():
    conn = await get_db_connection()
    try:
        rows = await conn.fetch("SELECT user_id FROM blocked_users")
    finally:
        await conn.close()
    return {r['user_id'] for r in rows}

# --- Общие данные для статистики/рассылки ---
async def db_get_all_user_ids():
    conn = await get_db_connection()
    try:
        rows = await conn.fetch("SELECT user_id FROM users")
    finally:
        await conn.close()
    return [r['user_id'] for r in rows]

async def db_get_total_balance():
    conn = await get_db_connection()
    try:
        total_balance = await conn.fetchval("SELECT SUM(balance) FROM users")
    finally:
        await conn.close()
    return float(total_balance) if total_balance is not None else 0.0

async def db_get_users_for_export():
    conn = await get_db_connection()
    try:
        rows = await conn.fetch('''
            SELECT
                u.user_id,
                u.username,
                u.reg_date,
                u.balance,
                COUNT(DISTINCT r.referred_user_id) AS referral_count,
                STRING_AGG(DISTINCT tp.task_num::text, ',' ORDER BY tp.task_num) AS completed_task_nums,
                STRING_AGG(DISTINCT tp.completion_date, ',' ORDER BY tp.task_num) AS completed_task_dates
            FROM users u
            LEFT JOIN referrals r ON u.user_id = r.referrer_id
            LEFT JOIN task_proofs tp ON u.user_id = tp.user_id
            GROUP BY u.user_id, u.username, u.reg_date, u.balance
            ORDER BY u.user_id
        ''')
    finally:
        await conn.close()
    # asyncpg возвращает Row-объекты, которые можно преобразовать в кортежи или словари
    return [tuple(row.values()) for row in rows]


# =====================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =====================

# Декоратор для проверки блокировки и режима техобслуживания
def check_not_blocked(func):
    async def wrapper(message: types.Message, **kwargs):
        if await db_is_user_blocked(message.from_user.id):
            await message.answer("⛔ Вы заблокированы.")
            return

        if maintenance_mode and message.from_user.id not in ADMIN_IDS:
            await message.answer("🔧 Бот находится на техническом обслуживании. Пожалуйста, попробуйте позже.")
            return

        return await func(message, **kwargs)
    return wrapper

# Функции для клавиатур
def get_main_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="👀Профиль")],
        [KeyboardButton(text="👥Рефералы"), KeyboardButton(text="💼Задания")],
        [KeyboardButton(text="⛏️Майнинг"), KeyboardButton(text="📈Топы")],
        [KeyboardButton(text="✉️Помощь")]
    ]
    if is_admin:
        kb.append([KeyboardButton(text="👑Админка")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_admin_kb() -> ReplyKeyboardMarkup:
    global maintenance_mode # Используем глобальную переменную
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="🧾 Список пользователей")],
            [KeyboardButton(text="📨 Рассылка"), KeyboardButton(text="💼 Задания (Админ)")], # Изменил название
            [KeyboardButton(text="🚫 Заблокировать"), KeyboardButton(text="🔓 Разблокировать")],
            [KeyboardButton(text="✏️ Редактировать пользователя")],
            [KeyboardButton(text="📥 Экспорт данных")],
            [KeyboardButton(text="🔧 Техперерыв Вкл" if not maintenance_mode else "🔧 Техперерыв Выкл")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )

async def get_tasks_kb() -> ReplyKeyboardMarkup:
    all_tasks = await db_get_all_tasks()
    kb = []
    # Разделяем кнопки на несколько строк, чтобы не было слишком длинных
    current_row = []
    for i, task in enumerate(all_tasks):
        current_row.append(KeyboardButton(text=f"Задание {task['task_num']}"))
        if len(current_row) == 2 or i == len(all_tasks) - 1: # По 2 кнопки в ряд или последняя строка
            kb.append(current_row)
            current_row = []
    
    kb.append([KeyboardButton(text="🔙 Назад")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_task_kb(task_num: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=f"✅ Выполнил задание {task_num}")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )

def get_tops_type_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏆 Топы приглашений"), KeyboardButton(text="🏆 Топы заданий")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )

def get_period_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📅 Топ недели"), KeyboardButton(text="📅 Топ месяца")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )

def get_tasks_admin_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить задание"), KeyboardButton(text="❌ Удалить задание")],
            [KeyboardButton(text="🔙 Назад в админку")]
        ],
        resize_keyboard=True
    )

def get_edit_user_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Баланс")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )

# =====================
# ФУНКЦИИ ВЫВОДА И ПОПОЛНЕНИЯ (через Crypto Bot API)
# =====================

async def create_crypto_bot_check(user_id: int, amount_usdt: float) -> dict:
    if not CRYPTO_BOT_TOKEN:
        logger.warning("Crypto Bot API Token не установлен. Невозможно создать чек.")
        return {'ok': False, 'error': {'name': 'Crypto Bot API Token не установлен'}}
    
    headers = {
        'Crypto-Pay-API-Token': CRYPTO_BOT_TOKEN,
        'Content-Type': 'application/json'
    }

    payload = {
        'asset': 'USDT',
        'amount': f"{amount_usdt:.2f}", # Форматируем до 2 знаков после запятой
        'description': f'Вывод средств пользователя {user_id}',
        'payload': str(user_id), # Используем user_id как payload для идентификации
        'public': True # Чек будет публичным, чтобы пользователь мог его активировать
    }

    try:
        response = requests.post(
            f'{CRYPTO_BOT_API_URL}createCheck',
            headers=headers,
            json=payload,
            timeout=15
        )
        response_data = response.json()
        logger.info(f"Crypto Bot API Response (createCheck): {response_data}")
        return response_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при создании чека (сетевая ошибка): {e}")
        return {'ok': False, 'error': {'name': f"Ошибка сети при создании чека: {e}"}}
    except Exception as e:
        logger.error(f"Неизвестная ошибка при создании чека: {e}")
        return {'ok': False, 'error': {'name': str(e)}}

async def create_crypto_bot_invoice(user_id: int, amount_usdt: float) -> dict:
    if not CRYPTO_BOT_TOKEN:
        logger.warning("Crypto Bot API Token не установлен. Невозможно создать инвойс.")
        return {'ok': False, 'error': {'name': 'Crypto Bot API Token не установлен'}}

    headers = {
        'Crypto-Pay-API-Token': CRYPTO_BOT_TOKEN,
        'Content-Type': 'application/json'
    }

    payload = {
        'asset': 'USDT',
        'amount': f"{amount_usdt:.2f}",
        'description': f'Пополнение баланса пользователя {user_id}',
        'payload': str(user_id),
        'allow_anonymous': False
    }

    try:
        response = requests.post(
            f'{CRYPTO_BOT_API_URL}createInvoice',
            headers=headers,
            json=payload,
            timeout=15
        )
        response_data = response.json()
        logger.info(f"Crypto Bot API Response (createInvoice): {response_data}")
        return response_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при создании инвойса (сетевая ошибка): {e}")
        return {'ok': False, 'error': {'name': f"Ошибка сети при создании инвойса: {e}"}}
    except Exception as e:
        logger.error(f"Неизвестная ошибка при создании инвойса: {e}")
        return {'ok': False, 'error': {'name': str(e)}}

async def check_invoice_status(invoice_id: int) -> dict:
    if not CRYPTO_BOT_TOKEN:
        logger.warning("Crypto Bot API Token не установлен. Невозможно проверить статус инвойса.")
        return {'ok': False, 'error': {'name': 'Crypto Bot API Token не установлен'}}

    headers = {
        'Crypto-Pay-API-Token': CRYPTO_BOT_TOKEN,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(
            f'{CRYPTO_BOT_API_URL}getInvoices?invoice_ids={invoice_id}',
            headers=headers,
            timeout=15
        )
        response_data = response.json()
        logger.info(f"Crypto Bot API Response (getInvoices for {invoice_id}): {response_data}")
        return response_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при проверке инвойса (сетевая ошибка): {e}")
        return {'ok': False, 'error': {'name': f"Ошибка сети при проверке инвойса: {e}"}}
    except Exception as e:
        logger.error(f"Неизвестная ошибка при проверке инвойса: {e}")
        return {'ok': False, 'error': {'name': str(e)}}

async def process_withdrawal(user_id: int, amount_zb: int):
    user_data = await db_get_user(user_id)
    if not user_data:
        return False, "Пользователь не найден."

    if user_data['balance'] < amount_zb:
        return False, "Недостаточно средств на балансе."

    amount_usdt = amount_zb * ZB_EXCHANGE_RATE
    if amount_usdt < MIN_WITHDRAWAL:
        return False, f"Минимальная сумма вывода: {MIN_WITHDRAWAL} USDT."

    # Создаем чек
    check_response = await create_crypto_bot_check(user_id, amount_usdt)

    if not check_response.get('ok', False):
        error_msg = check_response.get('error', {}).get('name', 'Неизвестная ошибка')
        return False, f"Ошибка платежной системы: {error_msg}"

    if not check_response.get('result'):
        return False, "Платежная система не предоставила данные для вывода."

    if 'bot_check_url' not in check_response['result']:
        return False, "Платежная система не предоставила ссылку для вывода."

    # Уменьшаем баланс только после успешного создания чека
    new_balance = user_data['balance'] - amount_zb
    await db_update_user_balance(user_id, new_balance)
    
    return True, check_response['result']['bot_check_url']

async def process_deposit(user_id: int, amount_usdt: float):
    user_data = await db_get_user(user_id)
    if not user_data:
        return False, "Пользователь не найден."

    # Создаем инвойс
    invoice_response = await create_crypto_bot_invoice(user_id, amount_usdt)

    if not invoice_response.get('ok', False):
        error_msg = invoice_response.get('error', {}).get('name', 'Неизвестная ошибка')
        return False, f"Ошибка платежной системы: {error_msg}"

    if not invoice_response.get('result'):
        return False, "Платежная система не предоставила данные для оплаты."

    if 'pay_url' not in invoice_response['result']:
        return False, "Платежная система не предоставила ссылку для оплаты."

    return True, {
        'pay_url': invoice_response['result']['pay_url'],
        'invoice_id': invoice_response['result']['invoice_id']
    }

async def check_payment_status(user_id: int, invoice_id: int, amount_usdt: float):
    """
    Фоновая задача для проверки статуса оплаты инвойса.
    """
    max_attempts = 60 # Проверяем в течение 10 минут (60 * 10 секунд)
    attempt = 0

    while attempt < max_attempts:
        await asyncio.sleep(10) # Проверяем каждые 10 секунд

        invoice_data = await check_invoice_status(invoice_id)

        if not invoice_data.get('ok', False) or not invoice_data.get('result', {}).get('items'):
            logger.error(f"Ошибка или некорректный ответ при проверке счета {invoice_id}: {invoice_data.get('error', {}).get('name')}")
            attempt += 1
            continue

        invoice_status = invoice_data['result']['items'][0]['status']
        logger.info(f"Статус инвойса {invoice_id} для пользователя {user_id}: {invoice_status}")

        if invoice_status == 'paid':
            user_data = await db_get_user(user_id)
            if user_data:
                amount_zb = int(amount_usdt / ZB_EXCHANGE_RATE)
                new_balance = user_data['balance'] + amount_zb
                await db_update_user_balance(user_id, new_balance)

                try:
                    await bot.send_message(
                        user_id,
                        f"✅ Ваш баланс успешно пополнен на {amount_zb} Zebranium!"
                    )
                except TelegramForbiddenError:
                    logger.warning(f"Не удалось отправить уведомление о пополнении пользователю {user_id}: бот заблокирован.")
                except Exception as e:
                    logger.error(f"Ошибка при отправке уведомления пользователю {user_id}: {e}")
            return # Выход из цикла и задачи

        elif invoice_status in ['expired', 'cancelled']:
            try:
                await bot.send_message(
                    user_id,
                    f"❌ Счет на оплату {amount_usdt} USDT был отменен или истек."
                )
            except TelegramForbiddenError:
                logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: бот заблокирован.")
            except Exception as e:
                logger.error(f"Ошибка при отправке уведомления пользователю {user_id}: {e}")
            return # Выход из цикла и задачи

        attempt += 1

    # Если цикл завершился по количеству попыток, а статус не 'paid'
    try:
        await bot.send_message(
            user_id,
            f"❌ Время ожидания платежа истекло. Если вы произвели оплату, обратитесь в поддержку."
        )
    except TelegramForbiddenError:
        logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: бот заблокирован.")
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления пользователю {user_id}: {e}")


# =====================
# ОСНОВНЫЕ КОМАНДЫ БОТА
# =====================

@dp.message(CommandStart())
@check_not_blocked
async def cmd_start(message: types.Message, command: CommandObject = None, **kwargs):
    global maintenance_mode
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        return # Если техобслуживание, и это не админ, ничего не делаем

    user_id = message.from_user.id
    username = message.from_user.username or f"id_{user_id}"

    user_data = await db_get_user(user_id)

    if user_data:
        # Обновляем юзернейм, если он изменился
        if user_data.get('username') != username:
            await db_update_username(user_id, username)
        logger.info(f"Существующий пользователь {user_id} (@{username}) начал/перезапустил бота.")
    else:
        reg_date = datetime.now().strftime('%d.%m.%Y %H:%M')
        await db_add_user(user_id, username, reg_date, 0.0, None)
        logger.info(f"Новый пользователь {user_id} (@{username}) зарегистрирован.")

        referrer_id = None
        if command and command.args and command.args.isdigit():
            referrer_id = int(command.args)

        if referrer_id and referrer_id != user_id: # Нельзя быть своим рефералом
            # Проверяем, существует ли реферер в БД и еще не является реферером
            if await db_get_user(referrer_id):
                # Проверяем, что текущий пользователь уже не был чьим-то рефералом
                conn = await get_db_connection()
                existing_referral = await conn.fetchrow("SELECT 1 FROM referrals WHERE referred_user_id = $1", user_id)
                await conn.close()

                if not existing_referral:
                    await db_add_referral(referrer_id, user_id)
                    referrer_data = await db_get_user(referrer_id)
                    referrer_balance = referrer_data['balance']
                    await db_update_user_balance(referrer_id, referrer_balance + REFERRAL_REWARD)
                    logger.info(f"Пользователь {user_id} стал рефералом {referrer_id}. Начислено {REFERRAL_REWARD} ZB.")
                    try:
                        await bot.send_message(
                            referrer_id,
                            f"🎉 Вы получили {REFERRAL_REWARD} Zebranium за приглашенного реферала {message.from_user.full_name}!"
                        )
                    except TelegramForbiddenError:
                        logger.warning(f"Не удалось отправить сообщение рефереру {referrer_id}: бот заблокирован или пользователь недоступен.")
                    except Exception as e:
                        logger.error(f"Ошибка при отправке сообщения рефереру {referrer_id}: {e}")
                else:
                    logger.info(f"Пользователь {user_id} уже является рефералом другого пользователя. Реферал не будет засчитан.")
            else:
                logger.warning(f"Реферер {referrer_id} не найден в базе данных. Реферал не будет засчитан.")


    is_admin = user_id in ADMIN_IDS
    await message.answer(
        "🤖 Добро пожаловать в бота!\nВыберите действие в меню ниже:",
        reply_markup=get_main_kb(is_admin)
    )

@dp.message(F.text == "👀Профиль")
@check_not_blocked
async def profile_handler(message: types.Message, **kwargs):
    user_id = message.from_user.id
    user_data = await db_get_user(user_id)
    if not user_data:
        await message.answer("❌ Ваш профиль не найден. Пожалуйста, начните с команды /start.")
        return

    balance = user_data.get('balance', 0.0)
    referrals_count = await db_get_referrals_count(user_id)
    completed_tasks_count = len(await db_get_user_completed_tasks(user_id))

    builder = InlineKeyboardBuilder()
    if CRYPTO_BOT_TOKEN: # Показываем кнопки только если токен Crypto Bot API установлен
        builder.add(InlineKeyboardButton(
            text="💰 Пополнить баланс",
            callback_data="deposit_funds"
        ))
        builder.add(InlineKeyboardButton(
            text="💸 Вывести средства",
            callback_data="withdraw_funds"
        ))
        builder.adjust(2) # Размещаем две кнопки в одной строке

    await message.answer(
        f"👤 Ваш профиль:\n"
        f"🆔 ID: `{message.from_user.id}`\n"
        f"🔗 Юзернейм: @{user_data.get('username', '—')}\n"
        f"📅 Регистрация: {user_data.get('reg_date', '—')}\n"
        f"👥 Рефералов: {referrals_count}\n"
        f"✅ Выполнено заданий: {completed_tasks_count}\n"
        f"💎 Баланс: {balance:.2f} Zebranium (≈{balance * ZB_EXCHANGE_RATE:.2f} USDT)\n\n"
        f"Минимальная сумма вывода: {MIN_WITHDRAWAL} USDT",
        parse_mode="Markdown",
        reply_markup=builder.as_markup() if CRYPTO_BOT_TOKEN else None # Если нет токена, нет кнопок
    )

@dp.callback_query(F.data == "deposit_funds")
@check_not_blocked
async def deposit_funds_handler(callback: types.CallbackQuery, state: FSMContext):
    if not CRYPTO_BOT_TOKEN:
        await callback.answer("❌ Функции пополнения временно недоступны. Обратитесь к администратору.", show_alert=True)
        return

    await callback.message.answer(
        "💰 **Пополнение баланса**\n\n"
        "Введите сумму в USDT, которую хотите внести (например, `5.00` или `10`):\n"
        "*(Минимальная сумма пополнения может зависеть от Crypto Bot)*",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(DepositState.waiting_for_amount)
    await callback.answer()

@dp.message(DepositState.waiting_for_amount, F.text == "🔙 Отмена")
@check_not_blocked
async def cancel_deposit(message: types.Message, state: FSMContext):
    await message.answer(
        "❌ Пополнение баланса отменено.",
        reply_markup=get_main_kb(message.from_user.id in ADMIN_IDS)
    )
    await state.clear()

@dp.message(DepositState.waiting_for_amount)
@check_not_blocked
async def process_deposit_amount(message: types.Message, state: FSMContext):
    if not CRYPTO_BOT_TOKEN:
        await message.answer("❌ Функции пополнения временно недоступны. Обратитесь к администратору.")
        await state.clear()
        return

    try:
        amount_usdt = float(message.text.replace(',', '.')) # Для корректной обработки десятичных дробей
        if amount_usdt <= 0:
            raise ValueError("Сумма должна быть положительной.")

        user_id = message.from_user.id
        success, result = await process_deposit(user_id, amount_usdt)

        if success:
            invoice_url = result['pay_url']
            invoice_id = result['invoice_id']

            await message.answer(
                f"✅ **Счет на оплату создан!**\n"
                f"Сумма: `{amount_usdt:.2f}` USDT\n"
                f"Для оплаты перейдите по ссылке: [Оплатить]({invoice_url})\n\n"
                "После оплаты баланс будет зачислен автоматически. Пожалуйста, ожидайте.",
                parse_mode="Markdown",
                disable_web_page_preview=True,
                reply_markup=get_main_kb(user_id in ADMIN_IDS)
            )
            # Запускаем фоновую проверку статуса платежа
            asyncio.create_task(check_payment_status(user_id, invoice_id, amount_usdt))
        else:
            await message.answer(
                f"❌ Ошибка при создании счета: {result}",
                reply_markup=get_main_kb(user_id in ADMIN_IDS)
            )
    except ValueError as ve:
        await message.answer(f"❌ Неверный формат суммы. Пожалуйста, введите положительное число (например, `5.00`). Ошибка: {ve}", parse_mode="Markdown")
        return
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при обработке пополнения для пользователя {user_id}: {e}")
        await message.answer("❌ Произошла непредвиденная ошибка. Пожалуйста, попробуйте еще раз или обратитесь в поддержку.", reply_markup=get_main_kb(user_id in ADMIN_IDS))

    await state.clear()


@dp.callback_query(F.data == "withdraw_funds")
@check_not_blocked
async def withdraw_funds_handler(callback: types.CallbackQuery, state: FSMContext):
    if not CRYPTO_BOT_TOKEN:
        await callback.answer("❌ Функции вывода временно недоступны. Обратитесь к администратору.", show_alert=True)
        return

    user_id = callback.from_user.id
    user_data = await db_get_user(user_id)
    if not user_data:
        await callback.message.answer("❌ Ваш профиль не найден. Пожалуйста, начните с команды /start.")
        await callback.answer()
        return

    balance = user_data.get('balance', 0.0)
    max_withdraw_zb = int(balance) # Выводим только целые Zebranium
    max_withdraw_usdt = max_withdraw_zb * ZB_EXCHANGE_RATE

    if max_withdraw_usdt < MIN_WITHDRAWAL:
        await callback.message.answer(
            f"❌ У вас недостаточно Zebranium для вывода.\n"
            f"Текущий баланс: {balance:.2f} Zebranium (≈{max_withdraw_usdt:.2f} USDT)\n"
            f"Минимальная сумма вывода: {MIN_WITHDRAWAL} USDT."
        )
        await callback.answer()
        return

    await callback.message.answer(
        f"💸 **Вывод средств**\n\n"
        f"Доступно для вывода: `{max_withdraw_zb}` Zebranium (≈`{max_withdraw_usdt:.2f}` USDT)\n"
        f"Минимальная сумма вывода: `{MIN_WITHDRAWAL}` USDT\n\n"
        f"Введите целую сумму Zebranium для вывода (например, `100`):",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(WithdrawState.waiting_for_amount)
    await callback.answer()

@dp.message(WithdrawState.waiting_for_amount, F.text == "🔙 Отмена")
@check_not_blocked
async def cancel_withdrawal(message: types.Message, state: FSMContext):
    await message.answer(
        "❌ Вывод средств отменен.",
        reply_markup=get_main_kb(message.from_user.id in ADMIN_IDS)
    )
    await state.clear()

@dp.message(WithdrawState.waiting_for_amount)
@check_not_blocked
async def process_withdrawal_amount(message: types.Message, state: FSMContext):
    if not CRYPTO_BOT_TOKEN:
        await message.answer("❌ Функции вывода временно недоступны. Обратитесь к администратору.")
        await state.clear()
        return

    try:
        amount_zb = int(message.text)
        if amount_zb <= 0:
            raise ValueError("Сумма должна быть положительной.")

        user_id = message.from_user.id
        success, result = await process_withdrawal(user_id, amount_zb)

        if success:
            await message.answer(
                f"✅ **Запрос на вывод {amount_zb} Zebranium (≈{amount_zb * ZB_EXCHANGE_RATE:.2f} USDT) принят!**\n"
                f"Для получения средств перейдите по ссылке: [Активировать чек]({result})\n\n"
                "Чек действует ограниченное время.",
                parse_mode="Markdown",
                disable_web_page_preview=True,
                reply_markup=get_main_kb(user_id in ADMIN_IDS)
            )
        else:
            await message.answer(
                f"❌ Ошибка: {result}",
                reply_markup=get_main_kb(user_id in ADMIN_IDS)
            )
    except ValueError as ve:
        await message.answer(f"❌ Неверный формат суммы. Пожалуйста, введите целое положительное число. Ошибка: {ve}")
        return
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при обработке вывода для пользователя {user_id}: {e}")
        await message.answer("❌ Произошла непредвиденная ошибка. Пожалуйста, попробуйте еще раз или обратитесь в поддержку.", reply_markup=get_main_kb(user_id in ADMIN_IDS))

    await state.clear()


@dp.message(F.text == "👥Рефералы")
@check_not_blocked
async def referrals_handler(message: types.Message, **kwargs):
    user_id = message.from_user.id
    bot_username = (await bot.get_me()).username
    link = f"https://t.me/{bot_username}?start={user_id}"
    count = await db_get_referrals_count(user_id)

    await message.answer(
        f"👥 **Ваши рефералы:**\n"
        f"У вас **{count}** рефералов.\n\n"
        f"Пригласите друзей по вашей ссылке и получайте **{REFERRAL_REWARD} Zebranium** за каждого!\n"
        f"Ваша реферальная ссылка:\n`{link}`",
        parse_mode="Markdown"
    )

@dp.message(F.text == "💼Задания")
@check_not_blocked
async def tasks_handler(message: types.Message, **kwargs):
    await message.answer("💼 Выберите задание:", reply_markup=await get_tasks_kb())

@dp.message(F.text.regexp(r"^Задание (\d+)$"))
@check_not_blocked
async def show_specific_task(message: types.Message, state: FSMContext, **kwargs):
    try:
        task_num = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("❌ Неверный формат команды. Пожалуйста, выберите задание из меню.", reply_markup=await get_tasks_kb())
        return

    task_data = await db_get_task(task_num)
    user_id = message.from_user.id
    user_completed_tasks = await db_get_user_completed_tasks(user_id)

    if not task_data:
        await message.answer("❌ Такого задания не существует.", reply_markup=await get_tasks_kb())
        return

    text = task_data['text']
    photo_file_id = task_data['photo']

    status_message = ""
    if task_num in user_completed_tasks:
        status_message = f"✅ Вы уже выполнили это задание {user_completed_tasks[task_num]}."
    else:
        status_message = "Вы еще не выполнили это задание."

    caption_text = f"💼 **Задание {task_num}:**\n{text}\n\n{status_message}"

    if photo_file_id:
        try:
            await bot.send_photo(
                chat_id=message.chat.id,
                photo=photo_file_id,
                caption=caption_text,
                parse_mode="Markdown",
                reply_markup=get_task_kb(task_num)
            )
        except Exception as e:
            logger.error(f"Не удалось отправить фото задания {task_num} пользователю {user_id}: {e}")
            await message.answer(
                f"{caption_text}\n\n"
                f"*(Ошибка при загрузке фото. Возможно, фото было удалено или недоступно.)*",
                parse_mode="Markdown",
                reply_markup=get_task_kb(task_num)
            )
    else:
        await message.answer(
            caption_text,
            parse_mode="Markdown",
            reply_markup=get_task_kb(task_num)
        )

    await state.update_data(current_task_num=task_num)

@dp.message(F.text.regexp(r"^✅ Выполнил задание (\d+)$"))
@check_not_blocked
async def complete_task_button(message: types.Message, state: FSMContext, **kwargs):
    try:
        task_num = int(message.text.split()[2])
    except (IndexError, ValueError):
        await message.answer("❌ Неверный формат команды. Пожалуйста, выберите 'Выполнил задание' из меню.", reply_markup=await get_tasks_kb())
        return

    user_id = message.from_user.id
    user_completed_tasks = await db_get_user_completed_tasks(user_id)
    if task_num in user_completed_tasks:
        await message.answer(
            f"⛔ Вы уже выполнили задание {task_num}. Отправьте доказательство для другого задания, или выберите 'Назад'.",
            reply_markup=get_task_kb(task_num)
        )
        return

    await state.set_state(TaskStates.waiting_for_proof)
    await state.update_data(current_task_num=task_num)
    await message.answer("Отправьте фото-доказательство выполнения задания.")

@dp.message(TaskStates.waiting_for_proof, F.photo)
@check_not_blocked
async def process_task_proof(message: types.Message, state: FSMContext, **kwargs):
    user_id = message.from_user.id
    data = await state.get_data()
    task_num = data.get('current_task_num')

    if not task_num:
        await message.answer("❌ Произошла ошибка. Пожалуйста, выберите задание еще раз.")
        await state.clear()
        return

    user_completed_tasks = await db_get_user_completed_tasks(user_id)
    if task_num in user_completed_tasks:
        await message.answer(
            f"⛔ Вы уже выполнили задание {task_num}. Отправьте доказательство для другого задания, или выберите 'Назад'.",
            reply_markup=get_task_kb(task_num)
        )
        return

    proof_photo_file_id = message.photo[-1].file_id # Берем фото с наибольшим разрешением
    completion_date = datetime.now().strftime('%d.%m.%Y %H:%M')

    await db_add_task_proof(user_id, task_num, proof_photo_file_id, completion_date)
    logger.info(f"Пользователь {user_id} отправил доказательство для задания {task_num}.")

    # Начисляем награду
    user_data = await db_get_user(user_id)
    reward = random.randint(*TASK_REWARD_RANGE)
    new_balance = user_data['balance'] + reward
    await db_update_user_balance(user_id, new_balance)
    logger.info(f"Начислено {reward} ZB пользователю {user_id} за задание {task_num}. Новый баланс: {new_balance}.")

    await message.answer(
        f"✅ Доказательство для задания {task_num} принято! На ваш баланс зачислено **{reward} Zebranium**.",
        parse_mode="Markdown",
        reply_markup=await get_tasks_kb()
    )
    await state.clear()

@dp.message(TaskStates.waiting_for_proof)
@check_not_blocked
async def process_task_proof_invalid(message: types.Message, state: FSMContext, **kwargs):
    await message.answer("❌ Пожалуйста, отправьте фото-доказательство или нажмите 'Назад'.")

@dp.message(F.text == "⛏️Майнинг")
@check_not_blocked
async def mining_handler(message: types.Message, **kwargs):
    user_id = message.from_user.id
    user_data = await db_get_user(user_id)
    if not user_data:
        await message.answer("❌ Ваш профиль не найден. Пожалуйста, начните с команды /start.")
        return

    last_mine_time_str = user_data.get('last_mine_time')
    current_time = datetime.now()

    if last_mine_time_str:
        last_mine_time = datetime.strptime(last_mine_time_str, '%Y-%m-%d %H:%M:%S')
        time_since_last_mine = current_time - last_mine_time
        remaining_cooldown = MINING_COOLDOWN - time_since_last_mine.total_seconds()

        if remaining_cooldown > 0:
            hours, remainder = divmod(remaining_cooldown, 3600)
            minutes, seconds = divmod(remainder, 60)
            await message.answer(
                f"⛏️ Майнинг недоступен. Следующая возможность майнить через "
                f"{int(hours)} ч {int(minutes)} мин {int(seconds)} сек."
            )
            return

    reward = random.randint(*MINING_REWARD_RANGE)
    new_balance = user_data['balance'] + reward
    await db_update_user_balance(user_id, new_balance)
    await db_update_user_last_mine_time(user_id, current_time.strftime('%Y-%m-%d %H:%M:%S'))
    logger.info(f"Пользователь {user_id} успешно помайнил и получил {reward} ZB. Новый баланс: {new_balance}.")

    await message.answer(f"⛏️ Вы успешно помайнили и получили **{reward} Zebranium**!", parse_mode="Markdown")

@dp.message(F.text == "📈Топы")
@check_not_blocked
async def tops_handler(message: types.Message, state: FSMContext, **kwargs):
    await message.answer("📈 Выберите тип топов:", reply_markup=get_tops_type_kb())
    await state.set_state(TopStates.waiting_top_type)

@dp.message(TopStates.waiting_top_type, F.text == "🏆 Топы приглашений")
@check_not_blocked
async def top_referrals_handler(message: types.Message, state: FSMContext, **kwargs):
    top_users_data = await db_get_all_users_with_referral_count()
    
    # Сортируем по количеству рефералов в убывающем порядке
    top_users_data.sort(key=lambda x: x[2], reverse=True)

    result = "🏆 **Топ приглашений (всего):**\n\n"
    if not top_users_data:
        result = "🏆 Топ приглашений пуст."
    else:
        for i, (user_id, username, count) in enumerate(top_users_data[:10], 1): # Топ-10
            username_str = f"@{username}" if username else f"ID: `{user_id}`"
            result += f"{i}. {username_str} - **{count}** рефералов\n"
    
    await message.answer(result, parse_mode="Markdown", reply_markup=get_tops_type_kb())
    await state.clear()


@dp.message(TopStates.waiting_top_type, F.text == "🏆 Топы заданий")
@check_not_blocked
async def top_tasks_handler(message: types.Message, state: FSMContext, **kwargs):
    await message.answer("📈 Выберите период для топа заданий:", reply_markup=get_period_kb())
    await state.set_state(TopStates.waiting_task_period)

@dp.message(TopStates.waiting_task_period, F.text == "📅 Топ недели")
@check_not_blocked
async def top_tasks_week(message: types.Message, state: FSMContext, **kwargs):
    result = await get_top_completed_tasks('week')
    await message.answer(result, parse_mode="Markdown", reply_markup=get_tops_type_kb())
    await state.clear()

@dp.message(TopStates.waiting_task_period, F.text == "📅 Топ месяца")
@check_not_blocked
async def top_tasks_month(message: types.Message, state: FSMContext, **kwargs):
    result = await get_top_completed_tasks('month')
    await message.answer(result, parse_mode="Markdown", reply_markup=get_tops_type_kb())
    await state.clear()

async def get_top_completed_tasks(period: str):
    all_completed_tasks = await db_get_all_completed_tasks_with_dates()
    
    now = datetime.now()
    if period == 'week':
        start_date = now - timedelta(weeks=1)
    elif period == 'month':
        start_date = now - timedelta(days=30) # Приближенно месяц
    else:
        return "Неверный период."

    top_users = [] # (user_id, count, username)

    for user_id, tasks_by_user in all_completed_tasks.items():
        count = 0
        for task_num, completion_date_str in tasks_by_user.items():
            try:
                task_date = datetime.strptime(completion_date_str, '%d.%m.%Y %H:%M')
                if task_date >= start_date:
                    count += 1
            except ValueError:
                logger.warning(f"Не удалось распарсить дату {completion_date_str} для пользователя {user_id}, задания {task_num}")
                continue # Пропускаем некорректные даты

        if count > 0:
            user_data = await db_get_user(user_id)
            username = user_data.get('username', '—') if user_data else '—'
            top_users.append((user_id, count, username))

    top_users.sort(key=lambda x: x[1], reverse=True)

    if not top_users:
        return f"🏆 **Топ заданий пуст за выбранный период ({'неделю' if period == 'week' else 'месяц'}).**"

    result = f"🏆 **Топ заданий за {'неделю' if period == 'week' else 'месяц'}:**\n\n"
    for i, (user_id, count, username) in enumerate(top_users[:10], 1):
        username_str = f"@{username}" if username else f"ID: `{user_id}`"
        result += f"{i}. {username_str} - **{count}** заданий\n"

    return result


@dp.message(F.text == "✉️Помощь")
@check_not_blocked
async def help_handler(message: types.Message, **kwargs):
    await message.answer(
        "👋 **Как использовать бота:**\n\n"
        "👀 **Профиль:** Просмотр вашего баланса, количества рефералов и выполненных заданий. Здесь же можно пополнить или вывести Zebranium.\n"
        "👥 **Рефералы:** Получите вашу уникальную реферальную ссылку и приглашайте друзей, чтобы получать бонусы.\n"
        "💼 **Задания:** Выполняйте простые задания (например, подписаться на канал, посмотреть видео) и получайте Zebranium.\n"
        "⛏️ **Майнинг:** Получайте бесплатные Zebranium каждые 1 час.\n"
        "📈 **Топы:** Просматривайте рейтинги самых активных пользователей по количеству приглашений и выполненных заданий.\n\n"
        "Если у вас возникли вопросы, свяжитесь с администратором.",
        parse_mode="Markdown"
    )

@dp.message(F.text == "🔙 Назад")
@check_not_blocked
async def back_to_main_menu(message: types.Message, state: FSMContext, **kwargs):
    current_state = await state.get_state()
    if current_state == TaskStates.waiting_for_proof:
        # Если были в состоянии ожидания доказательства, возвращаемся к выбору заданий
        await message.answer("Выбор задания:", reply_markup=await get_tasks_kb())
    elif current_state and current_state.startswith("TopStates"): # Для всех состояний топов
        await message.answer("Выбор типа топов:", reply_markup=get_tops_type_kb())
    else:
        # Для всех остальных случаев возвращаемся в главное меню
        is_admin = message.from_user.id in ADMIN_IDS
        await message.answer("Возвращаемся в главное меню.", reply_markup=get_main_kb(is_admin))
    await state.clear()


# =====================
# АДМИН-КОМАНДЫ
# =====================

@dp.message(F.text == "👑Админка")
async def admin_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ У вас нет доступа к админ-панели.")
        return
    global maintenance_mode # Используем глобальную переменную
    await message.answer("Добро пожаловать в админ-панель!", reply_markup=get_admin_kb())

@dp.message(F.text == "🔙 Назад в админку")
async def back_to_admin_menu(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    global maintenance_mode # Используем глобальную переменную
    await message.answer("Возвращаемся в админ-панель.", reply_markup=get_admin_kb())

@dp.message(F.text == "📊 Статистика")
async def show_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    total_users = len(await db_get_all_user_ids())
    total_balance = await db_get_total_balance()
    total_tasks_completed = await db_get_total_completed_tasks_count()

    await message.answer(
        f"📊 **Статистика бота:**\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"💰 Общий баланс Zebranium: {total_balance:.2f}\n"
        f"✅ Всего выполнено заданий: {total_tasks_completed}",
        parse_mode="Markdown",
        reply_markup=get_admin_kb()
    )

@dp.message(F.text == "🧾 Список пользователей")
async def list_users(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    all_user_ids = await db_get_all_user_ids()
    if not all_user_ids:
        await message.answer("Список пользователей пуст.", reply_markup=get_admin_kb())
        return

    users_info = []
    for user_id in all_user_ids:
        user_data = await db_get_user(user_id)
        if user_data:
            username = user_data.get('username', '—')
            balance = user_data.get('balance', 0.0)
            referrals = await db_get_referrals_count(user_id)
            users_info.append(f"ID: `{user_id}`, @{username}, Баланс: {balance:.2f}, Рефералов: {referrals}")
    
    # Ограничиваем количество пользователей, если их слишком много
    if len(users_info) > 50:
        await message.answer(
            "**Список пользователей (первые 50):**\n\n" + "\n".join(users_info[:50]),
            parse_mode="Markdown",
            reply_markup=get_admin_kb()
        )
        return
    await message.answer(
        "**Список пользователей:**\n\n" + "\n".join(users_info) or "Список пуст.",
        parse_mode="Markdown",
        reply_markup=get_admin_kb()
    )

@dp.message(F.text == "📨 Рассылка")
async def start_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "Введите сообщение для рассылки всем пользователям. "
        "Поддерживается HTML-разметка (например, `<b>жирный</b>`, `<i>курсив</i>`, `<a href=\"URL\">ссылка</a>`):",
        parse_mode="Markdown"
    )
    await state.set_state(BroadcastState.waiting_for_message)

@dp.message(BroadcastState.waiting_for_message)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    broadcast_text = message.html_text # Сохраняем форматирование
    all_user_ids = await db_get_all_user_ids()
    
    sent_count = 0
    blocked_count = 0
    
    for user_id in all_user_ids:
        try:
            await bot.send_message(user_id, broadcast_text, parse_mode="HTML")
            sent_count += 1
            await asyncio.sleep(0.05) # Небольшая задержка, чтобы избежать RateLimit
        except TelegramForbiddenError:
            logger.warning(f"Пользователь {user_id} заблокировал бота. Добавляем в список заблокированных.")
            await db_block_user(user_id) # Блокируем пользователя в БД
            blocked_count += 1
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")

    await message.answer(
        f"✅ Рассылка завершена. Отправлено {sent_count} сообщений. "
        f"Заблокировано ботом {blocked_count} пользователей.",
        reply_markup=get_admin_kb()
    )
    await state.clear()

@dp.message(F.text == "🚫 Заблокировать")
async def start_block_user(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите ID пользователя, которого нужно заблокировать:")
    await state.set_state(BlockState.waiting_for_id)

@dp.message(BlockState.waiting_for_id)
async def process_block_user_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        user_id_to_block = int(message.text)
        if await db_is_user_blocked(user_id_to_block):
            await message.answer(f"❌ Пользователь {user_id_to_block} уже заблокирован.", reply_markup=get_admin_kb())
        else:
            await db_block_user(user_id_to_block)
            await message.answer(f"✅ Пользователь {user_id_to_block} заблокирован.", reply_markup=get_admin_kb())
            logger.info(f"Админ {message.from_user.id} заблокировал пользователя {user_id_to_block}.")
    except ValueError:
        await message.answer("❌ Неверный ID пользователя. Пожалуйста, введите число.")
    await state.clear()

@dp.message(F.text == "🔓 Разблокировать")
async def start_unblock_user(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите ID пользователя, которого нужно разблокировать:")
    await state.set_state(UnblockState.waiting_for_id)

@dp.message(UnblockState.waiting_for_id)
async def process_unblock_user_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        user_id_to_unblock = int(message.text)
        if not await db_is_user_blocked(user_id_to_unblock):
            await message.answer(f"❌ Пользователь {user_id_to_unblock} не был заблокирован.", reply_markup=get_admin_kb())
        else:
            await db_unblock_user(user_id_to_unblock)
            await message.answer(f"✅ Пользователь {user_id_to_unblock} разблокирован.", reply_markup=get_admin_kb())
            logger.info(f"Админ {message.from_user.id} разблокировал пользователя {user_id_to_unblock}.")
    except ValueError:
        await message.answer("❌ Неверный ID пользователя. Пожалуйста, введите число.")
    await state.clear()


@dp.message(F.text == "💼 Задания (Админ)")
async def admin_tasks_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Выберите действие с заданиями:", reply_markup=get_tasks_admin_kb())

@dp.message(F.text == "➕ Добавить задание")
async def start_add_task(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите номер нового задания (целое положительное число):")
    await state.set_state(AddTaskState.waiting_for_task_number)

@dp.message(AddTaskState.waiting_for_task_number)
async def process_add_task_number(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        task_num = int(message.text)
        if task_num <= 0:
            raise ValueError
        await state.update_data(new_task_num=task_num)
        await message.answer("Введите текст нового задания:")
        await state.set_state(AddTaskState.waiting_for_task_text)
    except ValueError:
        await message.answer("❌ Неверный номер задания. Пожалуйста, введите положительное целое число.")

@dp.message(AddTaskState.waiting_for_task_text)
async def process_add_task_text(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    task_text = message.text
    if not task_text.strip():
        await message.answer("❌ Текст задания не может быть пустым. Пожалуйста, введите текст задания:")
        return
    await state.update_data(new_task_text=task_text)
    await message.answer("Теперь отправьте фото для задания (или 'Пропустить', если фото не нужно):",
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Пропустить")]], resize_keyboard=True))
    await state.set_state(AddTaskState.waiting_for_task_photo)

@dp.message(AddTaskState.waiting_for_task_photo, F.text == "Пропустить")
async def process_add_task_photo_skip(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    task_num = data['new_task_num']
    task_text = data['new_task_text']
    await db_add_task(task_num, task_text, None)
    logger.info(f"Админ {message.from_user.id} добавил задание {task_num} без фото.")
    await message.answer(f"✅ Задание {task_num} добавлено без фото.", reply_markup=get_tasks_admin_kb())
    await state.clear()

@dp.message(AddTaskState.waiting_for_task_photo, F.photo)
async def process_add_task_photo(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    task_num = data['new_task_num']
    task_text = data['new_task_text']
    photo_file_id = message.photo[-1].file_id # Берем фото с наибольшим разрешением
    await db_add_task(task_num, task_text, photo_file_id)
    logger.info(f"Админ {message.from_user.id} добавил задание {task_num} с фото.")
    await message.answer(f"✅ Задание {task_num} добавлено с фото.", reply_markup=get_tasks_admin_kb())
    await state.clear()

@dp.message(AddTaskState.waiting_for_task_photo)
async def process_add_task_photo_invalid(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("❌ Пожалуйста, отправьте фото или нажмите 'Пропустить'.")


@dp.message(F.text == "❌ Удалить задание")
async def start_delete_task(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    all_tasks = await db_get_all_tasks()
    if not all_tasks:
        await message.answer("Список заданий пуст. Нечего удалять.", reply_markup=get_tasks_admin_kb())
        return

    task_list_str = "\n".join([f"- Задание {t['task_num']}: {t['text'][:50]}..." for t in all_tasks]) # Ограничим длину текста
    await message.answer(
        f"Введите номер задания, которое нужно удалить:\n\n**Список заданий:**\n{task_list_str}",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад в админку")]],
            resize_keyboard=True
        )
    )
    await state.set_state(DeleteTaskState.waiting_for_task_number)

@dp.message(DeleteTaskState.waiting_for_task_number)
async def process_delete_task_number(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        task_num_to_delete = int(message.text)
        task_exists = await db_get_task(task_num_to_delete)
        if not task_exists:
            await message.answer(f"❌ Задания с номером {task_num_to_delete} не существует.", reply_markup=get_tasks_admin_kb())
            await state.clear()
            return

        await db_delete_task(task_num_to_delete)
        logger.info(f"Админ {message.from_user.id} удалил задание {task_num_to_delete}.")
        await message.answer(f"✅ Задание {task_num_to_delete} удалено.", reply_markup=get_tasks_admin_kb())
    except ValueError:
        await message.answer("❌ Неверный номер задания. Пожалуйста, введите число.")
    await state.clear()


@dp.message(F.text == "✏️ Редактировать пользователя")
async def start_edit_user(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("Введите ID пользователя, которого хотите отредактировать:")
    await state.set_state(EditUserState.waiting_for_id)

@dp.message(EditUserState.waiting_for_id)
async def process_edit_user_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        user_id_to_edit = int(message.text)
        user_data = await db_get_user(user_id_to_edit)
        if not user_data:
            await message.answer("❌ Пользователь с таким ID не найден.", reply_markup=get_admin_kb())
            await state.clear()
            return
        
        await state.update_data(edit_user_id=user_id_to_edit)
        await message.answer(
            f"Выбран пользователь ID: `{user_id_to_edit}` (@{user_data.get('username', '—')}).\n"
            "Что хотите отредактировать?",
            parse_mode="Markdown",
            reply_markup=get_edit_user_kb()
        )
        await state.set_state(EditUserState.waiting_for_field)
    except ValueError:
        await message.answer("❌ Неверный ID пользователя. Пожалуйста, введите число.", reply_markup=get_admin_kb())
        await state.clear()

@dp.message(EditUserState.waiting_for_field)
async def process_edit_user_field(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    field = message.text
    data = await state.get_data()
    user_id_to_edit = data['edit_user_id']
    user_data = await db_get_user(user_id_to_edit)

    if field == "💰 Баланс":
        await state.update_data(edit_field='balance')
        await message.answer(
            f"Введите новое значение баланса для пользователя `{user_id_to_edit}` (текущий: `{user_data.get('balance', 0.0):.2f}`):",
            parse_mode="Markdown"
        )
        await state.set_state(EditUserState.waiting_for_value)
    elif field == "🔙 Назад":
        await state.clear()
        await message.answer("Возвращаемся в админ-панель.", reply_markup=get_admin_kb())
    else:
        await message.answer("❌ Неизвестное поле для редактирования. Пожалуйста, выберите из предложенных вариантов.", reply_markup=get_edit_user_kb())

@dp.message(EditUserState.waiting_for_value)
async def process_edit_user_value(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    user_id_to_edit = data['edit_user_id']
    edit_field = data['edit_field']

    try:
        if edit_field == 'balance':
            new_balance = float(message.text.replace(',', '.'))
            if new_balance < 0:
                await message.answer("❌ Баланс не может быть отрицательным. Введите положительное число.")
                return
            await db_update_user_balance(user_id_to_edit, new_balance)
            logger.info(f"Админ {message.from_user.id} изменил баланс пользователя {user_id_to_edit} на {new_balance}.")
            await message.answer(f"✅ Баланс пользователя `{user_id_to_edit}` установлен на `{new_balance:.2f}` Zebranium.", parse_mode="Markdown", reply_markup=get_admin_kb())
        else:
            await message.answer("❌ Неизвестное поле для редактирования.", reply_markup=get_admin_kb())
    except ValueError:
        await message.answer("❌ Неверное значение. Пожалуйста, введите число (например, `100.50`).", parse_mode="Markdown")
        return
    except Exception as e:
        logger.error(f"Ошибка при редактировании пользователя {user_id_to_edit} поля {edit_field}: {e}")
        await message.answer(f"❌ Произошла ошибка при редактировании: {e}", reply_markup=get_admin_kb())
    await state.clear()

@dp.message(F.text == "📥 Экспорт данных")
async def export_data(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer("Начинаю экспорт данных. Это может занять некоторое время...")

    try:
        users_raw_data = await db_get_users_for_export()
        
        # Подготовка данных для DataFrame
        columns = [
            "ID Пользователя",
            "Юзернейм",
            "Дата Регистрации",
            "Баланс (Zebranium)",
            "Количество Рефералов",
            "Выполненные Задания (номера)",
            "Даты Выполнения Заданий"
        ]
        
        df = pd.DataFrame(users_raw_data, columns=columns)

        # Сохранение в Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Пользователи')
        output.seek(0)

        # Отправка файла
        await message.answer_document(
            document=BufferedInputFile(output.getvalue(), filename="users_data.xlsx"),
            caption="✅ Ваши данные экспортированы в Excel файл."
        )
        logger.info(f"Админ {message.from_user.id} экспортировал данные пользователей.")

    except Exception as e:
        logger.error(f"Ошибка при экспорте данных: {e}")
        await message.answer(f"❌ Произошла ошибка при экспорте данных: {e}", reply_markup=get_admin_kb())

@dp.message(F.text.in_(["🔧 Техперерыв Вкл", "🔧 Техперерыв Выкл"]))
async def toggle_maintenance_mode(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    global maintenance_mode
    if message.text == "🔧 Техперерыв Вкл":
        maintenance_mode = True
        await message.answer("✅ Режим технического обслуживания **включен**. Бот будет недоступен для обычных пользователей.", reply_markup=get_admin_kb())
        logger.warning(f"Админ {message.from_user.id} включил режим техобслуживания.")
    else:
        maintenance_mode = False
        await message.answer("✅ Режим технического обслуживания **выключен**. Бот снова доступен для всех.", reply_markup=get_admin_kb())
        logger.warning(f"Админ {message.from_user.id} выключил режим техобслуживания.")


# =====================
# ЗАПУСК БОТА
# =====================

async def main():
    logger.info("Инициализация базы данных...")
    await init_db() # Инициализируем БД при старте
    logger.info("Запуск бота в режиме polling...")
    try:
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}")

if __name__ == "__main__":
    asyncio.run(main())
