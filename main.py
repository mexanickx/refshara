import asyncio
import inspect
import random
import pandas as pd
import io
import logging
logging.basicConfig(
     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
     level=logging.INFO
 )
import requests
from datetime import datetime, timedelta
from collections import defaultdict
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
import os

# Import for the fake server
from aiohttp import web

# ТОКЕНЫ И НАСТРОЙКИ (обязательно настройте эти переменные окружения на Render)
# TELEGRAM_BOT_API_TOKEN - для вашего основного бота из @BotFather
API_TOKEN = os.environ.get('TELEGRAM_BOT_API_TOKEN', 'ВАШ_ТОКЕН_БОТА_ИЗ_BOTFATHER_ЗДЕСЬ_КАК_ЗАГЛУШКА') # Замените на реальный, если не используете env vars
# CRYPTO_BOT_API_TOKEN - для интеграции с Crypto Bot из @CryptoBot
CRYPTO_BOT_TOKEN = os.environ.get('CRYPTO_BOT_API_TOKEN', 'ВАШ_ТОКЕН_CRYPTOBOT_ЗДЕСЬ_КАК_ЗАГЛУШКА') # Замените на реальный, если не используете env vars

# !!! ВРЕМЕННОЕ ЛОГИРОВАНИЕ ДЛЯ ОТЛАДКИ !!!
# Эта строка выведет первые 5 символов токена Crypto Bot в логи Render.
# После того, как вы убедитесь, что токен читается верно, вы можете удалить эту строку.
logging.info(f"Using Crypto Bot Token (first 5 chars): {CRYPTO_BOT_TOKEN[:5]}... (length: {len(CRYPTO_BOT_TOKEN)})")

ADMIN_IDS = [1041720539, 6216901034] # Ваши ID администраторов
CRYPTO_BOT_API_URL = 'https://pay.crypt.bot/api/'

# Константы майнинга
MINING_COOLDOWN = 3600  # 1 час в секундах
MINING_REWARD_RANGE = (3, 3)  # Диапазон награды
TASK_REWARD_RANGE = (5, 10)  # Награда за задание
REFERRAL_REWARD = 3  # Награда за приглашенного реферала
MIN_WITHDRAWAL = 0.05  # Минимальная сумма вывода в USDT
ZB_EXCHANGE_RATE = 0.01  # Курс 1 Zebranium = 0.01 USDT

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Хранилища данных
blocked_users = set()
users = {}
tasks = {}
task_proofs = defaultdict(dict)
task_completion_dates = defaultdict(dict)
pending_approvals = {}
maintenance_mode = False

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
     waiting_referral_period = State()
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
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =====================

def check_not_blocked(func):
     async def wrapper(message: types.Message, **kwargs):
         kwargs.pop('dispatcher', None)

         if message.from_user.id in blocked_users:
             await message.answer("⛔ Вы заблокированы.")
             return

         if maintenance_mode and message.from_user.id not in ADMIN_IDS:
             await message.answer("🔧 Бот находится на техническом обслуживании. Пожалуйста, попробуйте позже.")
             return

         return await func(message, **kwargs)
     return wrapper

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
     return ReplyKeyboardMarkup(
         keyboard=[
             [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="🧾 Список пользователей")],
             [KeyboardButton(text="📨 Рассылка"), KeyboardButton(text="💼 Задания")], # Corrected from Keyboard to KeyboardButton
             [KeyboardButton(text="🚫 Заблокировать"), KeyboardButton(text="🔓 Разблокировать")],
             [KeyboardButton(text="✏️ Редактировать пользователя")],
             [KeyboardButton(text="📥 Экспорт данных")],
             [KeyboardButton(text="🔧 Техперерыв Вкл" if not maintenance_mode else "🔧 Техперерыв Выкл")],
             [KeyboardButton(text="🔙 Назад")]
         ],
         resize_keyboard=True
     )

def get_tasks_kb() -> ReplyKeyboardMarkup:
     kb = []
     for task_num in sorted(tasks.keys()):
         kb.append([KeyboardButton(text=f"Задание {task_num}")])
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
             [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="👥 Рефералы")],
             [KeyboardButton(text="✅ Выполнено заданий"), KeyboardButton(text="🔙 Назад")]
         ],
         resize_keyboard=True
     )

# =====================
# ФУНКЦИИ ВЫВОДА И ПОПОЛНЕНИЯ
# =====================

async def create_crypto_bot_check(user_id: int, amount_usdt: float) -> dict:
     headers = {
         'Crypto-Pay-API-Token': CRYPTO_BOT_TOKEN,
         'Content-Type': 'application/json'
     }

     payload = {
         'asset': 'USDT',
         'amount': str(amount_usdt),
         'description': f'Вывод средств пользователя {user_id}',
         'payload': str(user_id),
         'public': True
     }

     try:
         response = requests.post(
             f'{CRYPTO_BOT_API_URL}createCheck',
             headers=headers,
             json=payload,
             timeout=15
         )
         response_data = response.json()
         print("Crypto Bot API Response:", response_data)
         return response_data
     except Exception as e:
         print(f"Error creating check: {e}")
         return {'ok': False, 'error': {'name': str(e)}}

async def create_crypto_bot_invoice(user_id: int, amount_usdt: float) -> dict:
     headers = {
         'Crypto-Pay-API-Token': CRYPTO_BOT_TOKEN,
         'Content-Type': 'application/json'
     }

     payload = {
         'asset': 'USDT',
         'amount': str(amount_usdt),
         'description': f'Пополнение баланса пользователя {user_id}',
         'payload': str(user_id),
         'allow_anonymous': False,
         'compact': True # Added for compact mode
     }

     try:
         response = requests.post(
             f'{CRYPTO_BOT_API_URL}createInvoice',
             headers=headers,
             json=payload,
             timeout=15
         )
         response_data = response.json()
         print("Crypto Bot API Response:", response_data)
         return response_data
     except Exception as e:
         print(f"Error creating invoice: {e}")
         return {'ok': False, 'error': {'name': str(e)}}

async def check_invoice_status(invoice_id: int) -> dict:
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
         return response_data
     except Exception as e:
         print(f"Error checking invoice status: {e}")
         return {'ok': False, 'error': {'name': str(e)}}

async def process_withdrawal(user_id: int, amount_zb: int):
     if user_id not in users:
         return False, "Пользователь не найден"

     user_data = users[user_id]
     if user_data['balance'] < amount_zb:
         return False, "Недостаточно средств на балансе"

     amount_usdt = amount_zb * ZB_EXCHANGE_RATE
     if amount_usdt < MIN_WITHDRAWAL:
         return False, f"Минимальная сумма вывода: {MIN_WITHDRAWAL} USDT"

     check = await create_crypto_bot_check(user_id, amount_usdt)

     if not check:
         return False, "Ошибка при подключении к платежной системе"

     if not check.get('ok', False):
         error_msg = check.get('error', {}).get('name', 'Неизвестная ошибка')
         return False, f"Ошибка платежной системы: {error_msg}"

     if not check.get('result'):
         return False, "Платежная система не предоставила данные для вывода"

     if 'bot_check_url' not in check['result']:
         return False, "Платежная система не предоставила ссылку для вывода"

     users[user_id]['balance'] -= amount_zb
     return True, check['result']['bot_check_url']

async def process_deposit(user_id: int, amount_usdt: float):
     if user_id not in users:
         return False, "Пользователь не найден"

     invoice = await create_crypto_bot_invoice(user_id, amount_usdt)

     if not invoice:
         return False, "Ошибка при подключении к платежной системе"

     if not invoice.get('ok', False):
         error_msg = invoice.get('error', {}).get('name', 'Неизвестная ошибка')
         return False, f"Ошибка платежной системы: {error_msg}"

     if not invoice.get('result'):
         return False, "Платежная система не предоставила данные для оплаты"

     if 'pay_url' not in invoice['result']:
         return False, "Платежная система не предоставила ссылку для оплаты"

     return True, {
         'pay_url': invoice['result']['pay_url'],
         'invoice_id': invoice['result']['invoice_id']
     }

# =====================
# ОСНОВНЫЕ КОМАНДЫ БОТА
# =====================

@dp.message(CommandStart())
@check_not_blocked
async def cmd_start(message: types.Message, command: CommandObject = None, **kwargs):
     if maintenance_mode and message.from_user.id not in ADMIN_IDS:
         return

     user_id = message.from_user.id
     username = message.from_user.username or "—"

     referrer_id = None
     if command and command.args and command.args.isdigit():
         referrer_id = int(command.args)

     if user_id not in users:
         users[user_id] = {
             'reg_date': datetime.now().strftime('%d.%m.%Y %H:%M'),
             'referrals': [],
             'username': username,
             'balance': 0,
             'last_mine_time': None
         }

         if referrer_id and referrer_id != user_id and referrer_id in users:
             users[referrer_id]['referrals'].append(user_id)
             users[referrer_id]['balance'] += REFERRAL_REWARD
             try:
                 await bot.send_message(
                     referrer_id,
                     f"🎉 Вы получили {REFERRAL_REWARD} Zebranium за приглашенного реферала!"
                 )
             except Exception:
                 pass

     is_admin = user_id in ADMIN_IDS
     await message.answer(
         "🤖 Добро пожаловать в бота!\nВыберите действие в меню ниже:",
         reply_markup=get_main_kb(is_admin)
     )

@dp.message(F.text == "👀Профиль")
@check_not_blocked
async def profile_handler(message: types.Message, **kwargs):
     user = users.get(message.from_user.id, {})
     balance = user.get('balance', 0)

     builder = InlineKeyboardBuilder()
     builder.add(InlineKeyboardButton(
         text="💰 Пополнить баланс",
         callback_data="deposit_funds"
     ))

     if balance >= (MIN_WITHDRAWAL / ZB_EXCHANGE_RATE):
         builder.add(InlineKeyboardButton(
             text="💸 Вывести средства",
             callback_data="withdraw_funds"
         ))

     await message.answer(
         f"👤 Ваш профиль:\n"
         f"🆔 ID: {message.from_user.id}\n"
         f"🔗 Юзернейм: @{user.get('username', '—')}\n"
         f"📅 Регистрация: {user.get('reg_date', '—')}\n"
         f"👥 Рефералов: {len(user.get('referrals', []))}\n"
         f"✅ Выполнено заданий: {len(task_proofs.get(message.from_user.id, {}))}\n"
         f"💎 Баланс: {balance} Zebranium (≈{balance * ZB_EXCHANGE_RATE:.2f} USDT)\n\n"
         f"Минимальная сумма вывода: {MIN_WITHDRAWAL} USDT",
         reply_markup=builder.as_markup()
     )

@dp.callback_query(F.data == "deposit_funds")
async def deposit_funds_handler(callback: types.CallbackQuery, state: FSMContext):
     await callback.message.answer(
         "💰 Пополнение баланса\n\n"
         "Введите сумму в USDT, которую хотите внести (например: 5):",
         reply_markup=ReplyKeyboardMarkup(
             keyboard=[[KeyboardButton(text="🔙 Отмена")]],
             resize_keyboard=True
         )
     )
     await state.set_state(DepositState.waiting_for_amount)
     await callback.answer()

@dp.message(DepositState.waiting_for_amount, F.text == "🔙 Отмена")
async def cancel_deposit(message: types.Message, state: FSMContext):
     await message.answer(
         "❌ Пополнение баланса отменено.",
         reply_markup=get_main_kb(message.from_user.id in ADMIN_IDS)
     )
     await state.clear()

@dp.message(DepositState.waiting_for_amount)
async def process_deposit_amount(message: types.Message, state: FSMContext):
     try:
         amount_usdt = float(message.text)
         if amount_usdt <= 0:
             raise ValueError

         user_id = message.from_user.id
         success, result = await process_deposit(user_id, amount_usdt)

         if success:
             invoice_url = result['pay_url']
             invoice_id = result['invoice_id']

             await message.answer(
                 f"✅ Счет на оплату создан!\n"
                 f"Сумма: {amount_usdt} USDT\n"
                 f"Для оплаты перейдите по ссылке: {invoice_url}\n\n"
                 "После оплаты баланс будет зачислен автоматически.",
                 reply_markup=get_main_kb(user_id in ADMIN_IDS)
             )

             # Запускаем проверку статуса платежа
             asyncio.create_task(check_payment_status(user_id, invoice_id, amount_usdt))
         else:
             await message.answer(
                 f"❌ Ошибка: {result}",
                 reply_markup=get_main_kb(user_id in ADMIN_IDS)
             )
     except ValueError:
         await message.answer("❌ Пожалуйста, введите положительное число.")
         return

     await state.clear()

async def check_payment_status(user_id: int, invoice_id: int, amount_usdt: float):
     max_attempts = 30  # Максимальное количество проверок
     attempt = 0

     while attempt < max_attempts:
         await asyncio.sleep(10)  # Проверяем каждые 10 секунд

         invoice_data = await check_invoice_status(invoice_id)

         if not invoice_data.get('ok', False):
             print(f"Ошибка при проверке счета {invoice_id}: {invoice_data.get('error', {}).get('name')}")
             attempt += 1
             continue

         invoice_status = invoice_data['result']['items'][0]['status']

         if invoice_status == 'paid':
             # Зачисляем средства
             amount_zb = int(amount_usdt / ZB_EXCHANGE_RATE)
             users[user_id]['balance'] += amount_zb

             try:
                 await bot.send_message(
                     user_id,
                     f"✅ Ваш баланс успешно пополнен на {amount_zb} Zebranium!"
                 )
             except Exception as e:
                 print(f"Ошибка при отправке уведомления пользователю {user_id}: {e}")

             return

         elif invoice_status in ['expired', 'cancelled']:
             try:
                 await bot.send_message(
                     user_id,
                     f"❌ Счет на оплату {amount_usdt} USDT был отменен или истек."
                 )
             except Exception:
                 pass
             return

         attempt += 1

     # Если платеж не подтвердился за отведенное время
     try:
         await bot.send_message(
             user_id,
             f"❌ Время ожидания платежа истекло. Если вы произвели оплату, обратитесь в поддержку."
         )
     except Exception:
         pass

@dp.callback_query(F.data == "withdraw_funds")
async def withdraw_funds_handler(callback: types.CallbackQuery, state: FSMContext):
     user_id = callback.from_user.id
     user_data = users.get(user_id, {})
     balance = user_data.get('balance', 0)
     max_withdraw_zb = int(balance)
     max_withdraw_usdt = max_withdraw_zb * ZB_EXCHANGE_RATE

     await callback.message.answer(
         f"💸 Вывод средств\n\n"
         f"Доступно для вывода: {max_withdraw_zb} Zebranium (≈{max_withdraw_usdt:.2f} USDT)\n"
         f"Минимальная сумма вывода: {MIN_WITHDRAWAL} USDT\n\n"
         f"Введите сумму Zebranium для вывода (целое число):",
         reply_markup=ReplyKeyboardMarkup(
             keyboard=[[KeyboardButton(text="🔙 Отмена")]],
             resize_keyboard=True
         )
     )
     await state.set_state(WithdrawState.waiting_for_amount)
     await callback.answer()

@dp.message(WithdrawState.waiting_for_amount, F.text == "🔙 Отмена")
async def cancel_withdrawal(message: types.Message, state: FSMContext):
     await message.answer(
         "❌ Вывод средств отменен.",
         reply_markup=get_main_kb(message.from_user.id in ADMIN_IDS)
     )
     await state.clear()

@dp.message(WithdrawState.waiting_for_amount)
async def process_withdrawal_amount(message: types.Message, state: FSMContext):
     try:
         amount_zb = int(message.text)
         if amount_zb <= 0:
             raise ValueError

         user_id = message.from_user.id
         success, result = await process_withdrawal(user_id, amount_zb)

         if success:
             await message.answer(
                 f"✅ Запрос на вывод {amount_zb} Zebranium (≈{amount_zb * ZB_EXCHANGE_RATE:.2f} USDT) принят!\n"
                 f"Для получения средств перейдите по ссылке: {result}",
                 reply_markup=get_main_kb(user_id in ADMIN_IDS)
             )
         else:
             await message.answer(
                 f"❌ Ошибка: {result}",
                 reply_markup=get_main_kb(user_id in ADMIN_IDS)
             )
     except ValueError:
         await message.answer("❌ Пожалуйста, введите целое положительное число.")
         return

     await state.clear()

@dp.message(F.text == "👥Рефералы")
@check_not_blocked
async def referrals_handler(message: types.Message, **kwargs):
     user_id = message.from_user.id
     bot_username = (await bot.get_me()).username
     link = f"https://t.me/{bot_username}?start={user_id}"
     count = len(users.get(user_id, {}).get("referrals", []))
     await message.answer(
         f"🔗 Ваша реферальная ссылка:\n{link}\n\n"
         f"👥 Приглашено пользователей: {count}\n\n"
         f"💎 Вы получаете {REFERRAL_REWARD} Zebranium за каждого приглашенного друга!"
     )

@dp.message(F.text == "⛏️Майнинг")
@check_not_blocked
async def mining_handler(message: types.Message, **kwargs):
     user_id = message.from_user.id
     if user_id not in users:
         await message.answer("❌ Сначала зарегистрируйтесь через /start")
         return

     user_data = users[user_id]
     now = datetime.now()

     if user_data['last_mine_time']:
         last_mine = datetime.strptime(user_data['last_mine_time'], '%d.%m.%Y %H:%M')
         delta = now - last_mine
         if delta.total_seconds() < MINING_COOLDOWN:
             wait_time = MINING_COOLDOWN - delta.total_seconds()
             hours = int(wait_time // 3600)
             minutes = int((wait_time % 3600) // 60)
             await message.answer(
                 f"⏳ Следующий майнинг доступен через: {hours} час. {minutes} мин.\n"
                 f"💎 Ваш баланс: {user_data['balance']} Zebranium"
             )
             return

     reward = random.randint(*MINING_REWARD_RANGE)
     users[user_id]['balance'] += reward
     users[user_id]['last_mine_time'] = now.strftime('%d.%m.%Y %H:%M')

     await message.answer(
         f"⛏ Вы успешно добыли {reward} Zebranium!\n"
         f"💎 Текущий баланс: {users[user_id]['balance']} ZB\n"
         f"⏳ Следующий майнинг через 1 час"
     )

@dp.message(F.text == "💼Задания")
@check_not_blocked
async def tasks_handler(message: types.Message, **kwargs):
     if not tasks:
         await message.answer("📭 На данный момент нет доступных заданий.")
         return

     tasks_list = "\n".join([f"• Задание {num}" for num in sorted(tasks.keys())])
     await message.answer(
         f"📋 Доступные задания:\n{tasks_list}\n\n"
         "Выберите задание из списка ниже:",
         reply_markup=get_tasks_kb()
     )

@dp.message(F.text == "📈Топы")
@check_not_blocked
async def tops_handler(message: types.Message, **kwargs):
     await message.answer("🏆 Выберите тип топа:", reply_markup=get_tops_type_kb())

@dp.message(F.text == "🏆 Топы приглашений")
@check_not_blocked
async def referral_top_type(message: types.Message, state: FSMContext, **kwargs):
     await message.answer("📅 Выберите период:", reply_markup=get_period_kb())
     await state.set_state(TopStates.waiting_referral_period)

@dp.message(F.text == "🏆 Топы заданий")
@check_not_blocked
async def tasks_top_type(message: types.Message, state: FSMContext, **kwargs):
     await message.answer("📅 Выберите период:", reply_markup=get_period_kb())
     await state.set_state(TopStates.waiting_task_period)

@dp.message(TopStates.waiting_referral_period, F.text.in_(["📅 Топ недели", "📅 Топ месяца"]))
@check_not_blocked
async def referral_top_period(message: types.Message, state: FSMContext, **kwargs):
     period = "week" if "недели" in message.text else "month"
     top_text = await get_referral_top(period)
     await message.answer(top_text, reply_markup=get_tops_type_kb())
     await state.clear()

@dp.message(TopStates.waiting_task_period, F.text.in_(["📅 Топ недели", "📅 Топ месяца"]))
@check_not_blocked
async def tasks_top_period(message: types.Message, state: FSMContext, **kwargs):
     period = "week" if "недели" in message.text else "month"
     top_text = await get_tasks_top(period)
     await message.answer(top_text, reply_markup=get_tops_type_kb())
     await state.clear()

@dp.message(F.text.regexp(r'^Задание \d+$'))
@check_not_blocked
async def show_task(message: types.Message, **kwargs):
     try:
         task_num = int(message.text.split()[1])
         task = tasks.get(task_num)

         if not task:
             await message.answer(f"⚠️ Задание {task_num} не найдено.")
             return

         response = f"📌 Задание {task_num}\n\n{task['text']}"

         if task.get('photo'):
             await message.answer_photo(
                 task['photo'],
                 caption=response,
                 reply_markup=get_task_kb(task_num)
             )
         else:
             await message.answer(
                 response,
                 reply_markup=get_task_kb(task_num)
             )

     except Exception:
         await message.answer("❌ Ошибка при загрузке задания.")

@dp.message(F.text.regexp(r'^✅ Выполнил задание \d+$'))
@check_not_blocked
async def task_complete_handler(message: types.Message, state: FSMContext, **kwargs):
     try:
         task_num = int(message.text.split()[-1])

         if task_num not in tasks:
             await message.answer(f"❌ Задание {task_num} не существует.")
             return

         await message.answer(
             "📎 Пришлите фото или скриншот в качестве доказательства выполнения задания:",
             reply_markup=ReplyKeyboardMarkup(
                 keyboard=[[KeyboardButton(text="🔙 Отмена")]],
                 resize_keyboard=True
             )
         )
         await state.update_data(task_num=task_num)
         await state.set_state(TaskStates.waiting_for_proof)

     except Exception:
         await message.answer("❌ Произошла ошибка. Пожалуйста, попробуйте еще раз.")

@dp.message(TaskStates.waiting_for_proof, F.photo)
async def process_task_proof(message: types.Message, state: FSMContext, **kwargs):
     data = await state.get_data()
     task_num = data['task_num']
     user_id = message.from_user.id
     username = users.get(user_id, {}).get('username', 'нет username')

     pending_approvals[(user_id, task_num)] = {
         'photo': message.photo[-1].file_id,
         'date': datetime.now()
     }

     proof_keyboard = InlineKeyboardMarkup(inline_keyboard=[
         [
             InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_{user_id}_{task_num}"),
             InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{user_id}_{task_num}")
         ]
     ])

     for admin_id in ADMIN_IDS:
         try:
             await bot.send_photo(
                 admin_id,
                 photo=message.photo[-1].file_id,
                 caption=f"🆔 Пользователь @{username} (ID: {user_id}) выполнил задание {task_num}!",
                 reply_markup=proof_keyboard
             )
         except Exception:
             pass

     await message.answer(
         "✅ Ваше доказательство отправлено администраторам на проверку!",
         reply_markup=get_main_kb(user_id in ADMIN_IDS)
     )
     await state.clear()

@dp.message(TaskStates.waiting_for_proof, F.text == "🔙 Отмена")
async def cancel_proof_upload(message: types.Message, state: FSMContext, **kwargs):
     await message.answer(
         "❌ Отправка доказательства отменена.",
         reply_markup=get_main_kb(message.from_user.id in ADMIN_IDS)
     )
     await state.clear()

@dp.callback_query(F.data.startswith("accept_"))
async def accept_proof(callback: types.CallbackQuery, **kwargs):
     if callback.from_user.id not in ADMIN_IDS:
         await callback.answer("⛔ У вас нет прав для этого действия!")
         return

     _, user_id_str, task_num_str = callback.data.split('_')
     user_id = int(user_id_str)
     task_num = int(task_num_str)

     if (user_id, task_num) not in pending_approvals:
         await callback.answer("⚠️ Доказательство уже обработано!")
         return

     proof_data = pending_approvals.pop((user_id, task_num))
     task_proofs[user_id][task_num] = proof_data['photo']
     task_completion_dates[user_id][task_num] = proof_data['date']

     reward = random.randint(*TASK_REWARD_RANGE)
     if user_id in users:
         users[user_id]['balance'] += reward

     try:
         await bot.send_message(
             user_id,
             f"🎉 Ваше доказательство для задания {task_num} было принято!\n"
             f"💎 Вы получили {reward} Zebranium!"
         )
     except Exception:
         pass

     await callback.answer("✅ Доказательство принято!")
     await callback.message.edit_reply_markup(reply_markup=None)

@dp.callback_query(F.data.startswith("reject_"))
async def reject_proof(callback: types.CallbackQuery, **kwargs):
     if callback.from_user.id not in ADMIN_IDS:
         await callback.answer("⛔ У вас нет прав для этого действия!")
         return

     _, user_id_str, task_num_str = callback.data.split('_')
     user_id = int(user_id_str)
     task_num = int(task_num_str)

     if (user_id, task_num) not in pending_approvals:
         await callback.answer("⚠️ Доказательство уже обработано!")
         return

     pending_approvals.pop((user_id, task_num))

     try:
         await bot.send_message(
             user_id,
             f"❌ Ваше доказательство для задания {task_num} было отклонено администратором."
         )
     except Exception:
         pass

     await callback.answer("❌ Доказательство отклонено!")
     await callback.message.edit_reply_markup(reply_markup=None)

@dp.message(F.text == "✉️Помощь")
@check_not_blocked
async def help_handler(message: types.Message, **kwargs):
     await message.answer(
         "ℹ️ Справка по боту:\n\n"
         "• 👀Профиль - ваши данные\n"
         "• 👥Рефералы - ваша реферальная ссылка\n"
         "• ⛏️Майнинг - добыча Zebranium каждые 60 минут\n"
         "• 💼Задания - выполнение заданий за награду\n"
         "• 📈Топы - рейтинги пользователей\n\n"
         "После выполнения задания нажмите '✅ Выполнил' и отправьте доказательство.\n\n"
         "По всем вопросам обращайтесь к администратору."
     )

@dp.message(F.text == "🔙 Назад")
@check_not_blocked
async def back_handler(message: types.Message, state: FSMContext, **kwargs):
     current_state = await state.get_state()

     if current_state in [TopStates.waiting_referral_period, TopStates.waiting_task_period]:
         await message.answer("🏆 Выберите тип топа:", reply_markup=get_tops_type_kb())
         await state.set_state(TopStates.waiting_top_type)
     else:
         is_admin = message.from_user.id in ADMIN_IDS
         await message.answer("↩️ Возврат в главное меню", reply_markup=get_main_kb(is_admin))
         await state.clear()

@dp.message(F.text == "👑Админка")
async def admin_panel(message: types.Message, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         await message.answer("⛔ Доступ запрещен.")
         return

     await message.answer("👑 Админ-панель", reply_markup=get_admin_kb())

@dp.message(F.text == "💼 Задания")
async def tasks_admin_menu(message: types.Message, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     await message.answer("📋 Управление заданиями:", reply_markup=get_tasks_admin_kb())

@dp.message(F.text == "➕ Добавить задание")
async def add_task_start(message: types.Message, state: FSMContext, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     await message.answer("Введите номер нового задания (только цифры):")
     await state.set_state(AddTaskState.waiting_for_task_number)

@dp.message(AddTaskState.waiting_for_task_number)
async def process_task_number(message: types.Message, state: FSMContext, **kwargs):
     try:
         task_num = int(message.text)
         if task_num <= 0:
             raise ValueError
         if task_num in tasks:
             await message.answer(f"❌ Задание {task_num} уже существует.")
             return
     except ValueError:
         await message.answer("❌ Некорректный номер. Введите положительное число.")
         return

     await state.update_data(task_num=task_num)
     await message.answer("Введите текст задания:")
     await state.set_state(AddTaskState.waiting_for_task_text)

@dp.message(AddTaskState.waiting_for_task_text)
async def process_task_text(message: types.Message, state: FSMContext, **kwargs):
     if not message.text:
         await message.answer("❌ Текст задания не может быть пустым.")
         return

     await state.update_data(text=message.text)
     await message.answer(
         "Отправьте фото для задания (если нужно) или нажмите 'Пропустить':",
         reply_markup=ReplyKeyboardMarkup(
             keyboard=[[KeyboardButton(text="Пропустить")]],
             resize_keyboard=True
         )
     )
     await state.set_state(AddTaskState.waiting_for_task_photo)

@dp.message(AddTaskState.waiting_for_task_photo, F.text == "Пропустить")
async def skip_task_photo(message: types.Message, state: FSMContext, **kwargs):
     data = await state.get_data()
     tasks[data['task_num']] = {
         'text': data['text'],
         'photo': None
     }
     await message.answer(
         f"✅ Задание {data['task_num']} успешно добавлено!",
         reply_markup=get_admin_kb()
     )
     await state.clear()

@dp.message(AddTaskState.waiting_for_task_photo, F.photo)
async def add_task_with_photo(message: types.Message, state: FSMContext, **kwargs):
     data = await state.get_data()
     tasks[data['task_num']] = {
         'text': data['text'],
         'photo': message.photo[-1].file_id
     }
     await message.answer(
         f"✅ Задание {data['task_num']} с фото успешно добавлено!",
         reply_markup=get_admin_kb()
     )
     await state.clear()

@dp.message(F.text == "❌ Удалить задание")
async def delete_task_start(message: types.Message, state: FSMContext, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     if not tasks:
         await message.answer("❌ Нет доступных заданий для удаления.")
         return

     tasks_list = "\n".join([f"Задание {num}" for num in sorted(tasks.keys())])
     await message.answer(
         f"📝 Список заданий:\n{tasks_list}\n\n"
         "Введите номер задания для удаления:"
     )
     await state.set_state(DeleteTaskState.waiting_for_task_number)

@dp.message(DeleteTaskState.waiting_for_task_number)
async def delete_task_process(message: types.Message, state: FSMContext, **kwargs):
     try:
         task_num = int(message.text)
         if task_num not in tasks:
             await message.answer(f"❌ Задание {task_num} не существует.")
             return

         del tasks[task_num]

         # Удаляем все связанные данные
         for user_id in task_proofs:
             if task_num in task_proofs[user_id]:
                 del task_proofs[user_id][task_num]
             if task_num in task_completion_dates[user_id]:
                 del task_completion_dates[user_id][task_num]

         # Удаляем ожидающие подтверждения
         keys_to_delete = [key for key in pending_approvals.keys() if key[1] == task_num]
         for key in keys_to_delete:
             del pending_approvals[key]

         await message.answer(
             f"✅ Задание {task_num} успешно удалено!",
             reply_markup=get_admin_kb()
         )
     except ValueError:
         await message.answer("❌ Некорректный номер задания.")
     await state.clear()

@dp.message(F.text == "📊 Статистика")
async def stats_handler(message: types.Message, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     completed_tasks = sum(len(proofs) for proofs in task_proofs.values())
     total_balance = sum(user.get('balance', 0) for user in users.values())

     await message.answer(
         f"📊 Статистика бота:\n\n"
         f"👤 Пользователей: {len(users)}\n"
         f"💰 Всего Zebranium: {total_balance}\n"
         f"📝 Заданий: {len(tasks)}\n"
         f"✅ Выполнено заданий: {completed_tasks}\n"
         f"🚫 Заблокировано: {len(blocked_users)}\n"
         f"⏳ Ожидают проверки: {len(pending_approvals)}"
     )

@dp.message(F.text == "📥 Экспорт данных")
async def export_users_data(message: types.Message, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     if not users:
         await message.answer("Нет данных для экспорта.")
         return

     try:
         data = []
         for user_id, user_data in users.items():
             completed_tasks = task_proofs.get(user_id, {})
             task_dates = [
                 task_completion_dates.get(user_id, {}).get(task_num, datetime.min).strftime('%d.%m.%Y %H:%M')
                 for task_num in completed_tasks
             ]

             data.append({
                 "ID": user_id,
                 "Username": f"@{user_data.get('username', '—')}",
                 "Дата регистрации": user_data.get('reg_date', '—'),
                 "Баланс ZB": user_data.get('balance', 0),
                 "Рефералов": len(user_data.get('referrals', [])),
                 "Выполнено заданий": len(completed_tasks),
                 "Номера заданий": ", ".join(map(str, completed_tasks.keys())) if completed_tasks else "—",
                 "Даты выполнения": "; ".join(task_dates) if task_dates else "—",
                 "Статус": "Заблокирован" if user_id in blocked_users else "Активен"
             })

         df = pd.DataFrame(data)

         excel_buffer = io.BytesIO()
         with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
             df.to_excel(writer, index=False, sheet_name='Пользователи')

             workbook = writer.book
             worksheet = writer.sheets['Пользователи']

             column_widths = {
                 'A:A': 12,
                 'B:B': 20,
                 'C:C': 20,
                 'D:D': 15,
                 'E:E': 10,
                 'F:F': 15,
                 'G:G': 20,
                 'H:H': 30,
                 'I:I': 12
             }

             for cols, width in column_widths.items():
                 worksheet.set_column(cols, width)

             header_format = workbook.add_format({'bold': True})
             for col_num, value in enumerate(df.columns.values):
                 worksheet.write(0, col_num, value, header_format)

         excel_buffer.seek(0)

         await message.answer_document(
             BufferedInputFile(
                 excel_buffer.getvalue(),
                 filename="users_export.xlsx"
             ),
             caption="📊 Экспорт данных пользователей"
         )

     except Exception as e:
         await message.answer(f"❌ Ошибка при экспорте данных: {str(e)}")

@dp.message(F.text == "🧾 Список пользователей")
async def users_list_handler(message: types.Message, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     if not users:
         await message.answer("Список пользователей пуст.")
         return

     text = "🧾 Список пользователей:\n\n"
     for uid, data in users.items():
         completed = len(task_proofs.get(uid, {}))
         text += f"{uid} - @{data.get('username', '—')} (💰 {data.get('balance', 0)} ZB | ✅ {completed})\n"

     for i in range(0, len(text), 4000):
         await message.answer(text[i:i+4000])

@dp.message(F.text == "🚫 Заблокировать")
async def block_user_start(message: types.Message, state: FSMContext, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     await message.answer("Введите ID пользователя для блокировки:")
     await state.set_state(BlockState.waiting_for_id)

@dp.message(BlockState.waiting_for_id)
async def block_user_process(message: types.Message, state: FSMContext, **kwargs):
     try:
         user_id = int(message.text)
     except ValueError:
         await message.answer("❌ Неверный формат ID.")
         await state.clear()
         return

     blocked_users.add(user_id)
     await message.answer(f"✅ Пользователь {user_id} заблокирован.")
     await state.clear()

@dp.message(F.text == "🔓 Разблокировать")
async def unblock_user_start(message: types.Message, state: FSMContext, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     await message.answer("Введите ID пользователя для разблокировки:")
     await state.set_state(UnblockState.waiting_for_id)

@dp.message(UnblockState.waiting_for_id)
async def unblock_user_process(message: types.Message, state: FSMContext, **kwargs):
     try:
         user_id = int(message.text)
     except ValueError:
         await message.answer("❌ Неверный формат ID.")
         await state.clear()
         return

     if user_id in blocked_users:
         blocked_users.remove(user_id)
         await message.answer(f"✅ Пользователь {user_id} разблокирован.")
     else:
         await message.answer(f"ℹ️ Пользователь {user_id} не был заблокирован.")
     await state.clear()

@dp.message(F.text == "📨 Рассылка")
async def broadcast_start(message: types.Message, state: FSMContext, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     await message.answer("Введите сообщение для рассылки:")
     await state.set_state(BroadcastState.waiting_for_message)

@dp.message(BroadcastState.waiting_for_message)
async def broadcast_process(message: types.Message, state: FSMContext, **kwargs):
     text = message.text
     success = 0
     errors = 0

     for user_id in users:
         try:
             await bot.send_message(user_id, text)
             success += 1
         except Exception:
             errors += 1

     await message.answer(
         f"📨 Результаты рассылки:\n\n"
         f"✅ Успешно: {success}\n"
         f"❌ Ошибок: {errors}"
     )
     await state.clear()

@dp.message(F.text == "✏️ Редактировать пользователя")
async def edit_user_start(message: types.Message, state: FSMContext, **kwargs):
     if message.from_user.id not in ADMIN_IDS:
         return

     await message.answer("Введите ID пользователя для редактирования:")
     await state.set_state(EditUserState.waiting_for_id)

@dp.message(EditUserState.waiting_for_id)
async def process_user_id(message: types.Message, state: FSMContext, **kwargs):
     try:
         user_id = int(message.text)
     except ValueError:
         await message.answer("❌ Неверный формат ID.")
         await state.clear()
         return

     if user_id not in users:
         await message.answer("❌ Пользователь с таким ID не найден.")
         await state.clear()
         return

     await state.update_data(user_id=user_id)
     await message.answer(
         f"✏️ Редактирование пользователя {user_id}\n"
         f"Выберите параметр для изменения:",
         reply_markup=get_edit_user_kb()
     )
     await state.set_state(EditUserState.waiting_for_field)

@dp.message(EditUserState.waiting_for_field, F.text.in_(["💰 Баланс", "👥 Рефералы", "✅ Выполнено заданий"]))
async def process_edit_field(message: types.Message, state: FSMContext, **kwargs):
     field_map = {
         "💰 Баланс": "balance",
         "👥 Рефералы": "referrals",
         "✅ Выполнено заданий": "completed_tasks"
     }

     await state.update_data(field=field_map[message.text])
     await message.answer(
         f"Введите новое значение для {message.text.lower()}:",
         reply_markup=ReplyKeyboardMarkup(
             keyboard=[[KeyboardButton(text="🔙 Назад")]],
             resize_keyboard=True
         )
     )
     await state.set_state(EditUserState.waiting_for_value)

@dp.message(EditUserState.waiting_for_field, F.text == "🔙 Назад")
async def back_from_edit_user(message: types.Message, state: FSMContext, **kwargs):
     await message.answer("👑 Админ-панель", reply_markup=get_admin_kb())
     await state.clear()

@dp.message(EditUserState.waiting_for_value, F.text == "🔙 Назад")
async def back_from_edit_value(message: types.Message, state: FSMContext, **kwargs):
     data = await state.get_data()
     await message.answer(
         f"✏️ Редактирование пользователя {data['user_id']}\n"
         f"Выберите параметр для изменения:",
         reply_markup=get_edit_user_kb()
     )
     await state.set_state(EditUserState.waiting_for_field)

@dp.message(EditUserState.waiting_for_value)
async def process_edit_value(message: types.Message, state: FSMContext, **kwargs):
     data = await state.get_data()
     user_id = data['user_id']
     field = data['field']

     try:
         if field == "balance":
             new_value = int(message.text)
             users[user_id]['balance'] = new_value
         elif field == "referrals":
             new_value = int(message.text)
             users[user_id]['referrals'] = [0] * new_value
         elif field == "completed_tasks":
             new_value = int(message.text)

             # Очищаем текущие задания пользователя
             if user_id in task_proofs:
                 del task_proofs[user_id]
             if user_id in task_completion_dates:
                 del task_completion_dates[user_id]

             # Добавляем новые задания
             if new_value > 0:
                 available_tasks = sorted(tasks.keys())
                 tasks_to_add = min(new_value, len(available_tasks))

                 task_proofs[user_id] = {}
                 task_completion_dates[user_id] = {}

                 for task_num in available_tasks[:tasks_to_add]:
                     task_proofs[user_id][task_num] = "manually_added_by_admin"
                     task_completion_dates[user_id][task_num] = datetime.now().strftime('%d.%m.%Y %H:%M')

         await message.answer(
             f"✅ Данные пользователя {user_id} успешно обновлены!",
             reply_markup=get_admin_kb()
         )
         await state.clear()
     except ValueError:
         await message.answer("❌ Неверный формат значения. Введите целое число.")

# =====================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ТОПОВ
# =====================

async def get_referral_top(period: str = "week") -> str:
     now = datetime.now()

     if period == "week":
         start_date = now - timedelta(days=7)
     else:  # month
         start_date = now - timedelta(days=30)

     top_users = []
     for user_id, user_data in users.items():
         referral_count = 0
         for ref_id in user_data.get('referrals', []):
             if ref_id in users and 'reg_date' in users[ref_id]:
                 try:
                     ref_reg_date = datetime.strptime(users[ref_id]['reg_date'], '%d.%m.%Y %H:%M')
                     if ref_reg_date >= start_date:
                         referral_count += 1
                 except ValueError:
                     continue
         if referral_count > 0:
             username = user_data.get('username', '—')
             top_users.append((user_id, referral_count, username))

     top_users.sort(key=lambda x: x[1], reverse=True)

     if not top_users:
         return "🏆 Топ приглашений пуст за выбранный период."

     result = f"🏆 Топ приглашений за {'неделю' if period == 'week' else 'месяц'}:\\n\\n"
     for i, (user_id, count, username) in enumerate(top_users[:10], 1):
         result += f"{i}. @{username} (ID: {user_id}) - {count} рефералов\\n"

     return result

async def get_tasks_top(period: str = "week") -> str:
     now = datetime.now()

     if period == "week":
         start_date = now - timedelta(days=7)
     else:  # month
         start_date = now - timedelta(days=30)

     top_users = []
     for user_id, user_tasks in task_completion_dates.items():
         count = 0
         for task_num, task_date in user_tasks.items():
             # task_date может быть datetime object или строкой
             if isinstance(task_date, datetime):
                 if task_date >= start_date:
                     count += 1
             elif isinstance(task_date, str):
                 try:
                     date_obj = datetime.strptime(task_date, '%d.%m.%Y %H:%M')
                     if date_obj >= start_date:
                         count += 1
                 except ValueError:
                     continue

         if count > 0:
             username = users.get(user_id, {}).get('username', '—')
             top_users.append((user_id, count, username))

     top_users.sort(key=lambda x: x[1], reverse=True)

     if not top_users:
         return "🏆 Топ заданий пуст за выбранный период."

     result = f"🏆 Топ заданий за {'неделю' if period == 'week' else 'месяц'}:\\n\\n"
     for i, (user_id, count, username) in enumerate(top_users[:10], 1):
         result += f"{i}. @{username} (ID: {user_id}) - {count} заданий\\n"

     return result

# =====================
# ЗАПУСК БОТА И ВЕБ-СЕРВЕРА
# =====================

async def run_bot():
     while True:
         try:
             logging.info("Запуск бота в режиме polling...")
             await dp.start_polling(bot, skip_updates=True)
         except TelegramForbiddenError as e:
             logging.error(f"Бот заблокирован пользователем или ошибка токена: {e}")
             # В продакшене можно добавить логику для удаления пользователя из рассылки и т.д.
             await asyncio.sleep(60) # Подождать перед повторной попыткой
         except Exception as e:
             logging.error(f"Произошла ошибка при запуске бота: {e}")
             await asyncio.sleep(5) # Ждем перед перезапуском

async def fake_server(request):
    """
    Простой HTTP сервер, чтобы Render видел открытый порт.
    """
    return web.Response(text="Bot is running and listening for Telegram updates!")

async def main():
    # Запускаем фейковый веб-сервер
    port = int(os.environ.get("PORT", 8080)) # Render предоставляет порт через переменную окружения PORT
    app = web.Application()
    app.router.add_get("/", fake_server)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    logging.info(f"Запускаем фейковый веб-сервер на порту {port}")
    await site.start()

    # Запускаем бота
    await run_bot() # run_bot уже содержит бесконечный цикл и обработку ошибок

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен вручную.")
    except Exception as e:
        logging.critical(f"Критическая ошибка в главном процессе: {e}")
