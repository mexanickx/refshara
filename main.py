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
TOKEN = os.environ.get("7740361367:AAGAnKLBl9G_2ooB7UbIpAiOB5YfUzsw9fs")
import threading
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

# запускаем фейковый сервер, чтобы Render не ругался
def run_fake_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'Bot is running!')

    server_address = ('0.0.0.0', 10000)
    httpd = HTTPServer(server_address, Handler)
    print(f"Запускаем фейковый сервер на порту {server_address[1]}")
    httpd.serve_forever()

threading.Thread(target=run_fake_server, daemon=True).start()

# Замените на ваш токен бота и токен Crypto Bot API
# BOT_TOKEN = os.environ.get('BOT_TOKEN') # Замените на ваш токен бота
BOT_TOKEN = "7740361367:AAGAnKLBl9G_2ooB7UbIpAiOB5YfUzsw9fs" # Замените на ваш токен бота
CRYPTO_BOT_TOKEN = "15372:AAyD0R4vM9Ld1qF2V3yqfE55L17e7Ff92g3tN" # Замените на ваш токен Crypto Bot API
CRYPTO_BOT_API_URL = "https://pay.crypt.bot/api/"

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# =====================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# =====================
users = {}  # Словарь для хранения информации о пользователях
tasks = {}  # Словарь для хранения заданий
invoices_in_progress = {} # {invoice_id: user_id}

# =====================
# ХЭНДЛЕРЫ КОМАНД
# =====================

@dp.message(CommandStart())
async def send_welcome(message: types.Message):
    user_id = message.from_user.id
    if user_id not in users:
        users[user_id] = {
            'username': message.from_user.username if message.from_user.username else f"id{user_id}",
            'balance': 0.0,
            'reg_date': datetime.now().strftime('%d.%m.%Y %H:%M')
        }
        await save_data()
    await message.reply("Привет! Я ваш бот для управления задачами и балансом. Используйте меню для навигации.")
    await show_main_menu(message)

# =====================
# ГЛАВНОЕ МЕНЮ И КНОПКИ
# =====================

async def show_main_menu(message: types.Message):
    markup = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🗂 Мои задания")],
            [KeyboardButton(text="💰 Пополнить баланс"), KeyboardButton(text="💸 Вывести средства")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Профиль")]
        ],
        resize_keyboard=True
    )
    await message.answer("Выберите действие:", reply_markup=markup)

@dp.message(F.text == "Вернуться в главное меню")
async def back_to_main_menu(message: types.Message):
    await show_main_menu(message)

# =====================
# СОСТОЯНИЯ ДЛЯ FSM
# =====================

class CreateTask(StatesGroup):
    waiting_for_task_description = State()
    waiting_for_task_price = State()

class Deposit(StatesGroup):
    waiting_for_deposit_amount = State()

class Withdraw(StatesGroup):
    waiting_for_withdraw_amount = State()
    waiting_for_wallet_address = State()

class CompleteTask(StatesGroup):
    waiting_for_task_id_to_complete = State()

class ChooseRatingPeriod(StatesGroup):
    waiting_for_period_selection = State()

# =====================
# ХЭНДЛЕРЫ ТЕКСТОВЫХ КНОПОК
# =====================

@dp.message(F.text == "🗂 Мои задания")
async def my_tasks_handler(message: types.Message):
    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text="➕ Создать задание", callback_data="create_task"))
    keyboard.add(InlineKeyboardButton(text="📝 Доступные задания", callback_data="show_available_tasks"))
    keyboard.add(InlineKeyboardButton(text="✅ Завершить задание", callback_data="complete_task"))
    await message.answer("Управление заданиями:", reply_markup=keyboard.as_markup())

@dp.message(F.text == "💰 Пополнить баланс")
async def deposit_handler(message: types.Message, state: FSMContext):
    await message.answer("Введите сумму пополнения в USDT (например, 1.0):")
    await state.set_state(Deposit.waiting_for_deposit_amount)

@dp.message(F.text == "💸 Вывести средства")
async def withdraw_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    balance = users.get(user_id, {}).get('balance', 0.0)
    await message.answer(f"Ваш текущий баланс: {balance:.2f} USDT. Введите сумму для вывода:")
    await state.set_state(Withdraw.waiting_for_withdraw_amount)

@dp.message(F.text == "📊 Статистика")
async def stats_handler(message: types.Message):
    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text="🏆 Топ заданий", callback_data="top_tasks"))
    await message.answer("Статистика:", reply_markup=keyboard.as_markup())

@dp.message(F.text == "⚙️ Профиль")
async def profile_handler(message: types.Message):
    user_id = message.from_user.id
    user_data = users.get(user_id, {})
    username = user_data.get('username', '—')
    balance = user_data.get('balance', 0.0)
    reg_date = user_data.get('reg_date', '—')
    num_created_tasks = sum(1 for task_id, task in tasks.items() if task['creator_id'] == user_id)
    num_completed_tasks = sum(1 for task_id, task in tasks.items() if task.get('executor_id') == user_id and task['status'] == 'completed')

    await message.answer(
        f"👤 Ваш профиль:\n\n"
        f"ID: `{user_id}`\n"
        f"Имя пользователя: @{username}\n"
        f"Баланс: {balance:.2f} USDT\n"
        f"Дата регистрации: {reg_date}\n"
        f"Создано заданий: {num_created_tasks}\n"
        f"Выполнено заданий: {num_completed_tasks}",
        parse_mode="Markdown"
    )

# =====================
# ОБРАБОТКА КОЛЛБЭКОВ (ИНЛАЙН КНОПКИ)
# =====================

@dp.callback_query(F.data == "create_task")
async def start_create_task(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите описание задания:")
    await state.set_state(CreateTask.waiting_for_task_description)
    await callback.answer()

@dp.callback_query(F.data == "show_available_tasks")
async def show_available_tasks(callback: types.CallbackQuery):
    available_tasks = [task for task_id, task in tasks.items() if task['status'] == 'available' and task['creator_id'] != callback.from_user.id]
    if not available_tasks:
        await callback.message.answer("Нет доступных заданий.")
        return

    for task_id, task in available_tasks:
        creator_username = users.get(task['creator_id'], {}).get('username', f"id{task['creator_id']}")
        keyboard = InlineKeyboardBuilder()
        keyboard.add(InlineKeyboardButton(text="🤝 Взять задание", callback_data=f"take_task_{task_id}"))
        await callback.message.answer(
            f"📝 Задание ID: {task_id}\n"
            f"Описание: {task['description']}\n"
            f"Цена: {task['price']:.2f} USDT\n"
            f"Создатель: @{creator_username}",
            reply_markup=keyboard.as_markup()
        )
    await callback.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith('take_task_'))
async def take_task_callback(callback: types.CallbackQuery):
    task_id = callback.data.split('_')[2]
    user_id = callback.from_user.id

    if task_id not in tasks or tasks[task_id]['status'] != 'available':
        await callback.message.answer("Это задание уже недоступно или не существует.")
        await callback.answer()
        return

    if tasks[task_id]['creator_id'] == user_id:
        await callback.message.answer("Вы не можете взять собственное задание.")
        await callback.answer()
        return

    tasks[task_id]['status'] = 'in_progress'
    tasks[task_id]['executor_id'] = user_id
    tasks[task_id]['taken_date'] = datetime.now().strftime('%d.%m.%Y %H:%M')
    await save_data()

    creator_id = tasks[task_id]['creator_id']
    creator_username = users.get(creator_id, {}).get('username', f"id{creator_id}")

    try:
        await bot.send_message(creator_id,
                               f"🔔 Ваше задание '{tasks[task_id]['description']}' (ID: {task_id}) было взято пользователем @{callback.from_user.username} (ID: {user_id}).")
    except TelegramForbiddenError:
        print(f"Не удалось отправить уведомление пользователю {creator_id}. Бот заблокирован.")

    await callback.message.answer(f"Вы взяли задание ID: {task_id}. Удачи!")
    await callback.answer()

@dp.callback_query(F.data == "complete_task")
async def start_complete_task(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    in_progress_tasks = [task_id for task_id, task in tasks.items() if task.get('executor_id') == user_id and task['status'] == 'in_progress']

    if not in_progress_tasks:
        await callback.message.answer("У вас нет заданий в процессе выполнения.")
        await callback.answer()
        return

    response_text = "Ваши задания в процессе выполнения:\n\n"
    for task_id in in_progress_tasks:
        task = tasks[task_id]
        response_text += f"ID: {task_id}\nОписание: {task['description']}\nЦена: {task['price']:.2f} USDT\n\n"

    response_text += "Введите ID задания, которое вы хотите завершить:"
    await callback.message.answer(response_text)
    await state.set_state(CompleteTask.waiting_for_task_id_to_complete)
    await callback.answer()


@dp.callback_query(F.data == "top_tasks")
async def choose_top_period(callback: types.CallbackQuery, state: FSMContext):
    keyboard = InlineKeyboardBuilder()
    keyboard.add(InlineKeyboardButton(text="📈 За неделю", callback_data="top_tasks_week"))
    keyboard.add(InlineKeyboardButton(text="📈 За месяц", callback_data="top_tasks_month"))
    await callback.message.answer("Выберите период для топа заданий:", reply_markup=keyboard.as_markup())
    await state.set_state(ChooseRatingPeriod.waiting_for_period_selection)
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith('top_tasks_'), state=ChooseRatingPeriod.waiting_for_period_selection)
async def show_top_tasks_period(callback: types.CallbackQuery, state: FSMContext):
    period = callback.data.split('_')[2] # 'week' or 'month'
    result_message = get_top_users_by_completed_tasks(period)
    await callback.message.answer(result_message, parse_mode="Markdown")
    await state.clear()
    await callback.answer()

# =====================
# ХЭНДЛЕРЫ СОСТОЯНИЙ (FSM)
# =====================

@dp.message(CreateTask.waiting_for_task_description)
async def process_task_description(message: types.Message, state: FSMContext):
    await state.update_data(description=message.text)
    await message.answer("Теперь введите цену задания в USDT (например, 1.5):")
    await state.set_state(CreateTask.waiting_for_task_price)

@dp.message(CreateTask.waiting_for_task_price)
async def process_task_price(message: types.Message, state: FSMContext):
    try:
        price = float(message.text.replace(',', '.'))
        if price <= 0:
            await message.answer("Цена должна быть положительным числом. Пожалуйста, введите корректную цену:")
            return
        
        user_id = message.from_user.id
        if users[user_id]['balance'] < price:
            await message.answer(f"Недостаточно средств на балансе ({users[user_id]['balance']:.2f} USDT). Пополните баланс.")
            await state.clear()
            await show_main_menu(message)
            return

        data = await state.get_data()
        description = data['description']
        task_id = str(random.randint(100000, 999999))
        tasks[task_id] = {
            'creator_id': user_id,
            'description': description,
            'price': price,
            'status': 'available',
            'created_date': datetime.now().strftime('%d.%m.%Y %H:%M')
        }
        users[user_id]['balance'] -= price # Списываем средства при создании
        await save_data()

        await message.answer(f"✅ Задание создано! ID: {task_id}\nОписание: {description}\nЦена: {price:.2f} USDT. Средства заморожены до выполнения задания.")
        await state.clear()
        await show_main_menu(message)
    except ValueError:
        await message.answer("Некорректная сумма. Пожалуйста, введите число (например, 1.5):")


@dp.message(Deposit.waiting_for_deposit_amount)
async def process_deposit_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.replace(',', '.'))
        if amount <= 0:
            await message.answer("Сумма пополнения должна быть положительным числом. Пожалуйста, введите корректную сумму:")
            return

        user_id = message.from_user.id
        success, result = await process_deposit(user_id, amount)

        if success:
            pay_url = result['pay_url']
            invoice_id = result['invoice_id']
            invoices_in_progress[invoice_id] = user_id # Сохраняем user_id для проверки
            await message.answer(
                f"✅ Счет на оплату создан!\n"
                f"Сумма: {amount:.2f} USDT\n"
                f"Для оплаты перейдите по ссылке: {pay_url}\n\n"
                f"После оплаты баланс будет зачислен автоматически."
            )
            logging.info(f"Generated invoice for user {user_id}: {pay_url}")
        else:
            await message.answer(f"Ошибка при создании счета: {result}")
        await state.clear()
        await show_main_menu(message)
    except ValueError:
        await message.answer("Некорректная сумма. Пожалуйста, введите число (например, 1.0):")


@dp.message(Withdraw.waiting_for_withdraw_amount)
async def process_withdraw_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.replace(',', '.'))
        user_id = message.from_user.id
        balance = users.get(user_id, {}).get('balance', 0.0)

        if amount <= 0:
            await message.answer("Сумма вывода должна быть положительным числом. Пожалуйста, введите корректную сумму:")
            return
        if amount > balance:
            await message.answer(f"Недостаточно средств на балансе ({balance:.2f} USDT). Введите сумму не более вашего баланса:")
            return

        await state.update_data(withdraw_amount=amount)
        await message.answer("Теперь введите адрес USDT TRC20 кошелька для вывода:")
        await state.set_state(Withdraw.waiting_for_wallet_address)
    except ValueError:
        await message.answer("Некорректная сумма. Пожалуйста, введите число (например, 10.0):")

@dp.message(Withdraw.waiting_for_wallet_address)
async def process_wallet_address(message: types.Message, state: FSMContext):
    wallet_address = message.text.strip()
    # Здесь можно добавить валидацию адреса USDT TRC20
    if not wallet_address: # Простейшая проверка на непустой адрес
        await message.answer("Адрес кошелька не может быть пустым. Пожалуйста, введите корректный адрес:")
        return

    data = await state.get_data()
    amount = data['withdraw_amount']
    user_id = message.from_user.id

    # Имитация вывода средств
    users[user_id]['balance'] -= amount
    await save_data()

    await message.answer(f"✅ Заявка на вывод {amount:.2f} USDT на адрес `{wallet_address}` принята. Средства будут отправлены в ближайшее время.", parse_mode="Markdown")
    logging.info(f"Withdrawal request: User {user_id} wants to withdraw {amount} USDT to {wallet_address}")
    await state.clear()
    await show_main_menu(message)

@dp.message(CompleteTask.waiting_for_task_id_to_complete)
async def process_task_id_to_complete(message: types.Message, state: FSMContext):
    task_id = message.text.strip()
    user_id = message.from_user.id

    if task_id not in tasks:
        await message.answer("Задание с таким ID не найдено. Пожалуйста, введите корректный ID:")
        return

    task = tasks[task_id]
    if task.get('executor_id') != user_id or task['status'] != 'in_progress':
        await message.answer("Это задание либо не находится в процессе выполнения вами, либо уже завершено. Пожалуйста, введите корректный ID:")
        return

    task['status'] = 'completed'
    task['completed_date'] = datetime.now().strftime('%d.%m.%Y %H:%M')
    creator_id = task['creator_id']
    price = task['price']

    # Зачисление средств исполнителю
    users[user_id]['balance'] += price
    await save_data()

    await message.answer(f"✅ Задание ID: {task_id} успешно завершено! {price:.2f} USDT зачислены на ваш баланс.")

    # Уведомить создателя задания
    try:
        await bot.send_message(creator_id,
                               f"🔔 Ваше задание '{task['description']}' (ID: {task_id}) было отмечено как завершенное пользователем @{message.from_user.username} (ID: {user_id}).")
    except TelegramForbiddenError:
        print(f"Не удалось отправить уведомление пользователю {creator_id}. Бот заблокирован.")

    await state.clear()
    await show_main_menu(message)


# =====================
# ФУНКЦИИ ИНТЕГРАЦИИ С CRYPTO BOT API
# =====================

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
        print("Crypto Bot API Response:", response_data) # Для отладки
        return response_data
    except Exception as e:
        print(f"Error creating invoice: {e}")
        return {'ok': False, 'error': {'name': str(e)}}


async def process_deposit(user_id: int, amount_usdt: float) -> tuple[bool, dict]:
    if user_id not in users:
        return False, {"name": "Пользователь не найден"}
    invoice = await create_crypto_bot_invoice(user_id, amount_usdt)
    if not invoice:
        return False, {"name": "Ошибка при подключении к платежной системе"}
    if not invoice.get('ok', False):
        error_msg = invoice.get('error', {}).get('name', 'Неизвестная ошибка')
        return False, {"name": f"Ошибка платежной системы: {error_msg}"}
    if not invoice.get('result'):
        return False, {"name": "Платежная система не предоставила данные для оплаты"}
    
    invoice_id = invoice['result']['invoice_id']
    
    # Construct the compact startapp URL
    # This is the line that was changed/added to provide the compact link
    compact_pay_url = f"https://t.me/CryptoBot/app?startapp=invoice-{invoice_id}&mode=compact"

    return True, {
        'pay_url': compact_pay_url, # Use the newly constructed compact URL
        'invoice_id': invoice_id
    }


async def check_crypto_bot_invoices():
    headers = {
        'Crypto-Pay-API-Token': CRYPTO_BOT_TOKEN
    }
    # Проверяем только инвойсы, которые мы отслеживаем
    invoice_ids_to_check = list(invoices_in_progress.keys())
    if not invoice_ids_to_check:
        return

    # Crypto Bot API позволяет проверять несколько инвойсов через запятую
    payload = {'invoice_ids': ','.join(invoice_ids_to_check)}

    try:
        response = requests.get(
            f'{CRYPTO_BOT_API_URL}getInvoices',
            headers=headers,
            params=payload,
            timeout=15
        )
        response_data = response.json()
        if response_data.get('ok') and response_data.get('result'):
            for invoice in response_data['result']:
                invoice_id = str(invoice['invoice_id'])
                if invoice_id in invoices_in_progress:
                    if invoice['status'] == 'paid':
                        user_id = invoices_in_progress.pop(invoice_id) # Удаляем из отслеживаемых
                        amount = float(invoice['amount'])
                        if user_id in users:
                            users[user_id]['balance'] += amount
                            await save_data()
                            print(f"Баланс пользователя {user_id} пополнен на {amount} USDT.")
                            try:
                                await bot.send_message(user_id, f"✅ Ваш баланс пополнен на {amount:.2f} USDT!")
                            except TelegramForbiddenError:
                                print(f"Не удалось отправить уведомление пользователю {user_id}. Бот заблокирован.")
                        else:
                            print(f"Пользователь {user_id} не найден, но инвойс {invoice_id} оплачен.")
                    elif invoice['status'] == 'expired' or invoice['status'] == 'cancelled':
                        invoices_in_progress.pop(invoice_id, None) # Удаляем, если истек или отменен
                        print(f"Инвойс {invoice_id} истек/отменен.")
        else:
            print(f"Ошибка при получении инвойсов: {response_data.get('error', {}).get('name', 'Неизвестная ошибка')}")
    except Exception as e:
        print(f"Error checking invoices: {e}")


# =====================
# ФУНКЦИИ УПРАВЛЕНИЯ ДАННЫМИ
# =====================

DATA_FILE = 'bot_data.csv'

async def save_data():
    data_to_save = []
    for user_id, user_data in users.items():
        data_to_save.append({
            'type': 'user',
            'id': user_id,
            'username': user_data.get('username'),
            'balance': user_data.get('balance'),
            'reg_date': user_data.get('reg_date')
        })
    for task_id, task_data in tasks.items():
        data_to_save.append({
            'type': 'task',
            'id': task_id,
            'creator_id': task_data.get('creator_id'),
            'executor_id': task_data.get('executor_id'),
            'description': task_data.get('description'),
            'price': task_data.get('price'),
            'status': task_data.get('status'),
            'created_date': task_data.get('created_date'),
            'taken_date': task_data.get('taken_date'),
            'completed_date': task_data.get('completed_date')
        })
    df = pd.DataFrame(data_to_save)
    df.to_csv(DATA_FILE, index=False)
    print("Данные сохранены.")

async def load_data():
    global users, tasks
    if not os.path.exists(DATA_FILE):
        print("Файл данных не найден, создаем новые пустые данные.")
        users = {}
        tasks = {}
        return

    try:
        df = pd.read_csv(DATA_FILE)
        users = {}
        tasks = {}
        for index, row in df.iterrows():
            if row['type'] == 'user':
                user_id = int(row['id'])
                users[user_id] = {
                    'username': row.get('username'),
                    'balance': float(row.get('balance', 0.0)),
                    'reg_date': row.get('reg_date')
                }
            elif row['type'] == 'task':
                task_id = str(row['id'])
                tasks[task_id] = {
                    'creator_id': int(row.get('creator_id')),
                    'executor_id': int(row.get('executor_id')) if pd.notna(row.get('executor_id')) else None,
                    'description': row.get('description'),
                    'price': float(row.get('price', 0.0)),
                    'status': row.get('status'),
                    'created_date': row.get('created_date'),
                    'taken_date': row.get('taken_date'),
                    'completed_date': row.get('completed_date')
                }
        print("Данные загружены.")
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}. Начинаем с пустых данных.")
        users = {}
        tasks = {}

# =====================
# ФУНКЦИИ АНАЛИТИКИ
# =====================

def get_top_users_by_completed_tasks(period: str):
    user_completed_tasks_count = defaultdict(int)
    current_date = datetime.now()

    if period == 'week':
        start_date = current_date - timedelta(days=7)
    elif period == 'month':
        start_date = current_date - timedelta(days=30)
    else:
        return "Неизвестный период."

    for task_id, task in tasks.items():
        if task['status'] == 'completed' and task.get('completed_date'):
            task_date_str = task['completed_date']
            task_date = None

            # Attempt to parse as datetime object
            if isinstance(task_date_str, datetime):
                task_date = task_date_str
            elif isinstance(task_date_str, str):
                try:
                    task_date = datetime.strptime(task_date_str, '%d.%m.%Y %H:%M')
                except ValueError:
                    # Fallback for older formats if necessary, or log error
                    continue # Skip if date format is not recognized

            if task_date and task_date >= start_date:
                executor_id = task.get('executor_id')
                if executor_id:
                    user_completed_tasks_count[executor_id] += 1

    top_users = []
    for user_id, count in user_completed_tasks_count.items():
        if user_id in users:
            username = users[user_id].get('username', '—')
            top_users.append((user_id, count, username))

    top_users.sort(key=lambda x: x[1], reverse=True)

    if not top_users:
        return "🏆 Топ заданий пуст за выбранный период."

    result = f"🏆 Топ заданий за {'неделю' if period == 'week' else 'месяц'}:\n\n"
    for i, (user_id, count, username) in enumerate(top_users[:10], 1):
        result += f"{i}. @{username} (ID: {user_id}) - {count} заданий\n"

    return result

def get_top_users_by_created_tasks(period: str):
    user_created_tasks_count = defaultdict(int)
    current_date = datetime.now()

    if period == 'week':
        start_date = current_date - timedelta(days=7)
    elif period == 'month':
        start_date = current_date - timedelta(days=30)
    else:
        return "Неизвестный период."

    for task_id, task in tasks.items():
        if task.get('created_date'):
            task_date = task['created_date']
            if isinstance(task_date, datetime):
                if task_date >= start_date:
                    user_created_tasks_count[task['creator_id']] += 1
            elif isinstance(task_date, str):
                try:
                    date_obj = datetime.strptime(task_date, '%d.%m.%Y %H:%M')
                    if date_obj >= start_date:
                        user_created_tasks_count[task['creator_id']] += 1
                except ValueError:
                    continue

    top_users = []
    for user_id, count in user_created_tasks_count.items():
        if user_id in users:
            username = users[user_id].get('username', '—')
            top_users.append((user_id, count, username))

    top_users.sort(key=lambda x: x[1], reverse=True)

    if not top_users:
        return "🏆 Топ заданий пуст за выбранный период."

    result = f"🏆 Топ создателей заданий за {'неделю' if period == 'week' else 'месяц'}:\n\n"
    for i, (user_id, count, username) in enumerate(top_users[:10], 1):
        result += f"{i}. @{username} (ID: {user_id}) - {count} заданий\n"

    return result


def get_top_users_by_total_completed_tasks_all_time():
    user_completed_tasks_count = defaultdict(int)

    for task_id, task in tasks.items():
        if task['status'] == 'completed' and task.get('executor_id'):
            user_completed_tasks_count[task['executor_id']] += 1

    top_users = []
    for user_id, count in user_completed_tasks_count.items():
        username = users.get(user_id, {}).get('username', '—')
        top_users.append((user_id, count, username))

    top_users.sort(key=lambda x: x[1], reverse=True)

    if not top_users:
        return "🏆 Общий топ заданий пуст."

    result = "🏆 Общий топ выполненных заданий (за все время):\n\n"
    for i, (user_id, count, username) in enumerate(top_users[:10], 1):
        result += f"{i}. @{username} (ID: {user_id}) - {count} заданий\n"

    return result


def get_total_tasks_completed_count(period: str):
    current_date = datetime.now()
    count = 0

    if period == 'week':
        start_date = current_date - timedelta(days=7)
    elif period == 'month':
        start_date = current_date - timedelta(days=30)
    else:
        return "Неизвестный период."

    for task_id, task in tasks.items():
        if task['status'] == 'completed' and task.get('completed_date'):
            task_date_str = task['completed_date']
            task_date = None

            if isinstance(task_date_str, datetime):
                task_date = task_date_str
            elif isinstance(task_date_str, str):
                try:
                    task_date = datetime.strptime(task_date_str, '%d.%m.%Y %H:%M')
                except ValueError:
                    continue

            if task_date and task_date >= start_date:
                count += 1
    return count

def get_total_tasks_created_count(period: str):
    current_date = datetime.now()
    count = 0

    if period == 'week':
        start_date = current_date - timedelta(days=7)
    elif period == 'month':
        start_date = current_date - timedelta(days=30)
    else:
        return "Неизвестный период."

    for task_id, task in tasks.items():
        if task.get('created_date'):
            task_date_str = task['created_date']
            task_date = None

            if isinstance(task_date_str, datetime):
                task_date = task_date_str
            elif isinstance(task_date_str, str):
                try:
                    task_date = datetime.strptime(task_date_str, '%d.%m.%Y %H:%M')
                    if task_date >= start_date:
                        count += 1
                except ValueError:
                    continue

    return count


def get_total_users_registered_count(period: str):
    current_date = datetime.now()
    count = 0

    if period == 'week':
        start_date = current_date - timedelta(days=7)
    elif period == 'month':
        start_date = current_date - timedelta(days=30)
    else:
        return "Неизвестный период."

    for user_id, user_data in users.items():
        if user_data.get('reg_date'):
            reg_date_str = user_data['reg_date']
            reg_date = None

            if isinstance(reg_date_str, datetime):
                reg_date = reg_date_str
            elif isinstance(reg_date_str, str):
                try:
                    reg_date = datetime.strptime(reg_date_str, '%d.%m.%Y %H:%M')
                    if reg_date >= start_date:
                        count += 1
                except ValueError:
                    continue
    return count

def get_top_users_by_completed_tasks_general():
    user_completed_tasks_count = defaultdict(int)

    for task_id, task in tasks.items():
        if task['status'] == 'completed' and task.get('executor_id'):
            user_completed_tasks_count[task['executor_id']] += 1

    top_users = []
    for user_id, count in user_completed_tasks_count.items():
        username = users.get(user_id, {}).get('username', '—')
        top_users.append((user_id, count, username))

    top_users.sort(key=lambda x: x[1], reverse=True)

    if not top_users:
        return "🏆 Топ заданий пуст."

    result = "🏆 Топ заданий (за все время):\n\n"
    for i, (user_id, count, username) in enumerate(top_users[:10], 1):
        result += f"{i}. @{username} (ID: {user_id}) - {count} заданий\n"

    return result

def get_top_users_by_created_tasks_general():
    user_created_tasks_count = defaultdict(int)

    for task_id, task in tasks.items():
        if task.get('creator_id'):
            user_created_tasks_count[task['creator_id']] += 1

    top_users = []
    for user_id, count in user_created_tasks_count.items():
        username = users.get(user_id, {}).get('username', '—')
        top_users.append((user_id, count, username))

    top_users.sort(key=lambda x: x[1], reverse=True)

    if not top_users:
        return "🏆 Топ создателей заданий пуст."

    result = "🏆 Топ создателей заданий (за все время):\n\n"
    for i, (user_id, count, username) in enumerate(top_users[:10], 1):
        result += f"{i}. @{username} (ID: {user_id}) - {count} заданий\n"

    return result

def get_total_tasks_completed_count_general():
    count = 0
    for task_id, task in tasks.items():
        if task['status'] == 'completed':
            count += 1
    return count

def get_total_tasks_created_count_general():
    return len(tasks)

def get_total_users_registered_count_general():
    return len(users)

def get_balance_distribution():
    balances = [user['balance'] for user_id, user in users.items()]
    if not balances:
        return "Нет данных о балансах."

    total_balance = sum(balances)
    avg_balance = total_balance / len(balances)
    max_balance = max(balances)
    min_balance = min(balances)

    result = (
        f"💰 Распределение балансов:\n\n"
        f"Общий баланс всех пользователей: {total_balance:.2f} USDT\n"
        f"Средний баланс: {avg_balance:.2f} USDT\n"
        f"Максимальный баланс: {max_balance:.2f} USDT\n"
        f"Минимальный баланс: {min_balance:.2f} USDT\n"
    )
    return result

def get_task_status_distribution():
    status_counts = defaultdict(int)
    for task_id, task in tasks.items():
        status_counts[task['status']] += 1

    if not status_counts:
        return "Нет данных о статусах заданий."

    total_tasks = sum(status_counts.values())
    result = "📊 Распределение статусов заданий:\n\n"
    for status, count in status_counts.items():
        percentage = (count / total_tasks) * 100 if total_tasks > 0 else 0
        result += f"- {status.capitalize()}: {count} ({percentage:.2f}%)\n"
    return result

def get_average_task_price():
    prices = [task['price'] for task_id, task in tasks.items()]
    if not prices:
        return "Нет данных о ценах заданий."

    avg_price = sum(prices) / len(prices)
    return f"💲 Средняя цена задания: {avg_price:.2f} USDT"


# =====================
# ЗАПУСК БОТА
# =====================

async def run_bot():
    while True:
        try:
            print("Запуск бота в режиме polling...")
            await dp.start_polling(bot, skip_updates=True)
        except Exception as e:
            print(f"Ошибка при запуске бота: {e}. Перезапускаем через 5 секунд...")
            await asyncio.sleep(5)

async def periodic_check_invoices():
    while True:
        await check_crypto_bot_invoices()
        await asyncio.sleep(60) # Проверяем инвойсы каждую минуту

async def main():
    await load_data()
    # Запускаем фоновую задачу для проверки инвойсов
    asyncio.create_task(periodic_check_invoices())
    # Запускаем основной процесс бота
    await run_bot()

if __name__ == "__main__":
    asyncio.run(main())
