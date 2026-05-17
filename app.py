import asyncio
import io
import csv
import sqlite3
import os
import re
import threading
from datetime import datetime
from flask import Flask, request, jsonify
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv

load_dotenv()

# ========== НАСТРОЙКИ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# База данных
db_path = os.path.join(os.path.dirname(__file__), "settings.db")
conn = sqlite3.connect(db_path, check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        user_id INTEGER PRIMARY KEY,
        channel_id INTEGER,
        group_id INTEGER,
        session_string TEXT,
        api_id INTEGER,
        api_hash TEXT
    )
""")
conn.commit()

# Глобальные переменные
telethon_clients = {}
tasks = {}
pending_cleanups = {}

# Состояния для авторизации
class AuthState(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_code = State()
    waiting_for_password = State()

# ========== ФУНКЦИЯ ПАРСИНГА ССЫЛОК ==========
def parse_post_url(url: str):
    import re
    # Убираем https://, http://, telegram.me, t.me
    clean_url = re.sub(r'https?://(telegram\.me|t\.me)/', '', url)
    parts = clean_url.split('/')
    
    if len(parts) != 2:
        return None, None
    
    channel_part = parts[0]
    try:
        post_id = int(parts[1])
    except ValueError:
        return None, None
    
    # Определяем тип ссылки
    if channel_part.isdigit():
        channel_id = int("-100" + channel_part)
    else:
        channel_id = channel_part
    
    return channel_id, post_id

# ========== ФУНКЦИИ РАБОТЫ С БАЗОЙ ==========
def get_settings(user_id: int):
    cursor.execute("SELECT channel_id, group_id, session_string, api_id, api_hash FROM settings WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row:
        return {
            "channel_id": row[0],
            "group_id": row[1],
            "session_string": row[2],
            "api_id": row[3],
            "api_hash": row[4]
        }
    return None

def save_settings(user_id: int, channel_id: int = None, group_id: int = None, session_string: str = None, api_id: int = None, api_hash: str = None):
    existing = get_settings(user_id)
    if existing:
        if channel_id is None:
            channel_id = existing.get("channel_id")
        if group_id is None:
            group_id = existing.get("group_id")
        if session_string is None:
            session_string = existing.get("session_string")
        if api_id is None:
            api_id = existing.get("api_id")
        if api_hash is None:
            api_hash = existing.get("api_hash")
    
    cursor.execute("INSERT OR REPLACE INTO settings (user_id, channel_id, group_id, session_string, api_id, api_hash) VALUES (?, ?, ?, ?, ?, ?)",
                   (user_id, channel_id, group_id, session_string, api_id, api_hash))
    conn.commit()

def save_api_data(user_id: int, api_id: int, api_hash: str):
    settings = get_settings(user_id)
    session_string = settings.get("session_string") if settings else None
    channel_id = settings.get("channel_id") if settings else None
    group_id = settings.get("group_id") if settings else None
    cursor.execute("INSERT OR REPLACE INTO settings (user_id, channel_id, group_id, session_string, api_id, api_hash) VALUES (?, ?, ?, ?, ?, ?)",
                   (user_id, channel_id, group_id, session_string, api_id, api_hash))
    conn.commit()

def save_session(user_id: int, session_string: str):
    settings = get_settings(user_id)
    api_id = settings.get("api_id") if settings else None
    api_hash = settings.get("api_hash") if settings else None
    channel_id = settings.get("channel_id") if settings else None
    group_id = settings.get("group_id") if settings else None
    cursor.execute("INSERT OR REPLACE INTO settings (user_id, channel_id, group_id, session_string, api_id, api_hash) VALUES (?, ?, ?, ?, ?, ?)",
                   (user_id, channel_id, group_id, session_string, api_id, api_hash))
    conn.commit()

async def get_telethon_client(user_id: int):
    if user_id in telethon_clients and telethon_clients[user_id].is_connected():
        return telethon_clients[user_id]
    
    settings = get_settings(user_id)
    if not settings or not settings.get("session_string"):
        return None
    
    client = TelegramClient(StringSession(settings["session_string"]), 0, "")
    await client.start()
    telethon_clients[user_id] = client
    return client

async def clean_channel_from_list(user_id: int, user_ids: list, progress_callback=None) -> dict:
    if not user_ids:
        return {"success": 0, "errors": 0, "total": 0}
    
    settings = get_settings(user_id)
    if not settings:
        return {"success": 0, "errors": 0, "total": 0, "error": "Настройки не найдены"}
    
    client = await get_telethon_client(user_id)
    if not client:
        return {"success": 0, "errors": 0, "total": 0, "error": "Сессия не найдена"}
    
    success = 0
    errors = 0
    
    try:
        channel_entity = await client.get_entity(settings["channel_id"])
    except Exception as e:
        return {"success": 0, "errors": 0, "total": 0, "error": f"Не удалось найти канал: {e}"}
    
    for idx, uid in enumerate(user_ids):
        try:
            await client.kick_participant(channel_entity, uid)
            success += 1
            await asyncio.sleep(0.3)
        except Exception:
            errors += 1
        
        if progress_callback and (idx + 1) % max(1, len(user_ids) // 10) == 0:
            await progress_callback(idx + 1, len(user_ids))
    
    return {"success": success, "errors": errors, "total": len(user_ids)}

# ========== КОМАНДЫ БОТА ==========
@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        "Бот для автоматической чистки канала\n\n"
        "/login - авторизация (один раз)\n"
        "/setup ID_канала ID_группы - настройка\n"
        "/check ссылка часы - запустить проверку\n"
        "/status - активные задачи\n"
        "/cancel ID_поста - отменить задачу\n"
        "/mysettings - текущие настройки"
    )

@dp.message(Command("login"))
async def cmd_login(message: types.Message, state: FSMContext):
    await message.answer(
        "АВТОРИЗАЦИЯ\n\n"
        "Шаг 1 из 2: Введите ваш API ID\n"
        "(число с сайта my.telegram.org -> API Development Tools)\n\n"
        "Пример: 1234567\n\n"
        "Для отмены: /cancel"
    )
    await state.set_state(AuthState.waiting_for_api_id)

@dp.message(AuthState.waiting_for_api_id)
async def process_api_id(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return
    
    try:
        api_id = int(message.text.strip())
        await state.update_data(api_id=api_id)
        await message.answer(
            "Шаг 2 из 2: Введите ваш API HASH\n"
            "(строка с сайта my.telegram.org)\n\n"
            "Пример: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6\n\n"
            "Для отмены: /cancel"
        )
        await state.set_state(AuthState.waiting_for_api_hash)
    except ValueError:
        await message.answer("Ошибка: API ID должен быть числом")

@dp.message(AuthState.waiting_for_api_hash)
async def process_api_hash(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return
    
    api_hash = message.text.strip()
    data = await state.get_data()
    api_id = data.get("api_id")
    
    save_api_data(message.from_user.id, api_id, api_hash)
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Поделиться номером телефона", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    await message.answer(
        "API данные сохранены!\n\n"
        "Нажмите на кнопку ниже, чтобы поделиться номером телефона.\n"
        "Telegram сам отправит ваш номер боту.\n\n"
        "Это нужно для авторизации.",
        reply_markup=keyboard
    )
    await state.clear()
    await state.update_data(waiting_for_contact=True, api_id=api_id, api_hash=api_hash)

@dp.message(lambda message: message.contact is not None)
async def handle_contact(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("waiting_for_contact"):
        pass
    
    phone_number = message.contact.phone_number
    
    await message.answer(
        f"Создаю сессию для номера {phone_number}...",
        reply_markup=types.ReplyKeyboardRemove()
    )
    
    settings = get_settings(message.from_user.id)
    if not settings or not settings.get("api_id"):
        await message.answer("Ошибка: API данные не найдены. Начните заново с /login")
        await state.clear()
        return
    
    client = TelegramClient(StringSession(), settings["api_id"], settings["api_hash"])
    await client.connect()
    
    try:
        await client.sign_in(phone_number)
        session_string = StringSession.save(client.session)
        save_session(message.from_user.id, session_string)
        await client.disconnect()
        
        await message.answer(
            "Авторизация успешна!\n\n"
            "Теперь выполните /setup для настройки канала и группы.\n"
            "Пример: /setup -1001234567890 -1009876543210\n\n"
            "Как получить ID: перешлите сообщение из канала/группы боту @userinfobot"
        )
        await state.clear()
        
    except Exception as e:
        error_text = str(e).lower()
        if "phone code" in error_text or "code" in error_text:
            await state.update_data(client=client, phone=phone_number)
            await message.answer(
                "Введите код подтверждения, который пришёл вам в Telegram.\n"
                "Пример: 12345\n\n"
                "Для отмены: /cancel"
            )
            await state.set_state(AuthState.waiting_for_code)
        elif "password" in error_text or "2fa" in error_text:
            await state.update_data(client=client, phone=phone_number)
            await message.answer(
                "Введите пароль двухфакторной аутентификации.\n\n"
                "Для отмены: /cancel"
            )
            await state.set_state(AuthState.waiting_for_password)
        else:
            await message.answer(f"Ошибка: {e}")
            await state.clear()

@dp.message(AuthState.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return
    
    code = message.text.strip()
    data = await state.get_data()
    client = data.get('client')
    phone = data.get('phone')
    
    if not client:
        await message.answer("Сессия потеряна. Начните заново с /login")
        await state.clear()
        return
    
    try:
        await client.sign_in(phone, code)
        session_string = StringSession.save(client.session)
        save_session(message.from_user.id, session_string)
        await client.disconnect()
        
        await message.answer(
            "Авторизация успешна!\n\n"
            "Теперь выполните /setup для настройки канала и группы.\n"
            "Пример: /setup -1001234567890 -1009876543210"
        )
        await state.clear()
        
    except Exception as e:
        error_text = str(e).lower()
        if "password" in error_text or "2fa" in error_text:
            await message.answer(
                "Введите пароль двухфакторной аутентификации.\n\n"
                "Для отмены: /cancel"
            )
            await state.set_state(AuthState.waiting_for_password)
            await state.update_data(client=client, phone=phone)
        else:
            await message.answer(f"Ошибка: {e}")
            await state.clear()

@dp.message(AuthState.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return
    
    password = message.text.strip()
    data = await state.get_data()
    client = data.get('client')
    
    try:
        await client.sign_in(password=password)
        session_string = StringSession.save(client.session)
        save_session(message.from_user.id, session_string)
        await client.disconnect()
        
        await message.answer(
            "Авторизация успешна!\n\n"
            "Теперь выполните /setup для настройки канала и группы.\n"
            "Пример: /setup -1001234567890 -1009876543210"
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

@dp.message(Command("setup"))
async def setup_command(message: types.Message):
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "Использование: /setup ID_канала ID_группы\n"
            "Пример: /setup -1001234567890 -1009876543210\n\n"
            "Как получить ID: перешлите сообщение из канала/группы боту @userinfobot"
        )
        return
    
    try:
        channel_id = int(args[1])
        group_id = int(args[2])
    except ValueError:
        await message.answer("ID канала и группы должны быть числами")
        return
    
    settings = get_settings(message.from_user.id)
    if not settings or not settings.get("session_string"):
        await message.answer("Сначала выполните /login")
        return
    
    save_settings(
        message.from_user.id,
        channel_id=channel_id,
        group_id=group_id,
        session_string=settings.get("session_string"),
        api_id=settings.get("api_id"),
        api_hash=settings.get("api_hash")
    )
    await message.answer(
        f"Настройки сохранены!\n\n"
        f"Канал: {channel_id}\n"
        f"Группа: {group_id}\n\n"
        f"Теперь используйте /check"
    )

@dp.message(Command("mysettings"))
async def mysettings(message: types.Message):
    settings = get_settings(message.from_user.id)
    
    text = "Ваши настройки:\n\n"
    if settings:
        text += f"Канал: {settings['channel_id'] if settings['channel_id'] else 'не настроен'}\n"
        text += f"Группа: {settings['group_id'] if settings['group_id'] else 'не настроена'}\n"
        text += f"API ID: {settings['api_id'] if settings['api_id'] else 'не указан'}\n"
        text += f"Авторизация: {'выполнена' if settings['session_string'] else 'не выполнена'}\n"
    else:
        text += "Настройки не найдены. Выполните /login и /setup"
    
    await message.answer(text)

@dp.message(Command("check"))
async def check_command(message: types.Message):
    settings = get_settings(message.from_user.id)
    if not settings or not settings.get("channel_id"):
        await message.answer("Сначала выполните /setup")
        return
    
    if not settings.get("session_string"):
        await message.answer("Сначала выполните /login")
        return
    
    args = message.text.split()
    if len(args) < 3:
        await message.answer(
            "Использование: /check ссылка_на_пост часы\n\n"
            "Примеры:\n"
            "/check https://t.me/c/1234567890/456 24\n"
            "/check https://t.me/mychannel/2 0.25\n"
            "/check t.me/mychannel/2 1.5\n\n"
            "Часы могут быть целыми или дробными (0.25 = 15 минут)"
        )
        return
    
    post_url = args[1]
    try:
        hours = float(args[2])
    except ValueError:
        await message.answer("Часы должны быть числом (целым или дробным, например 0.25)")
        return
    
    channel_id_or_username, post_id = parse_post_url(post_url)
    
    if channel_id_or_username is None or post_id is None:
        await message.answer(
            "Неверный формат ссылки.\n\n"
            "Поддерживаются:\n"
            "- https://t.me/c/1234567890/456\n"
            "- https://t.me/username/2\n"
            "- t.me/username/2"
        )
        return
    
    # Проверка канала
    if channel_id_or_username != settings["channel_id"]:
        await message.answer(
            f"Это не ваш канал.\n\n"
            f"Ваш канал: {settings['channel_id']}\n"
            f"Убедитесь, что ссылка ведёт на правильный канал."
        )
        return
    
    deadline = asyncio.get_event_loop().time() + hours * 3600
    
    tasks[post_id] = {
        "deadline": deadline,
        "chat_id": message.chat.id,
        "post_link": post_url,
        "hours": hours,
        "user_id": message.from_user.id
    }
    
    # Форматируем время для красивого вывода
    if hours < 1:
        minutes = int(hours * 60)
        time_str = f"{minutes} минут"
    elif hours == 1:
        time_str = "1 час"
    elif hours == int(hours):
        time_str = f"{int(hours)} часов"
    else:
        time_str = f"{hours} часов"
    
    await message.answer(
        f"Задача создана\n\n"
        f"Пост: {post_url}\n"
        f"Жду {time_str}\n"
        f"Готово: {datetime.fromtimestamp(deadline).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    asyncio.create_task(process_post(post_id, deadline, message.chat.id, post_url, message.from_user.id))

@dp.message(Command("status"))
async def status(message: types.Message):
    user_tasks = {pid: data for pid, data in tasks.items() if data.get("user_id") == message.from_user.id}
    
    if not user_tasks:
        await message.answer("Нет активных задач")
        return
    
    text = "Ваши активные задачи:\n\n"
    for pid, data in user_tasks.items():
        remaining = int(data["deadline"] - asyncio.get_event_loop().time())
        hours_left = remaining // 3600
        mins_left = (remaining % 3600) // 60
        text += f"Пост {pid}: {hours_left}ч {mins_left}м осталось\n"
    await message.answer(text)

@dp.message(Command("cancel"))
async def cancel(message: types.Message):
    args = message.text.split()
    if len(args) != 2:
        await message.answer("Использование: /cancel ID_поста")
        return
    
    try:
        post_id = int(args[1])
    except ValueError:
        await message.answer("ID поста должен быть числом")
        return
    
    if post_id in tasks and tasks[post_id].get("user_id") == message.from_user.id:
        del tasks[post_id]
        await message.answer(f"Задача для поста {post_id} отменена")
    else:
        await message.answer(f"Задача для поста {post_id} не найдена")

# ========== КОЛБЭКИ ==========
@dp.callback_query(lambda c: c.data.startswith("edit_"))
async def handle_edit(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[1]
    
    if temp_id not in pending_cleanups:
        await callback.answer("Данные устарели")
        return
    
    pending_cleanups[temp_id]["editing"] = True
    
    await callback.message.edit_text(
        f"РЕЖИМ РЕДАКТИРОВАНИЯ\n\n"
        f"Всего в списке: {pending_cleanups[temp_id]['total_count']} человек\n\n"
        f"Отправьте ID пользователей, которых нужно исключить\n"
        f"Пример: 123456789, 987654321\n\n"
        f"Для отмены: /cancel_edit"
    )

@dp.message(lambda message: message.text and not message.text.startswith("/"))
async def process_exclude_list(message: types.Message):
    active_temp_id = None
    active_data = None
    
    for temp_id, data in pending_cleanups.items():
        if data.get("editing") and data.get("user_id") == message.from_user.id:
            active_temp_id = temp_id
            active_data = data
            break
    
    if not active_data:
        return
    
    ids_raw = re.split(r'[,\s\n]+', message.text.strip())
    exclude_ids = set()
    for item in ids_raw:
        if item.strip().isdigit():
            exclude_ids.add(int(item.strip()))
    
    if not exclude_ids:
        await message.answer("Не найдено корректных ID")
        return
    
    original_ids = set(active_data["user_ids_list"])
    new_ids = list(original_ids - exclude_ids)
    
    active_data["user_ids_list"] = new_ids
    active_data["total_count"] = len(new_ids)
    active_data["editing"] = False
    
    user_lines = []
    for uid in new_ids[:30]:
        try:
            user = await bot.get_chat(uid)
            username = f" (@{user.username})" if user.username else ""
            user_lines.append(f"{uid}{username}")
        except:
            user_lines.append(f"{uid}")
    
    list_text = "\n".join(user_lines)
    if len(new_ids) > 30:
        list_text += f"\n\n... и ещё {len(new_ids) - 30} человек"
    
    confirm_text = (
        f"ПОДТВЕРЖДЕНИЕ\n\n"
        f"Пост: {active_data['post_link']}\n"
        f"Не отметилось: {len(new_ids)}"
    )
    if len(new_ids) != len(original_ids):
        confirm_text += f"\n(Исключено {len(original_ids) - len(new_ids)})"
    
    confirm_text += f"\n\nСписок:\n{list_text}\n\nУдалить этих пользователей из канала?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="ДА", callback_data=f"confirm_yes_{active_temp_id}"),
            InlineKeyboardButton(text="ИЗМЕНИТЬ ЕЩЁ", callback_data=f"edit_{active_temp_id}"),
            InlineKeyboardButton(text="НЕТ", callback_data=f"confirm_no_{active_temp_id}")
        ]
    ])
    
    await bot.send_message(message.chat.id, confirm_text, reply_markup=keyboard)
    await message.delete()

@dp.message(Command("cancel_edit"))
async def cancel_edit(message: types.Message):
    for temp_id, data in pending_cleanups.items():
        if data.get("editing") and data.get("user_id") == message.from_user.id:
            data["editing"] = False
            await message.answer("Редактирование отменено")
            return
    
    await message.answer("Нет активного редактирования")

@dp.callback_query(lambda c: c.data.startswith("confirm_yes_"))
async def handle_confirm_yes(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[2]
    
    if temp_id not in pending_cleanups:
        await callback.answer("Данные устарели")
        return
    
    data = pending_cleanups[temp_id]
    
    if data["total_count"] == 0:
        await callback.message.edit_text("Список пуст — некого удалять")
        del pending_cleanups[temp_id]
        return
    
    await callback.message.edit_text("Удаляю пользователей из канала...")
    
    async def update_progress(current, total):
        await callback.message.edit_text(f"Удаляю... {current}/{total} ({current*100//total}%)")
    
    result = await clean_channel_from_list(data["user_id"], data["user_ids_list"], update_progress)
    
    report = (
        f"ГОТОВО!\n\n"
        f"Пост: {data['post_link']}\n"
        f"Не отметилось: {data['total_count']}\n"
        f"Удалено из канала: {result['success']}\n"
        f"Ошибок: {result['errors']}"
    )
    
    await callback.message.edit_text(report)
    del pending_cleanups[temp_id]
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_no_"))
async def handle_confirm_no(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[2]
    
    if temp_id in pending_cleanups:
        await callback.message.edit_text("Удаление отменено")
        del pending_cleanups[temp_id]

# ========== ОСНОВНАЯ ЛОГИКА ==========
async def process_post(post_id: int, deadline: float, reply_chat_id: int, post_link: str, user_id: int):
    settings = get_settings(user_id)
    if not settings:
        await bot.send_message(reply_chat_id, "Настройки не найдены")
        return
    
    wait_seconds = deadline - asyncio.get_event_loop().time()
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)
    
    if post_id in tasks:
        del tasks[post_id]
    
    commenters = set()
    try:
        async for msg in bot.get_chat_history(settings["channel_id"], limit=1000):
            if msg.reply_to_message and msg.reply_to_message.message_id == post_id:
                commenters.add(msg.from_user.id)
    except Exception as e:
        await bot.send_message(reply_chat_id, f"Ошибка сбора комментаторов: {e}")
        return
    
    members = set()
    try:
        async for member in bot.get_chat_members(settings["group_id"]):
            if not member.user.is_bot:
                members.add(member.user.id)
    except Exception as e:
        await bot.send_message(reply_chat_id, f"Ошибка сбора участников группы: {e}")
        return
    
    to_kick = list(members - commenters)
    
    if not to_kick:
        await bot.send_message(reply_chat_id, f"Пост {post_link}\nВсе отметились!")
        return
    
    kicked_group = 0
    for uid in to_kick:
        try:
            await bot.ban_chat_member(settings["group_id"], uid)
            await asyncio.sleep(0.2)
            await bot.unban_chat_member(settings["group_id"], uid)
            kicked_group += 1
        except Exception:
            pass
    
    await bot.send_message(settings["group_id"], f"Кикнуто из группы: {kicked_group}")
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id"])
    for uid in to_kick:
        writer.writerow([uid])
    csv_bytes = output.getvalue().encode("utf-8")
    
    user_lines = []
    for uid in to_kick[:30]:
        try:
            user = await bot.get_chat(uid)
            username = f" (@{user.username})" if user.username else ""
            user_lines.append(f"{uid}{username}")
        except:
            user_lines.append(f"{uid}")
    
    list_text = "\n".join(user_lines)
    if len(to_kick) > 30:
        list_text += f"\n\n... и ещё {len(to_kick) - 30} человек"
    
    temp_id = f"{post_id}_{int(asyncio.get_event_loop().time())}"
    pending_cleanups[temp_id] = {
        "csv_bytes": csv_bytes,
        "post_id": post_id,
        "post_link": post_link,
        "total_count": len(to_kick),
        "user_ids_list": to_kick,
        "user_id": user_id
    }
    
    confirm_text = (
        f"ПОДТВЕРЖДЕНИЕ\n\n"
        f"Пост: {post_link}\n"
        f"Не отметилось: {len(to_kick)}\n\n"
        f"Список:\n{list_text}\n\n"
        f"Удалить этих пользователей из канала?"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="ДА", callback_data=f"confirm_yes_{temp_id}"),
            InlineKeyboardButton(text="ИЗМЕНИТЬ СПИСОК", callback_data=f"edit_{temp_id}"),
            InlineKeyboardButton(text="НЕТ", callback_data=f"confirm_no_{temp_id}")
        ]
    ])
    
    await bot.send_document(reply_chat_id, types.BufferedInputFile(csv_bytes, filename=f"to_kick_{post_id}.csv"))
    await bot.send_message(reply_chat_id, confirm_text, reply_markup=keyboard)

# ========== FLASK ==========
@app.route('/')
@app.route('/health')
def health():
    return jsonify({"status": "ok", "bot": "running"})

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)

async def main():
    print("Бот запущен и готов к работе")
    await dp.start_polling(bot)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    asyncio.run(main())
