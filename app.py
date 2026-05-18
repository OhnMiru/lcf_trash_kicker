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
import aiohttp
import requests

# ========== НАСТРОЙКИ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
RENDER_URL = "https://lcf-trash-kicker.onrender.com"

# Принудительно удаляем вебхук при запуске
try:
    requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=True")
    print("Вебхук удалён при запуске")
except Exception as e:
    print(f"Ошибка удаления вебхука: {e}")

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

cursor.execute("""
    CREATE TABLE IF NOT EXISTS active_tasks (
        post_id INTEGER PRIMARY KEY,
        deadline REAL,
        post_link TEXT,
        hours REAL,
        user_id INTEGER
    )
""")
conn.commit()

# Глобальные переменные
telethon_clients = {}
tasks = {}
pending_cleanups = {}

# Состояния для авторизации и настройки
class AuthState(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_channel_link = State()
    waiting_for_group_link = State()

# ========== ФУНКЦИЯ ПАРСИНГА ССЫЛОК ==========
def parse_post_url(url: str):
    import re
    clean_url = re.sub(r'https?://(telegram\.me|t\.me)/', '', url)
    parts = clean_url.split('/')
    
    if len(parts) != 2:
        return None, None
    
    channel_part = parts[0]
    try:
        post_id = int(parts[1])
    except ValueError:
        return None, None
    
    if channel_part.isdigit():
        channel_id = int("-100" + channel_part)
    else:
        channel_id = channel_part
    
    return channel_id, post_id

def extract_username_from_link(link: str):
    import re
    match = re.search(r'(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/([a-zA-Z0-9_]+)', link)
    if match:
        return match.group(1)
    return None

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
    if not session_string:
        print(f"ERROR: Попытка сохранить пустую сессию для user_id={user_id}")
        return False
    
    settings = get_settings(user_id)
    api_id = settings.get("api_id") if settings else None
    api_hash = settings.get("api_hash") if settings else None
    channel_id = settings.get("channel_id") if settings else None
    group_id = settings.get("group_id") if settings else None
    
    cursor.execute("INSERT OR REPLACE INTO settings (user_id, channel_id, group_id, session_string, api_id, api_hash) VALUES (?, ?, ?, ?, ?, ?)",
                   (user_id, channel_id, group_id, session_string, api_id, api_hash))
    conn.commit()
    print(f"DEBUG: Сессия сохранена для user_id={user_id}, длина={len(session_string)}")
    return True

def restore_tasks_from_db():
    cursor.execute("SELECT post_id, deadline, post_link, hours, user_id FROM active_tasks")
    rows = cursor.fetchall()
    
    restored_count = 0
    current_time = asyncio.get_event_loop().time()
    
    for row in rows:
        post_id, deadline_timestamp, post_link, hours, user_id = row
        if deadline_timestamp > current_time:
            tasks[post_id] = {
                "deadline": deadline_timestamp,
                "post_link": post_link,
                "hours": hours,
                "user_id": user_id,
                "restored": True
            }
            restored_count += 1
            asyncio.create_task(process_post(post_id, deadline_timestamp, None, post_link, user_id))
            print(f"Восстановлена задача для поста {post_id}")
        else:
            cursor.execute("DELETE FROM active_tasks WHERE post_id = ?", (post_id,))
    
    conn.commit()
    print(f"Восстановлено задач: {restored_count}")

async def get_telethon_client(user_id: int):
    print(f"DEBUG: Пытаюсь получить клиент для user_id={user_id}")
    
    settings = get_settings(user_id)
    if not settings:
        print(f"DEBUG: Настройки для user_id={user_id} не найдены")
        return None
    
    session_string = settings.get("session_string")
    if not session_string:
        print(f"DEBUG: session_string для user_id={user_id} отсутствует в БД")
        return None
    
    print(f"DEBUG: session_string для user_id={user_id} найдена (длина: {len(session_string)})")
    
    api_id = settings.get("api_id")
    api_hash = settings.get("api_hash")
    
    if not api_id or not api_hash:
        print(f"DEBUG: API данные для user_id={user_id} отсутствуют")
        return None
    
    print(f"DEBUG: API данные для user_id={user_id} найдены: api_id={api_id}")
    
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.start()
        
        me = await client.get_me()
        print(f"DEBUG: Клиент для user_id={user_id} успешно запущен. Пользователь: {me.first_name}")
        telethon_clients[user_id] = client
        return client
        
    except Exception as e:
        print(f"Ошибка при запуске клиента для user_id={user_id}: {type(e).__name__}: {e}")
        return None

async def clean_channel_from_list(user_id: int, user_ids: list, progress_callback=None) -> dict:
    if not user_ids:
        return {"success": 0, "errors": 0, "total": 0}
    
    settings = get_settings(user_id)
    if not settings:
        return {"success": 0, "errors": 0, "total": 0, "error": "Настройки не найдены"}
    
    client = await get_telethon_client(user_id)
    if not client:
        return {"success": 0, "errors": 0, "total": 0, "error": "Сессия не найдена. Выполните /login заново"}
    
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
        "/setup - настройка канала и группы\n"
        "/check ссылка часы - запустить проверку\n"
        "/status - активные задачи\n"
        "/cancel ID_поста - отменить задачу\n"
        "/mysettings - текущие настройки\n"
        "/check_session - диагностика сессии\n"
        "/test_session - тест работоспособности сессии\n"
        "/reset_session - сброс сессии (если не работает)"
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
        "API данные сохранены\n\n"
        "Нажмите на кнопку ниже, чтобы поделиться номером телефона.\n"
        "Telegram сам отправит ваш номер боту.\n\n"
        "Это нужно для авторизации.",
        reply_markup=keyboard
    )
    await state.clear()
    await state.update_data(waiting_for_contact=True, api_id=api_id, api_hash=api_hash)

@dp.message(Command("check_session"))
async def check_session(message: types.Message):
    """Диагностическая команда для проверки сессии"""
    settings = get_settings(message.from_user.id)
    if not settings:
        await message.answer("Настройки не найдены. Сначала выполните /login")
        return
    
    has_session = settings.get("session_string") is not None and len(settings.get("session_string", "")) > 0
    session_len = len(settings.get("session_string") or "")
    
    await message.answer(
        f"Диагностика сессии:\n\n"
        f"Сессия в БД: {'есть' if has_session else 'нет'}\n"
        f"Длина сессии: {session_len} символов\n"
        f"API ID: {settings.get('api_id') or 'нет'}\n"
        f"Канал: {settings.get('channel_id') or 'не настроен'}\n"
        f"Группа: {settings.get('group_id') or 'не настроена'}\n\n"
        f"Если сессии нет, выполните /login заново"
    )

@dp.message(Command("test_session"))
async def test_session(message: types.Message):
    """Прямой тест загруженной сессии"""
    settings = get_settings(message.from_user.id)
    if not settings:
        await message.answer("Настройки не найдены. Сначала выполните /login")
        return
    
    session_string = settings.get("session_string")
    api_id = settings.get("api_id")
    api_hash = settings.get("api_hash")
    
    if not session_string:
        await message.answer("Сессия отсутствует в БД. Выполните /login")
        return
    
    await message.answer(f"Тестирую сессию (длина: {len(session_string)})\nAPI ID: {api_id}")
    
    try:
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        await client.start()
        
        me = await client.get_me()
        await message.answer(f"✅ Сессия работает!\nПользователь: {me.first_name} (ID: {me.id})")
        
        await client.disconnect()
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {type(e).__name__}: {e}\n\nВыполните /reset_session, затем /login заново")

@dp.message(Command("reset_session"))
async def reset_session(message: types.Message):
    """Сбрасывает сессию"""
    cursor.execute("UPDATE settings SET session_string = NULL WHERE user_id = ?", (message.from_user.id,))
    conn.commit()
    await message.answer("Сессия сброшена. Теперь выполните /login заново")

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
        
        if not session_string or len(session_string) < 10:
            await message.answer("Ошибка: получена пустая сессия. Попробуйте ещё раз.")
            await client.disconnect()
            return
        
        save_session(message.from_user.id, session_string)
        
        check_settings = get_settings(message.from_user.id)
        if not check_settings or not check_settings.get("session_string"):
            await message.answer("Ошибка: не удалось сохранить сессию в базу данных. Попробуйте ещё раз.")
            await client.disconnect()
            return
        
        await client.disconnect()
        
        await message.answer(
            f"Авторизация успешна!\n\n"
            f"Сессия сохранена (длина: {len(session_string)} символов)\n\n"
            f"Теперь выполните /setup для настройки канала и группы.\n\n"
            f"/setup - настроить канал и группу по ссылке\n"
            f"/setup ID_канала ID_группы - ввести ID вручную"
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
        
        if not session_string or len(session_string) < 10:
            await message.answer("Ошибка: получена пустая сессия. Попробуйте ещё раз.")
            await client.disconnect()
            return
        
        save_session(message.from_user.id, session_string)
        await client.disconnect()
        
        await message.answer(
            f"Авторизация успешна!\n\n"
            f"Сессия сохранена (длина: {len(session_string)} символов)\n\n"
            f"Теперь выполните /setup для настройки канала и группы.\n\n"
            f"/setup - настроить канал и группу по ссылке\n"
            f"/setup ID_канала ID_группы - ввести ID вручную"
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
        
        if not session_string or len(session_string) < 10:
            await message.answer("Ошибка: получена пустая сессия. Попробуйте ещё раз.")
            await client.disconnect()
            return
        
        save_session(message.from_user.id, session_string)
        await client.disconnect()
        
        await message.answer(
            f"Авторизация успешна!\n\n"
            f"Сессия сохранена (длина: {len(session_string)} символов)\n\n"
            f"Теперь выполните /setup для настройки канала и группы.\n\n"
            f"/setup - настроить канал и группу по ссылке\n"
            f"/setup ID_канала ID_группы - ввести ID вручную"
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

@dp.message(Command("setup"))
async def setup_command(message: types.Message, state: FSMContext):
    settings = get_settings(message.from_user.id)
    if not settings or not settings.get("session_string"):
        await message.answer("Сначала выполните /login")
        return
    
    args = message.text.split()
    
    if len(args) == 3:
        try:
            channel_id = int(args[1])
            group_id = int(args[2])
        except ValueError:
            await message.answer("ID канала и группы должны быть числами")
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
            f"Настройки сохранены\n\n"
            f"Канал: {channel_id}\n"
            f"Группа: {group_id}\n\n"
            f"Теперь используйте /check"
        )
        return
    
    await message.answer(
        "НАСТРОЙКА КАНАЛА И ГРУППЫ\n\n"
        "Сначала отправьте ссылку на канал, где вы и бот являетесь администраторами.\n"
        "Пример: https://t.me/username\n\n"
        "После этого отправьте ссылку на группу обсуждения.\n\n"
        "Для отмены: /cancel"
    )
    await state.set_state(AuthState.waiting_for_channel_link)

@dp.message(AuthState.waiting_for_channel_link)
async def process_channel_link(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return
    
    link = message.text.strip()
    username = extract_username_from_link(link)
    
    if not username:
        await message.answer("Неверный формат ссылки. Пример: https://t.me/username")
        return
    
    try:
        chat = await bot.get_chat(f"@{username}")
        
        try:
            bot_member = await bot.get_chat_member(chat.id, bot.id)
            if bot_member.status not in ['administrator', 'creator']:
                await message.answer(
                    f"Бот не является администратором канала {chat.title}.\n"
                    f"Добавьте бота в администраторы и попробуйте снова."
                )
                return
        except Exception:
            await message.answer(
                f"Не удалось проверить права бота.\n"
                f"Убедитесь, что бот добавлен в администраторы канала {chat.title}."
            )
            return
        
        try:
            user_member = await bot.get_chat_member(chat.id, message.from_user.id)
            if user_member.status not in ['administrator', 'creator']:
                await message.answer(
                    f"Вы не являетесь администратором канала {chat.title}.\n"
                    f"Пожалуйста, выберите канал, где вы администратор."
                )
                return
        except Exception:
            await message.answer(
                f"Не удалось проверить ваши права.\n"
                f"Убедитесь, что вы являетесь администратором канала {chat.title}."
            )
            return
        
        settings = get_settings(message.from_user.id)
        save_settings(
            message.from_user.id,
            channel_id=chat.id,
            group_id=settings.get("group_id") if settings else None,
            session_string=settings.get("session_string") if settings else None,
            api_id=settings.get("api_id") if settings else None,
            api_hash=settings.get("api_hash") if settings else None
        )
        
        await message.answer(
            f"Канал выбран: {chat.title}\n"
            f"ID: {chat.id}\n\n"
            f"Теперь отправьте ссылку на группу обсуждения.\n"
            f"Бот и вы должны быть администраторами группы.\n\n"
            f"Для отмены: /cancel"
        )
        await state.set_state(AuthState.waiting_for_group_link)
        
    except Exception as e:
        await message.answer(f"Ошибка: {e}\nУбедитесь, что ссылка верна.")

@dp.message(AuthState.waiting_for_group_link)
async def process_group_link(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return
    
    link = message.text.strip()
    username = extract_username_from_link(link)
    
    if not username:
        await message.answer("Неверный формат ссылки. Пример: https://t.me/username")
        return
    
    try:
        chat = await bot.get_chat(f"@{username}")
        
        if chat.type not in ['group', 'supergroup']:
            await message.answer("Это не группа. Пожалуйста, отправьте ссылку на группу обсуждения.")
            return
        
        try:
            bot_member = await bot.get_chat_member(chat.id, bot.id)
            if bot_member.status not in ['administrator', 'creator']:
                await message.answer(
                    f"Бот не является администратором группы {chat.title}.\n"
                    f"Добавьте бота в администраторы и попробуйте снова."
                )
                return
        except Exception:
            await message.answer(
                f"Не удалось проверить права бота.\n"
                f"Убедитесь, что бот добавлен в администраторы группы {chat.title}."
            )
            return
        
        try:
            user_member = await bot.get_chat_member(chat.id, message.from_user.id)
            if user_member.status not in ['administrator', 'creator']:
                await message.answer(
                    f"Вы не являетесь администратором группы {chat.title}.\n"
                    f"Пожалуйста, выберите группу, где вы администратор."
                )
                return
        except Exception:
            await message.answer(
                f"Не удалось проверить ваши права.\n"
                f"Убедитесь, что вы являетесь администратором группы {chat.title}."
            )
            return
        
        settings = get_settings(message.from_user.id)
        save_settings(
            message.from_user.id,
            channel_id=settings.get("channel_id") if settings else None,
            group_id=chat.id,
            session_string=settings.get("session_string") if settings else None,
            api_id=settings.get("api_id") if settings else None,
            api_hash=settings.get("api_hash") if settings else None
        )
        
        if settings and settings.get("channel_id"):
            await message.answer(
                f"Настройки завершены\n\n"
                f"Канал: {settings['channel_id']}\n"
                f"Группа: {chat.id}\n\n"
                f"Теперь используйте /check"
            )
        else:
            await message.answer(
                f"Группа выбрана: {chat.title}\n"
                f"ID: {chat.id}\n\n"
                f"Сначала выберите канал. Повторите /setup"
            )
        
        await state.clear()
        
    except Exception as e:
        await message.answer(f"Ошибка: {e}\nУбедитесь, что ссылка верна.")

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
    
    saved_channel_id = settings["channel_id"]
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
    
    actual_channel_id = channel_id_or_username
    if isinstance(actual_channel_id, str):
        try:
            chat = await bot.get_chat(f"@{actual_channel_id}")
            actual_channel_id = chat.id
        except Exception as e:
            await message.answer(f"Не удалось получить информацию о канале {actual_channel_id}. Убедитесь, что ссылка верна.")
            return
    
    if actual_channel_id != saved_channel_id:
        channel_name = str(saved_channel_id)
        try:
            chat = await bot.get_chat(saved_channel_id)
            channel_name = chat.title
        except:
            pass
        
        await message.answer(
            f"Канал в ссылке не совпадает с настроенным каналом.\n\n"
            f"Настроенный канал: {channel_name} (ID: {saved_channel_id})\n"
            f"Канал из ссылки: ID: {actual_channel_id}\n\n"
            f"Если вы используете другой канал, выполните /setup для его настройки.\n"
            f"Если это тот же канал, проблема в несовпадении ID.\n\n"
            f"Попробуйте:\n"
            f"1. Узнать правильный ID канала через @userinfobot\n"
            f"2. Выполнить /setup {actual_channel_id} ID_группы\n"
            f"3. Или используйте /setup (без аргументов) для настройки по ссылке"
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
    
    cursor.execute("""
        INSERT OR REPLACE INTO active_tasks (post_id, deadline, post_link, hours, user_id)
        VALUES (?, ?, ?, ?, ?)
    """, (post_id, deadline, post_url, hours, message.from_user.id))
    conn.commit()
    
    print(f"DEBUG: Задача создана для поста {post_id}, deadline = {deadline}")
    
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
        cursor.execute("DELETE FROM active_tasks WHERE post_id = ?", (post_id,))
        conn.commit()
        await message.answer(f"Задача для поста {post_id} отменена")
    else:
        await message.answer(f"Задача для поста {post_id} не найдена")

# ========== КОЛБЭКИ ДЛЯ РЕДАКТИРОВАНИЯ ==========
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
        f"ГОТОВО\n\n"
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

# ========== ОСНОВНАЯ ЛОГИКА ПРОВЕРКИ ==========
async def process_post(post_id: int, deadline: float, reply_chat_id: int, post_link: str, user_id: int):
    print(f"DEBUG: process_post запущена для поста {post_id}")
    print(f"DEBUG: deadline = {deadline}, текущее время = {asyncio.get_event_loop().time()}")
    
    cursor.execute("DELETE FROM active_tasks WHERE post_id = ?", (post_id,))
    conn.commit()
    
    settings = get_settings(user_id)
    if not settings:
        await bot.send_message(reply_chat_id, "Настройки не найдены")
        return
    
    wait_seconds = deadline - asyncio.get_event_loop().time()
    if wait_seconds > 0:
        print(f"DEBUG: Ожидание {wait_seconds} секунд до дедлайна")
        await asyncio.sleep(wait_seconds)
    
    if post_id in tasks:
        del tasks[post_id]
    
    print(f"DEBUG: Начинаю сбор комментаторов для поста {post_id}")
    
    client = await get_telethon_client(user_id)
    if not client:
        await bot.send_message(reply_chat_id, "Ошибка: сессия Telethon не найдена. Выполните /login заново")
        return
    
    commenters = set()
    try:
        channel_entity = await client.get_entity(settings["channel_id"])
        
        message = await client.get_messages(channel_entity, ids=post_id)
        if not message:
            await bot.send_message(reply_chat_id, f"Пост с ID {post_id} не найден в канале")
            return
        
        async for reply in client.iter_messages(channel_entity, reply_to=post_id):
            if reply.from_id:
                if hasattr(reply.from_id, 'user_id'):
                    user_id_telethon = reply.from_id.user_id
                else:
                    user_id_telethon = reply.from_id
                commenters.add(user_id_telethon)
        
        print(f"DEBUG: Найдено комментаторов: {len(commenters)}")
                
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
    
    print(f"DEBUG: Участников группы: {len(members)}")
    
    to_kick = list(members - commenters)
    
    if not to_kick:
        await bot.send_message(reply_chat_id, f"Пост {post_link}\nВсе отметились")
        return
    
    print(f"DEBUG: Нужно кикнуть {len(to_kick)} человек")
    
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

async def self_ping():
    while True:
        await asyncio.sleep(60)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{RENDER_URL}/health") as resp:
                    if resp.status == 200:
                        print("Self-ping успешен")
                    else:
                        print(f"Self-ping вернул статус: {resp.status}")
        except Exception as e:
            print(f"Self-ping ошибка: {e}")

async def main():
    print("Бот запущен и готов к работе")
    restore_tasks_from_db()
    asyncio.create_task(self_ping())
    await dp.start_polling(bot)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    asyncio.run(main())
