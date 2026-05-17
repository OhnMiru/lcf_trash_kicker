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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from pyrogram import Client
from dotenv import load_dotenv

load_dotenv()

# ========== НАСТРОЙКИ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")

app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# База данных
conn = sqlite3.connect("settings.db", check_same_thread=False)
cursor = conn.cursor()

# Таблица настроек пользователей (канал и группа)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        user_id INTEGER PRIMARY KEY,
        channel_id INTEGER,
        group_id INTEGER
    )
""")

# Таблица API-данных пользователей
cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_api (
        user_id INTEGER PRIMARY KEY,
        api_id INTEGER,
        api_hash TEXT
    )
""")

# Таблица сессий Pyrogram
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        user_id INTEGER PRIMARY KEY,
        session_string TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

# Глобальные переменные
pyro_clients = {}  # user_id -> pyro_client
tasks = {}
pending_cleanups = {}

# Состояния для авторизации
class AuthState(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

# ========== ФУНКЦИИ РАБОТЫ С БАЗОЙ ==========
def get_settings(user_id: int):
    cursor.execute("SELECT channel_id, group_id FROM settings WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row:
        return {"channel_id": row[0], "group_id": row[1]}
    return None

def save_settings(user_id: int, channel_id: int, group_id: int):
    cursor.execute("INSERT OR REPLACE INTO settings (user_id, channel_id, group_id) VALUES (?, ?, ?)",
                   (user_id, channel_id, group_id))
    conn.commit()

def get_user_api(user_id: int):
    cursor.execute("SELECT api_id, api_hash FROM user_api WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row:
        return {"api_id": row[0], "api_hash": row[1]}
    return None

def save_user_api(user_id: int, api_id: int, api_hash: str):
    cursor.execute("INSERT OR REPLACE INTO user_api (user_id, api_id, api_hash) VALUES (?, ?, ?)",
                   (user_id, api_id, api_hash))
    conn.commit()

def get_session_string(user_id: int):
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    return row[0] if row else None

def save_session_string(user_id: int, session_string: str):
    cursor.execute("INSERT OR REPLACE INTO sessions (user_id, session_string) VALUES (?, ?)",
                   (user_id, session_string))
    conn.commit()

# ========== PYROGRAM КЛИЕНТ ==========
async def get_pyrogram_client(user_id: int):
    """Получает или создаёт Pyrogram клиент для пользователя"""
    if user_id in pyro_clients and pyro_clients[user_id].is_connected:
        return pyro_clients[user_id]
    
    api_data = get_user_api(user_id)
    if not api_data:
        return None
    
    session_string = get_session_string(user_id)
    
    if session_string:
        client = Client(
            f"user_{user_id}",
            api_id=api_data["api_id"],
            api_hash=api_data["api_hash"],
            session_string=session_string
        )
    else:
        client = Client(
            f"user_{user_id}",
            api_id=api_data["api_id"],
            api_hash=api_data["api_hash"],
            in_memory=True
        )
    
    await client.start()
    pyro_clients[user_id] = client
    return client

async def clean_channel_from_list(user_id: int, user_ids: list, progress_callback=None) -> dict:
    """Удаляет пользователей из канала по списку ID"""
    if not user_ids:
        return {"success": 0, "errors": 0, "total": 0}
    
    settings = get_settings(user_id)
    if not settings:
        return {"success": 0, "errors": 0, "total": 0, "error": "Настройки не найдены"}
    
    client = await get_pyrogram_client(user_id)
    if not client:
        return {"success": 0, "errors": 0, "total": 0, "error": "API данные не найдены. Выполните /login"}
    
    try:
        channel_username = str(settings["channel_id"]).replace("-100", "")
        channel = await client.get_chat(channel_username)
    except Exception as e:
        return {"success": 0, "errors": 0, "total": 0, "error": f"Не удалось найти канал: {e}"}
    
    success = 0
    errors = 0
    
    for idx, uid in enumerate(user_ids):
        try:
            await client.ban_chat_member(channel.id, uid)
            await asyncio.sleep(0.3)
            success += 1
        except Exception:
            errors += 1
        
        if progress_callback and (idx + 1) % max(1, len(user_ids) // 10) == 0:
            await progress_callback(idx + 1, len(user_ids))
    
    return {"success": success, "errors": errors, "total": len(user_ids)}

# ========== КОМАНДЫ БОТА ==========
@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        "**Бот для автоматической чистки канала**\n\n"
        "**Команды:**\n"
        "/login - авторизовать аккаунт Telegram (один раз)\n"
        "/setup [ID_канала] [ID_группы] - настроить канал и группу\n"
        "/check [ссылка_на_пост] [часы] - запустить проверку\n"
        "/status - показать активные проверки\n"
        "/cancel [id_поста] - отменить проверку\n"
        "/mysettings - показать текущие настройки\n\n"
        "**Порядок действий:**\n"
        "1. /login — ввести API ID, API HASH, номер телефона, код\n"
        "2. /setup — ввести ID канала и группы\n"
        "3. /check — запустить проверку поста",
        parse_mode="Markdown"
    )

@dp.message(Command("login"))
async def cmd_login(message: types.Message, state: FSMContext):
    await message.answer(
        "**Авторизация аккаунта Telegram**\n\n"
        "**Шаг 1 из 5:** Введите ваш API ID\n"
        "(число с сайта my.telegram.org → API Development Tools)\n\n"
        "Пример: `1234567`\n\n"
        "Для отмены: /cancel",
        parse_mode="Markdown"
    )
    await state.set_state(AuthState.waiting_for_api_id)

@dp.message(AuthState.waiting_for_api_id)
async def process_api_id(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Авторизация отменена")
        return
    
    try:
        api_id = int(message.text.strip())
        await state.update_data(api_id=api_id)
        await message.answer(
            "**Шаг 2 из 5:** Введите ваш API HASH\n"
            "(строка с сайта my.telegram.org)\n\n"
            "Пример: `a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6`\n\n"
            "Для отмены: /cancel",
            parse_mode="Markdown"
        )
        await state.set_state(AuthState.waiting_for_api_hash)
    except ValueError:
        await message.answer("API ID должен быть числом. Попробуйте снова.")

@dp.message(AuthState.waiting_for_api_hash)
async def process_api_hash(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Авторизация отменена")
        return
    
    api_hash = message.text.strip()
    await state.update_data(api_hash=api_hash)
    
    await message.answer(
        "**Шаг 3 из 5:** Введите ваш номер телефона\n"
        "в международном формате с +\n\n"
        "Пример: `+79001234567`\n\n"
        "Для отмены: /cancel",
        parse_mode="Markdown"
    )
    await state.set_state(AuthState.waiting_for_phone)

@dp.message(AuthState.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Авторизация отменена")
        return
    
    phone_number = message.text.strip()
    data = await state.get_data()
    
    try:
        client = Client(
            f"temp_{message.from_user.id}",
            api_id=data["api_id"],
            api_hash=data["api_hash"],
            in_memory=True
        )
        await client.connect()
        
        sent_code = await client.send_code(phone_number)
        
        # Временное хранилище
        pending_auth = getattr(cmd_login, "pending_auth", {})
        pending_auth[message.from_user.id] = {
            "client": client,
            "phone_number": phone_number,
            "phone_code_hash": sent_code.phone_code_hash,
            "api_id": data["api_id"],
            "api_hash": data["api_hash"]
        }
        cmd_login.pending_auth = pending_auth
        
        await message.answer(
            "**Код подтверждения отправлен!**\n\n"
            "**Шаг 4 из 5:** Введите код, который пришёл вам в Telegram\n"
            "(обычно 5 цифр)\n\n"
            "Пример: `12345`\n\n"
            "Для отмены: /cancel",
            parse_mode="Markdown"
        )
        await state.set_state(AuthState.waiting_for_code)
        
    except Exception as e:
        await message.answer(f"Ошибка: {e}\nПроверьте номер и попробуйте снова")
        await state.clear()

@dp.message(AuthState.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Авторизация отменена")
        return
    
    code = message.text.strip()
    pending_auth = getattr(cmd_login, "pending_auth", {})
    user_data = pending_auth.get(message.from_user.id)
    
    if not user_data:
        await message.answer("Сессия не найдена. Начните заново с /login")
        await state.clear()
        return
    
    try:
        client = user_data["client"]
        
        result = await client.sign_in(
            phone_number=user_data["phone_number"],
            phone_code_hash=user_data["phone_code_hash"],
            phone_code=code
        )
        
        if hasattr(result, 'is_password_required') and result.is_password_required:
            await message.answer(
                "**Шаг 5 из 5:** Введите пароль двухфакторной аутентификации\n\n"
                "Для отмены: /cancel",
                parse_mode="Markdown"
            )
            await state.set_state(AuthState.waiting_for_2fa)
            return
        
        session_string = await client.export_session_string()
        save_session_string(message.from_user.id, session_string)
        save_user_api(message.from_user.id, user_data["api_id"], user_data["api_hash"])
        
        await client.disconnect()
        del pending_auth[message.from_user.id]
        
        await message.answer(
            "**Авторизация успешно завершена!**\n\n"
            "Теперь выполните /setup для настройки канала и группы.\n\n"
            "Пример: `/setup -1001234567890 -1009876543210`",
            parse_mode="Markdown"
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(f"Ошибка: {e}\nПопробуйте снова /login")
        await state.clear()

@dp.message(AuthState.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Авторизация отменена")
        return
    
    password = message.text.strip()
    pending_auth = getattr(cmd_login, "pending_auth", {})
    user_data = pending_auth.get(message.from_user.id)
    
    if not user_data:
        await message.answer("Сессия не найдена. Начните заново с /login")
        await state.clear()
        return
    
    try:
        client = user_data["client"]
        await client.check_password(password)
        
        session_string = await client.export_session_string()
        save_session_string(message.from_user.id, session_string)
        save_user_api(message.from_user.id, user_data["api_id"], user_data["api_hash"])
        
        await client.disconnect()
        del pending_auth[message.from_user.id]
        
        await message.answer(
            "**Авторизация успешно завершена!**\n\n"
            "Теперь выполните /setup для настройки канала и группы.\n\n"
            "Пример: `/setup -1001234567890 -1009876543210`",
            parse_mode="Markdown"
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(f"Неверный пароль: {e}\nПопробуйте снова")

@dp.message(Command("setup"))
async def setup_command(message: types.Message):
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "Использование: /setup ID_канала ID_группы\n\n"
            "Пример: `/setup -1001234567890 -1009876543210`\n\n"
            "Как получить ID: перешлите сообщение из канала/группы боту @userinfobot",
            parse_mode="Markdown"
        )
        return
    
    try:
        channel_id = int(args[1])
        group_id = int(args[2])
    except ValueError:
        await message.answer("ID должны быть числами")
        return
    
    # Проверяем, что пользователь авторизован
    if not get_user_api(message.from_user.id):
        await message.answer("Сначала выполните /login для авторизации")
        return
    
    save_settings(message.from_user.id, channel_id, group_id)
    
    await message.answer(
        f"**Настройки сохранены!**\n\n"
        f"Канал: `{channel_id}`\n"
        f"Группа: `{group_id}`\n\n"
        f"Теперь используйте /check",
        parse_mode="Markdown"
    )

@dp.message(Command("mysettings"))
async def mysettings(message: types.Message):
    settings = get_settings(message.from_user.id)
    api_data = get_user_api(message.from_user.id)
    session_exists = get_session_string(message.from_user.id) is not None
    
    text = "📋 **Ваши настройки:**\n\n"
    
    if settings:
        text += f"Канал: `{settings['channel_id']}`\n"
        text += f"Группа: `{settings['group_id']}`\n"
    else:
        text += "Канал: не настроен\n"
        text += "Группа: не настроена\n"
    
    if api_data:
        text += f"API ID: `{api_data['api_id']}`\n"
    else:
        text += "API ID: не указан\n"
    
    text += f"Сессия: {'активна' if session_exists else 'не активна'}\n\n"
    
    if not api_data:
        text += "Выполните /login для авторизации\n"
    if not settings:
        text += "Выполните /setup для настройки канала и группы"
    
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("check"))
async def check_command(message: types.Message):
    settings = get_settings(message.from_user.id)
    if not settings:
        await message.answer("Сначала выполните /setup")
        return
    
    api_data = get_user_api(message.from_user.id)
    if not api_data:
        await message.answer("Сначала выполните /login для авторизации")
        return
    
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Использование:\n/check https://t.me/c/123/456 24")
        return
    
    post_url = args[1]
    try:
        hours = int(args[2])
    except ValueError:
        await message.answer("Часы должны быть числом")
        return
    
    try:
        parts = post_url.replace("https://t.me/c/", "").split("/")
        channel_from_url = int("-100" + parts[0])
        post_id = int(parts[1])
    except Exception:
        await message.answer("Неверный формат ссылки.\nПример: https://t.me/c/1234567890/456")
        return
    
    if channel_from_url != settings["channel_id"]:
        await message.answer(f"Это не ваш канал. Ваш ID канала: {settings['channel_id']}")
        return
    
    deadline = asyncio.get_event_loop().time() + hours * 3600
    
    tasks[post_id] = {
        "deadline": deadline,
        "chat_id": message.chat.id,
        "post_link": post_url,
        "hours": hours,
        "user_id": message.from_user.id
    }
    
    await message.answer(
        f"**Задача создана**\n\n"
        f"Пост: {post_url}\n"
        f"Жду {hours} часов\n"
        f"Готово: {datetime.fromtimestamp(deadline).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"Через {hours} часов я:\n"
        f"• Кикну неотметившихся из группы\n"
        f"• Пришлю список и спрошу подтверждение\n"
        f"• После подтверждения удалю из канала",
        parse_mode="Markdown"
    )
    
    asyncio.create_task(process_post(post_id, deadline, message.chat.id, post_url, message.from_user.id))

@dp.message(Command("status"))
async def status(message: types.Message):
    user_tasks = {pid: data for pid, data in tasks.items() if data.get("user_id") == message.from_user.id}
    
    if not user_tasks:
        await message.answer("Нет активных задач")
        return
    
    text = "**Ваши активные задачи:**\n\n"
    for pid, data in user_tasks.items():
        remaining = int(data["deadline"] - asyncio.get_event_loop().time())
        hours_left = remaining // 3600
        mins_left = (remaining % 3600) // 60
        text += f"• Пост {pid}: {hours_left}ч {mins_left}м осталось\n"
        text += f"  {data['post_link']}\n\n"
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("cancel"))
async def cancel(message: types.Message):
    args = message.text.split()
    if len(args) != 2:
        await message.answer("Использование: /cancel 123 (где 123 — ID поста из ссылки)")
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

# ========== ОБРАБОТЧИК РЕДАКТИРОВАНИЯ ==========
@dp.callback_query(lambda c: c.data.startswith("edit_"))
async def handle_edit(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[1]
    
    if temp_id not in pending_cleanups:
        await callback.answer("Данные устарели", show_alert=True)
        await callback.message.delete()
        return
    
    pending_cleanups[temp_id]["editing"] = True
    
    await callback.message.edit_text(
        f"**Режим редактирования списка**\n\n"
        f"Всего в списке: {pending_cleanups[temp_id]['total_count']} человек\n\n"
        f"**Исключить пользователей:**\n"
        f"Отправьте ID пользователей, которых НЕ нужно удалять.\n"
        f"Можно указать несколько: через запятую, пробел или каждый с новой строки.\n\n"
        f"Пример: `123456789, 987654321`\n\n"
        f"Для отмены редактирования отправьте /cancel_edit",
        parse_mode="Markdown"
    )
    await callback.answer()

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
    
    text = message.text.strip()
    ids_raw = re.split(r'[,\s\n]+', text)
    
    exclude_ids = set()
    for item in ids_raw:
        item = item.strip()
        if item.isdigit():
            exclude_ids.add(int(item))
    
    if not exclude_ids:
        await message.answer("Не найдено ни одного корректного ID. Попробуйте снова.")
        return
    
    original_ids = set(active_data["user_ids_list"])
    new_ids = list(original_ids - exclude_ids)
    
    active_data["user_ids_list"] = new_ids
    active_data["total_count"] = len(new_ids)
    active_data["editing"] = False
    
    # Обновляем CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id"])
    for uid in new_ids:
        writer.writerow([uid])
    active_data["csv_bytes"] = output.getvalue().encode("utf-8")
    
    # Формируем список
    user_lines = []
    for uid in new_ids[:30]:
        try:
            user = await bot.get_chat(uid)
            username = f" (@{user.username})" if user.username else ""
            user_lines.append(f"• `{uid}`{username}")
        except:
            user_lines.append(f"• `{uid}`")
    
    list_text = "\n".join(user_lines)
    if len(new_ids) > 30:
        list_text += f"\n\n... и ещё {len(new_ids) - 30} человек"
    
    confirm_text = (
        f"**Требуется подтверждение**\n\n"
        f"Пост: {active_data['post_link']}\n"
        f"Не отметилось: {len(new_ids)} человек"
    )
    
    if len(new_ids) != len(original_ids):
        confirm_text += f"\n\n(Исключено {len(original_ids) - len(new_ids)} человек)"
    
    confirm_text += f"\n\n**Список ID для удаления:**\n{list_text}\n\nУдалить этих пользователей из канала?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Удалить всех", callback_data=f"confirm_yes_{active_temp_id}"),
            InlineKeyboardButton(text="Изменить ещё", callback_data=f"edit_{active_temp_id}"),
            InlineKeyboardButton(text="Отмена", callback_data=f"confirm_no_{active_temp_id}")
        ]
    ])
    
    await bot.send_message(
        message.chat.id,
        confirm_text,
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await message.delete()

@dp.message(Command("cancel_edit"))
async def cancel_edit(message: types.Message):
    for temp_id, data in pending_cleanups.items():
        if data.get("editing") and data.get("user_id") == message.from_user.id:
            data["editing"] = False
            await message.answer("Редактирование отменено. Список не изменился.")
            return
    
    await message.answer("Нет активного редактирования")

@dp.callback_query(lambda c: c.data.startswith("confirm_yes_"))
async def handle_confirm_yes(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[2]
    
    if temp_id not in pending_cleanups:
        await callback.answer("Данные устарели", show_alert=True)
        await callback.message.delete()
        return
    
    data = pending_cleanups[temp_id]
    
    if data["total_count"] == 0:
        await callback.message.edit_text("Список пуст — некого удалять")
        del pending_cleanups[temp_id]
        await callback.answer()
        return
    
    await callback.message.edit_text("Удаляю пользователей из канала...")
    
    async def update_progress(current, total):
        await callback.message.edit_text(f"Удаляю... {current}/{total} ({current*100//total}%)")
    
    result = await clean_channel_from_list(data["user_id"], data["user_ids_list"], update_progress)
    
    report = (
        f"**Удаление из канала завершено**\n\n"
        f"Пост: {data['post_link']}\n"
        f"Не отметилось: {data['total_count']}\n"
        f"Удалено из канала: {result['success']}\n"
        f"Ошибок: {result['errors']}\n"
    )
    
    await callback.message.edit_text(report, parse_mode="Markdown")
    del pending_cleanups[temp_id]
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_no_"))
async def handle_confirm_no(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[2]
    
    if temp_id not in pending_cleanups:
        await callback.answer("Данные устарели", show_alert=True)
        await callback.message.delete()
        return
    
    data = pending_cleanups[temp_id]
    
    await callback.message.edit_text(
        f"**Удаление отменено**\n\n"
        f"Пост: {data['post_link']}\n"
        f"Не отметилось: {data['total_count']} человек\n"
        f"CSV-файл с ID сохранён выше.",
        parse_mode="Markdown"
    )
    del pending_cleanups[temp_id]
    await callback.answer()

# ========== ОСНОВНАЯ ЛОГИКА ПРОВЕРКИ ==========
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
    
    # Собираем комментаторов
    commenters = set()
    try:
        async for msg in bot.get_chat_history(settings["channel_id"], limit=1000):
            if msg.reply_to_message and msg.reply_to_message.message_id == post_id:
                commenters.add(msg.from_user.id)
    except Exception as e:
        await bot.send_message(reply_chat_id, f"Ошибка сбора комментаторов: {e}")
        return
    
    # Собираем участников группы
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
    
    # Кикаем из группы
    kicked_group = 0
    for uid in to_kick:
        try:
            await bot.ban_chat_member(settings["group_id"], uid)
            await asyncio.sleep(0.2)
            await bot.unban_chat_member(settings["group_id"], uid)
            kicked_group += 1
        except Exception:
            pass
    
    await bot.send_message(
        settings["group_id"],
        f"**Чистка группы**\nПост: {post_link}\nКикнуто из группы: {kicked_group}",
        parse_mode="Markdown"
    )
    
    # Готовим CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id"])
    for uid in to_kick:
        writer.writerow([uid])
    
    csv_bytes = output.getvalue().encode("utf-8")
    
    # Формируем список для показа
    user_lines = []
    for uid in to_kick[:30]:
        try:
            user = await bot.get_chat(uid)
            username = f" (@{user.username})" if user.username else ""
            user_lines.append(f"• `{uid}`{username}")
        except:
            user_lines.append(f"• `{uid}`")
    
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
        f"**Требуется подтверждение**\n\n"
        f"Пост: {post_link}\n"
        f"Не отметилось: {len(to_kick)} человек\n\n"
        f"**Список ID для удаления:**\n{list_text}\n\n"
        f"Удалить этих пользователей из канала?"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Удалить всех", callback_data=f"confirm_yes_{temp_id}"),
            InlineKeyboardButton(text="Изменить список", callback_data=f"edit_{temp_id}"),
            InlineKeyboardButton(text="Отмена", callback_data=f"confirm_no_{temp_id}")
        ]
    ])
    
    await bot.send_document(
        reply_chat_id,
        types.BufferedInputFile(csv_bytes, filename=f"to_kick_post_{post_id}.csv"),
        caption=f"📎 CSV-файл со всеми ID ({len(to_kick)} человек) — резервная копия"
    )
    
    await bot.send_message(
        reply_chat_id,
        confirm_text,
        parse_mode="Markdown",
        reply_markup=keyboard
    )

# ========== FLASK ДЛЯ RENDER ==========
@app.route('/')
def health():
    return jsonify({"status": "ok", "message": "Bot is running"})

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy"})

def run_bot():
    asyncio.run(dp.start_polling(bot))

# ========== ЗАПУСК ==========
if __name__ == "__main__":
    # Добавляем временное хранилище для авторизации
    cmd_login.pending_auth = {}
    
    # Запускаем бота в фоновом потоке
    bot_thread = threading.Thread(target=run_bot)
    bot_thread.start()
    
    print("Бот запущен и готов к работе")
    
    # Запускаем Flask
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
