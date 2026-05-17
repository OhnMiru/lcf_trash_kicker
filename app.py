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
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv

load_dotenv()

# ========== НАСТРОЙКИ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Публичные API ключи для Telethon
API_ID = 2496
API_HASH = "8da85c0d2d427f5c63c9a438d164bedf"

# Создаём Flask приложение (будет в фоновом потоке)
app = Flask(__name__)

# Создаём бота
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
        group_id INTEGER
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        user_id INTEGER PRIMARY KEY,
        session_string TEXT
    )
""")
conn.commit()

# Глобальные переменные
telethon_clients = {}
tasks = {}
pending_cleanups = {}

# Состояния для авторизации
class AuthState(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()

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

def get_session(user_id: int):
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    return row[0] if row else None

def save_session(user_id: int, session_string: str):
    cursor.execute("INSERT OR REPLACE INTO sessions (user_id, session_string) VALUES (?, ?)",
                   (user_id, session_string))
    conn.commit()

async def get_telethon_client(user_id: int):
    if user_id in telethon_clients and telethon_clients[user_id].is_connected():
        return telethon_clients[user_id]
    
    session_string = get_session(user_id)
    if not session_string:
        return None
    
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.start()
    telethon_clients[user_id] = client
    return client

async def clean_channel_from_list(user_id: int, user_ids: list, progress_callback=None) -> dict:
    if not user_ids:
        return {"success": 0, "errors": 0, "total": 0}
    
    settings = get_settings(user_id)
    if not settings:
        return {"success": 0, "errors": 0, "total": 0, "error": "Settings not found"}
    
    client = await get_telethon_client(user_id)
    if not client:
        return {"success": 0, "errors": 0, "total": 0, "error": "Session not found. Run /login"}
    
    success = 0
    errors = 0
    
    try:
        channel_entity = await client.get_entity(settings["channel_id"])
    except Exception as e:
        return {"success": 0, "errors": 0, "total": 0, "error": f"Cannot find channel: {e}"}
    
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
        "Bot for automatic channel cleaning\n\n"
        "/login - one-time authorization\n"
        "/setup channel_id group_id - configure channel and group\n"
        "/check link hours - start checking\n"
        "/status - active tasks\n"
        "/cancel post_id - cancel task\n"
        "/mysettings - current settings"
    )

@dp.message(Command("login"))
async def cmd_login(message: types.Message, state: FSMContext):
    await message.answer(
        "AUTHORIZATION\n\n"
        "Enter your phone number in international format.\n"
        "Example: +79001234567\n\n"
        "To cancel: /cancel"
    )
    await state.set_state(AuthState.waiting_for_phone)

@dp.message(AuthState.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Cancelled")
        return
    
    phone = message.text.strip()
    if not phone.startswith('+'):
        await message.answer("Phone number must start with +")
        return
    
    await state.update_data(phone=phone)
    
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    
    try:
        await client.send_code_request(phone)
        await state.update_data(client=client)
        await message.answer(
            "Code sent!\n\n"
            "Enter the confirmation code from Telegram.\n"
            "Example: 12345\n\n"
            "To cancel: /cancel"
        )
        await state.set_state(AuthState.waiting_for_code)
    except Exception as e:
        await message.answer(f"Error: {e}")
        await state.clear()

@dp.message(AuthState.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Cancelled")
        return
    
    code = message.text.strip()
    data = await state.get_data()
    client = data.get('client')
    phone = data.get('phone')
    
    if not client:
        await message.answer("Session lost. Start over with /login")
        await state.clear()
        return
    
    try:
        await client.sign_in(phone, code)
        session_string = StringSession.save(client.session)
        save_session(message.from_user.id, session_string)
        
        await client.disconnect()
        
        await message.answer(
            "Authorization successful!\n\n"
            "Now run /setup to configure channel and group.\n"
            "Example: /setup -1001234567890 -1009876543210"
        )
        await state.clear()
        
    except Exception as e:
        error_text = str(e).lower()
        if "password" in error_text or "2fa" in error_text:
            await message.answer(
                "2FA password required\n\n"
                "Enter your two-factor authentication password.\n"
                "To cancel: /cancel"
            )
            await state.set_state(AuthState.waiting_for_password)
            await state.update_data(client=client, phone=phone)
        else:
            await message.answer(f"Error: {e}")
            await state.clear()

@dp.message(AuthState.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Cancelled")
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
            "Authorization successful!\n\n"
            "Now run /setup to configure channel and group."
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(f"Error: {e}")

@dp.message(Command("setup"))
async def setup_command(message: types.Message):
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "Usage: /setup channel_id group_id\n"
            "Example: /setup -1001234567890 -1009876543210"
        )
        return
    
    try:
        channel_id = int(args[1])
        group_id = int(args[2])
    except ValueError:
        await message.answer("Channel ID and group ID must be numbers")
        return
    
    if not get_session(message.from_user.id):
        await message.answer("Please run /login first")
        return
    
    save_settings(message.from_user.id, channel_id, group_id)
    await message.answer(
        f"Settings saved!\n\n"
        f"Channel: {channel_id}\n"
        f"Group: {group_id}\n\n"
        f"Now use /check"
    )

@dp.message(Command("mysettings"))
async def mysettings(message: types.Message):
    settings = get_settings(message.from_user.id)
    has_session = get_session(message.from_user.id) is not None
    
    text = "Your settings:\n\n"
    if settings:
        text += f"Channel: {settings['channel_id']}\n"
        text += f"Group: {settings['group_id']}\n"
    else:
        text += "Channel: not configured\n"
    
    text += f"Authorization: {'done' if has_session else 'not done'}\n"
    
    await message.answer(text)

@dp.message(Command("check"))
async def check_command(message: types.Message):
    settings = get_settings(message.from_user.id)
    if not settings:
        await message.answer("Please run /setup first")
        return
    
    if not get_session(message.from_user.id):
        await message.answer("Please run /login first")
        return
    
    args = message.text.split()
    if len(args) < 3:
        await message.answer("Usage: /check https://t.me/c/123/456 24")
        return
    
    post_url = args[1]
    try:
        hours = int(args[2])
    except ValueError:
        await message.answer("Hours must be a number")
        return
    
    try:
        parts = post_url.replace("https://t.me/c/", "").split("/")
        channel_from_url = int("-100" + parts[0])
        post_id = int(parts[1])
    except Exception:
        await message.answer("Invalid link format. Example: https://t.me/c/1234567890/456")
        return
    
    if channel_from_url != settings["channel_id"]:
        await message.answer(f"This is not your channel. Your channel ID: {settings['channel_id']}")
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
        f"Task created\n\n"
        f"Post: {post_url}\n"
        f"Waiting {hours} hours\n"
        f"Ready at: {datetime.fromtimestamp(deadline).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    asyncio.create_task(process_post(post_id, deadline, message.chat.id, post_url, message.from_user.id))

@dp.message(Command("status"))
async def status(message: types.Message):
    user_tasks = {pid: data for pid, data in tasks.items() if data.get("user_id") == message.from_user.id}
    
    if not user_tasks:
        await message.answer("No active tasks")
        return
    
    text = "Your active tasks:\n\n"
    for pid, data in user_tasks.items():
        remaining = int(data["deadline"] - asyncio.get_event_loop().time())
        hours_left = remaining // 3600
        mins_left = (remaining % 3600) // 60
        text += f"Post {pid}: {hours_left}h {mins_left}m remaining\n"
    await message.answer(text)

@dp.message(Command("cancel"))
async def cancel(message: types.Message):
    args = message.text.split()
    if len(args) != 2:
        await message.answer("Usage: /cancel post_id")
        return
    
    try:
        post_id = int(args[1])
    except ValueError:
        await message.answer("Post ID must be a number")
        return
    
    if post_id in tasks and tasks[post_id].get("user_id") == message.from_user.id:
        del tasks[post_id]
        await message.answer(f"Task for post {post_id} cancelled")
    else:
        await message.answer(f"Task for post {post_id} not found")

# ========== КОЛБЭКИ ==========
@dp.callback_query(lambda c: c.data.startswith("edit_"))
async def handle_edit(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[1]
    
    if temp_id not in pending_cleanups:
        await callback.answer("Data expired")
        return
    
    pending_cleanups[temp_id]["editing"] = True
    
    await callback.message.edit_text(
        f"EDIT MODE\n\n"
        f"Total in list: {pending_cleanups[temp_id]['total_count']} users\n\n"
        f"Send user IDs to exclude\n"
        f"Example: 123456789, 987654321\n\n"
        f"To cancel: /cancel_edit"
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
        await message.answer("No valid IDs found")
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
        list_text += f"\n\n... and {len(new_ids) - 30} more"
    
    confirm_text = (
        f"CONFIRMATION REQUIRED\n\n"
        f"Post: {active_data['post_link']}\n"
        f"Not marked: {len(new_ids)}"
    )
    if len(new_ids) != len(original_ids):
        confirm_text += f"\n(Excluded {len(original_ids) - len(new_ids)})"
    
    confirm_text += f"\n\nList:\n{list_text}\n\nRemove these users from channel?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="YES", callback_data=f"confirm_yes_{active_temp_id}"),
            InlineKeyboardButton(text="EDIT AGAIN", callback_data=f"edit_{active_temp_id}"),
            InlineKeyboardButton(text="NO", callback_data=f"confirm_no_{active_temp_id}")
        ]
    ])
    
    await bot.send_message(message.chat.id, confirm_text, reply_markup=keyboard)
    await message.delete()

@dp.message(Command("cancel_edit"))
async def cancel_edit(message: types.Message):
    for temp_id, data in pending_cleanups.items():
        if data.get("editing") and data.get("user_id") == message.from_user.id:
            data["editing"] = False
            await message.answer("Edit cancelled")
            return
    
    await message.answer("No active edit")

@dp.callback_query(lambda c: c.data.startswith("confirm_yes_"))
async def handle_confirm_yes(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[2]
    
    if temp_id not in pending_cleanups:
        await callback.answer("Data expired")
        return
    
    data = pending_cleanups[temp_id]
    
    if data["total_count"] == 0:
        await callback.message.edit_text("List empty - nothing to remove")
        del pending_cleanups[temp_id]
        return
    
    await callback.message.edit_text("Removing users from channel...")
    
    async def update_progress(current, total):
        await callback.message.edit_text(f"Removing... {current}/{total} ({current*100//total}%)")
    
    result = await clean_channel_from_list(data["user_id"], data["user_ids_list"], update_progress)
    
    report = (
        f"DONE\n\n"
        f"Post: {data['post_link']}\n"
        f"Not marked: {data['total_count']}\n"
        f"Removed: {result['success']}\n"
        f"Errors: {result['errors']}"
    )
    
    await callback.message.edit_text(report)
    del pending_cleanups[temp_id]
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_no_"))
async def handle_confirm_no(callback: types.CallbackQuery):
    temp_id = callback.data.split("_")[2]
    
    if temp_id in pending_cleanups:
        await callback.message.edit_text("Removal cancelled")
        del pending_cleanups[temp_id]

# ========== ОСНОВНАЯ ЛОГИКА ==========
async def process_post(post_id: int, deadline: float, reply_chat_id: int, post_link: str, user_id: int):
    settings = get_settings(user_id)
    if not settings:
        await bot.send_message(reply_chat_id, "Settings not found")
        return
    
    wait_seconds = deadline - asyncio.get_event_loop().time()
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)
    
    if post_id in tasks:
        del tasks[post_id]
    
    # Get commenters
    commenters = set()
    try:
        async for msg in bot.get_chat_history(settings["channel_id"], limit=1000):
            if msg.reply_to_message and msg.reply_to_message.message_id == post_id:
                commenters.add(msg.from_user.id)
    except Exception as e:
        await bot.send_message(reply_chat_id, f"Error getting commenters: {e}")
        return
    
    # Get group members
    members = set()
    try:
        async for member in bot.get_chat_members(settings["group_id"]):
            if not member.user.is_bot:
                members.add(member.user.id)
    except Exception as e:
        await bot.send_message(reply_chat_id, f"Error getting members: {e}")
        return
    
    to_kick = list(members - commenters)
    
    if not to_kick:
        await bot.send_message(reply_chat_id, f"Post {post_link}\nAll marked!")
        return
    
    # Kick from group
    kicked_group = 0
    for uid in to_kick:
        try:
            await bot.ban_chat_member(settings["group_id"], uid)
            await asyncio.sleep(0.2)
            await bot.unban_chat_member(settings["group_id"], uid)
            kicked_group += 1
        except Exception:
            pass
    
    await bot.send_message(settings["group_id"], f"Kicked from group: {kicked_group}")
    
    # Prepare CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id"])
    for uid in to_kick:
        writer.writerow([uid])
    csv_bytes = output.getvalue().encode("utf-8")
    
    # Prepare list preview
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
        list_text += f"\n\n... and {len(to_kick) - 30} more"
    
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
        f"CONFIRMATION REQUIRED\n\n"
        f"Post: {post_link}\n"
        f"Not marked: {len(to_kick)}\n\n"
        f"List:\n{list_text}\n\n"
        f"Remove these users from channel?"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="YES", callback_data=f"confirm_yes_{temp_id}"),
            InlineKeyboardButton(text="EDIT LIST", callback_data=f"edit_{temp_id}"),
            InlineKeyboardButton(text="NO", callback_data=f"confirm_no_{temp_id}")
        ]
    ])
    
    await bot.send_document(reply_chat_id, types.BufferedInputFile(csv_bytes, filename=f"to_kick_{post_id}.csv"))
    await bot.send_message(reply_chat_id, confirm_text, reply_markup=keyboard)

# ========== FLASK ДЛЯ RENDER (ФОНОВЫЙ ПОТОК) ==========
def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ========== ЗАПУСК (БОТ В ГЛАВНОМ ПОТОКЕ) ==========
async def main():
    print("Bot started and ready")
    await dp.start_polling(bot)

if __name__ == "__main__":
    # Запускаем Flask в фоновом потоке
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # Запускаем бота в главном потоке
    asyncio.run(main())
