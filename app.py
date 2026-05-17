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
MINI_APP_URL = "https://telegram-auth-app.onrender.com"  # Замените на ваш URL

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
        session_string TEXT
    )
""")
conn.commit()

# Глобальные переменные
telethon_clients = {}
tasks = {}
pending_cleanups = {}

# Свой event loop для Telethon в отдельном потоке
telethon_loop = None
telethon_thread = None

# ========== ФУНКЦИИ ==========
def get_settings(user_id: int):
    cursor.execute("SELECT channel_id, group_id, session_string FROM settings WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row:
        return {"channel_id": row[0], "group_id": row[1], "session_string": row[2]}
    return None

def save_settings(user_id: int, channel_id: int, group_id: int, session_string: str = None):
    existing = get_settings(user_id)
    if existing and session_string is None:
        session_string = existing.get("session_string")
    cursor.execute("INSERT OR REPLACE INTO settings (user_id, channel_id, group_id, session_string) VALUES (?, ?, ?, ?)",
                   (user_id, channel_id, group_id, session_string))
    conn.commit()

def save_session(user_id: int, session_string: str):
    settings = get_settings(user_id)
    if settings:
        cursor.execute("UPDATE settings SET session_string = ? WHERE user_id = ?", (session_string, user_id))
    else:
        cursor.execute("INSERT INTO settings (user_id, session_string) VALUES (?, ?)", (user_id, session_string))
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

# ========== ОБРАБОТЧИК СЕССИИ ==========
@dp.message(lambda message: message.text and message.text.startswith("SESSION:"))
async def handle_session_string(message: types.Message):
    session_string = message.text.replace("SESSION:", "").strip()
    
    if len(session_string) > 50:
        save_session(message.from_user.id, session_string)
        await message.answer(
            "Авторизация успешна!\n\n"
            "Теперь выполните /setup для настройки канала и группы.\n"
            "Пример: /setup -1001234567890 -1009876543210"
        )
    else:
        await message.answer("Ошибка: получена неверная сессионная строка")

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
async def cmd_login(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Войти через Telegram",
            web_app=types.WebAppInfo(url=MINI_APP_URL)
        )]
    ])
    
    await message.answer(
        "АВТОРИЗАЦИЯ\n\n"
        "Нажмите на кнопку ниже, чтобы открыть окно авторизации.\n\n"
        "Что нужно ввести:\n"
        "1. API ID (число с my.telegram.org)\n"
        "2. API HASH (строка с my.telegram.org)\n"
        "3. Номер телефона в формате +79001234567\n\n"
        "Это нужно сделать один раз.",
        reply_markup=keyboard
    )

@dp.message(Command("setup"))
async def setup_command(message: types.Message):
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "Использование: /setup ID_канала ID_группы\n"
            "Пример: /setup -1001234567890 -1009876543210"
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
    
    save_settings(message.from_user.id, channel_id, group_id, settings.get("session_string"))
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
        await message.answer("Использование: /check https://t.me/c/123/456 24")
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
        await message.answer("Неверный формат ссылки")
        return
    
    if channel_from_url != settings["channel_id"]:
        await message.answer(f"Это не ваш канал. Ваш ID: {settings['channel_id']}")
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
        f"Задача создана\n\n"
        f"Пост: {post_url}\n"
        f"Жду {hours} часов\n"
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
        f"Отправьте ID для исключения\nПример: 123456789, 987654321"
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
    
    confirm_text += f"\n\nСписок:\n{list_text}\n\nУдалить из канала?"
    
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
        await callback.message.edit_text("Список пуст")
        del pending_cleanups[temp_id]
        return
    
    await callback.message.edit_text("Удаляю...")
    
    async def update_progress(current, total):
        await callback.message.edit_text(f"Удаляю... {current}/{total} ({current*100//total}%)")
    
    result = await clean_channel_from_list(data["user_id"], data["user_ids_list"], update_progress)
    
    report = (
        f"ГОТОВО!\n\n"
        f"Пост: {data['post_link']}\n"
        f"Не отметилось: {data['total_count']}\n"
        f"Удалено: {result['success']}\n"
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
        await bot.send_message(reply_chat_id, f"Ошибка: {e}")
        return
    
    members = set()
    try:
        async for member in bot.get_chat_members(settings["group_id"]):
            if not member.user.is_bot:
                members.add(member.user.id)
    except Exception as e:
        await bot.send_message(reply_chat_id, f"Ошибка: {e}")
        return
    
    to_kick = list(members - commenters)
    
    if not to_kick:
        await bot.send_message(reply_chat_id, f"Все отметились!")
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
        list_text += f"\n\n... и ещё {len(to_kick) - 30}"
    
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
        f"Список:\n{list_text}\n\nУдалить из канала?"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="ДА", callback_data=f"confirm_yes_{temp_id}"),
            InlineKeyboardButton(text="ИЗМЕНИТЬ", callback_data=f"edit_{temp_id}"),
            InlineKeyboardButton(text="НЕТ", callback_data=f"confirm_no_{temp_id}")
        ]
    ])
    
    await bot.send_document(reply_chat_id, types.BufferedInputFile(csv_bytes, filename=f"to_kick_{post_id}.csv"))
    await bot.send_message(reply_chat_id, confirm_text, reply_markup=keyboard)

# ========== FLASK (ВЕБ-ПРОСЛУШКА) ==========
@app.route('/')
@app.route('/health')
def health():
    return jsonify({"status": "ok", "bot": "running"})

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)

# ========== ЗАПУСК БОТА ==========
async def main():
    print("Бот запущен и готов к работе")
    await dp.start_polling(bot)

if __name__ == "__main__":
    # Запускаем Flask в отдельном потоке
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # Запускаем бота в основном потоке (aiogram сам управляет event loop)
    asyncio.run(main())
