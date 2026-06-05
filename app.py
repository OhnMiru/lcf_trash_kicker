import asyncio
import secrets
import time
import sqlite3
import os
import re
import threading
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from pyrogram import Client
from pyrogram.errors import SessionRevoked, AuthKeyInvalid, FloodWait
import aiohttp
import requests

# ========== НАСТРОЙКИ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
RENDER_URL = os.getenv("RENDER_URL", "https://lcf-trash-kicker.onrender.com")

# Удаляем вебхук при запуске
try:
    requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=True", timeout=10)
    print("Вебхук удалён при запуске")
except Exception as e:
    print(f"Ошибка удаления вебхука: {e}")

app = Flask(__name__)
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== БАЗА ДАННЫХ ==========
db_path = os.path.join(os.path.dirname(__file__), "settings.db")

def get_db_conn():
    """Создаёт новое подключение к БД для текущего потока."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# Инициализация таблиц
_init_conn = get_db_conn()
_init_conn.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        user_id INTEGER PRIMARY KEY,
        channel_id INTEGER,
        group_id INTEGER,
        session_string TEXT,
        api_id INTEGER,
        api_hash TEXT
    )
""")
_init_conn.execute("""
    CREATE TABLE IF NOT EXISTS active_tasks (
        post_id INTEGER PRIMARY KEY,
        deadline REAL,
        post_link TEXT,
        hours REAL,
        user_id INTEGER,
        reply_chat_id INTEGER
    )
""")
_init_conn.commit()
_init_conn.close()

# ========== КЭШИ ==========
pyrogram_clients: dict[int, Client] = {}
tasks: dict[int, dict] = {}
pending_cleanups: dict[str, dict] = {}
auth_sessions: dict[str, dict] = {}   # token -> {user_id, client, phone, phone_code_hash, api_id, api_hash, expire}
bot_loop: asyncio.AbstractEventLoop | None = None   # основной event loop бота

# ========== FSM ==========
class AuthState(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_phone = State()

class SetupState(StatesGroup):
    waiting_for_channel_link = State()
    waiting_for_group_link = State()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def to_pyrogram_channel_id(channel_id: int) -> int:
    """
    Конвертирует channel_id из формата Bot API (-100XXXXXXXXX)
    в числовой peer ID для Pyrogram (без префикса -100).
    """
    channel_id_str = str(channel_id)
    if channel_id_str.startswith("-100"):
        return int(channel_id_str[4:])
    return channel_id


def parse_post_url(url: str):
    """Парсит ссылку на пост, возвращает (channel_id_or_username, post_id)."""
    clean_url = re.sub(r'https?://(telegram\.me|t\.me)/', '', url)
    parts = clean_url.split('/')
    if len(parts) != 2:
        return None, None
    channel_part = parts[0]
    try:
        post_id = int(parts[1])
    except ValueError:
        return None, None
    if channel_part.lstrip('-').isdigit():
        channel_id = int("-100" + channel_part) if not channel_part.startswith('-') else int(channel_part)
    else:
        channel_id = channel_part
    return channel_id, post_id


def get_settings(user_id: int) -> dict | None:
    conn = get_db_conn()
    try:
        row = conn.execute(
            "SELECT channel_id, group_id, session_string, api_id, api_hash FROM settings WHERE user_id = ?",
            (user_id,)
        ).fetchone()
        if row:
            return {
                "channel_id": row["channel_id"],
                "group_id": row["group_id"],
                "session_string": row["session_string"],
                "api_id": row["api_id"],
                "api_hash": row["api_hash"],
            }
        return None
    finally:
        conn.close()


def save_settings(user_id: int, **kwargs):
    """Сохраняет только переданные поля, остальные берёт из существующей записи."""
    existing = get_settings(user_id) or {}
    channel_id  = kwargs.get("channel_id",    existing.get("channel_id"))
    group_id    = kwargs.get("group_id",       existing.get("group_id"))
    session_str = kwargs.get("session_string", existing.get("session_string"))
    api_id      = kwargs.get("api_id",         existing.get("api_id"))
    api_hash    = kwargs.get("api_hash",       existing.get("api_hash"))

    if session_str is not None:
        session_str = str(session_str)

    conn = get_db_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (user_id, channel_id, group_id, session_string, api_id, api_hash) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, channel_id, group_id, session_str, api_id, api_hash)
        )
        conn.commit()
        print(f"[DB] Сохранено для user_id={user_id}: session_len={len(session_str) if session_str else 0}")
    finally:
        conn.close()


def clear_session(user_id: int):
    conn = get_db_conn()
    try:
        conn.execute("UPDATE settings SET session_string = NULL WHERE user_id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()


# ========== PYROGRAM ==========

async def get_pyrogram_client(user_id: int) -> Client | None:
    """Возвращает активный Pyrogram-клиент, создаёт новый если нужно."""
    if user_id in pyrogram_clients:
        client = pyrogram_clients[user_id]
        if client.is_connected:
            return client
        del pyrogram_clients[user_id]

    settings = get_settings(user_id)
    if not settings:
        print(f"[Pyrogram] Настройки не найдены для user_id={user_id}")
        return None

    session_str = settings.get("session_string")
    if not session_str or not isinstance(session_str, str) or len(session_str) < 10:
        print(f"[Pyrogram] Нет валидной сессии для user_id={user_id}")
        return None

    api_id   = settings.get("api_id")
    api_hash = settings.get("api_hash")
    if not api_id or not api_hash:
        print(f"[Pyrogram] Нет API данных для user_id={user_id}")
        return None

    try:
        client = Client(
            name=f"user_{user_id}",
            api_id=int(api_id),
            api_hash=str(api_hash),
            session_string=session_str,
            in_memory=True,
        )
        await client.start()
        me = await client.get_me()
        print(f"[Pyrogram] Клиент запущен: {me.first_name} (id={me.id})")
        pyrogram_clients[user_id] = client
        return client

    except (SessionRevoked, AuthKeyInvalid) as e:
        print(f"[Pyrogram] Сессия недействительна: {e}")
        clear_session(user_id)
        if user_id in pyrogram_clients:
            del pyrogram_clients[user_id]
        return None

    except FloodWait as e:
        print(f"[Pyrogram] FloodWait {e.value} сек")
        await asyncio.sleep(e.value)
        return None

    except Exception as e:
        print(f"[Pyrogram] Ошибка запуска: {type(e).__name__}: {e}")
        return None


async def get_commenters(client: Client, channel_id: int, post_id: int) -> set[int]:
    """
    Собирает ID всех комментаторов под постом.
    channel_id передаётся в формате Pyrogram (без префикса -100).
    """
    commenters = set()
    try:
        async for msg in client.get_discussion_replies(channel_id, post_id):
            if msg.from_user and not msg.from_user.is_bot:
                commenters.add(msg.from_user.id)
        print(f"[Commenters] Найдено через discussion_replies: {len(commenters)}")
    except Exception as e:
        print(f"[Commenters] discussion_replies недоступен: {e}, пробуем fallback...")
        try:
            chat = await client.get_chat(channel_id)
            if chat.linked_chat:
                group_id = chat.linked_chat.id
                async for msg in client.get_chat_history(group_id, limit=1000):
                    if (msg.from_user and not msg.from_user.is_bot and
                            msg.reply_to_message_id is not None):
                        commenters.add(msg.from_user.id)
                print(f"[Commenters] Найдено через fallback: {len(commenters)}")
        except Exception as e2:
            print(f"[Commenters] Fallback тоже не сработал: {e2}")
    return commenters


async def kick_from_channel(client: Client, channel_id: int, user_ids: list[int],
                            progress_callback=None) -> dict:
    """
    Кикает пользователей из канала.
    channel_id передаётся в формате Pyrogram (без префикса -100).
    """
    if not user_ids:
        return {"success": 0, "errors": 0, "total": 0}
    success = 0
    errors = 0
    for idx, uid in enumerate(user_ids):
        try:
            await client.ban_chat_member(channel_id, uid)
            success += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await client.ban_chat_member(channel_id, uid)
                success += 1
            except Exception:
                errors += 1
        except Exception:
            errors += 1
        await asyncio.sleep(0.3)
        if progress_callback and (idx + 1) % max(1, len(user_ids) // 10) == 0:
            await progress_callback(idx + 1, len(user_ids))
    return {"success": success, "errors": errors, "total": len(user_ids)}


# ========== ВОССТАНОВЛЕНИЕ ЗАДАЧ ==========

def restore_tasks_from_db():
    conn = get_db_conn()
    try:
        rows = conn.execute(
            "SELECT post_id, deadline, post_link, hours, user_id, reply_chat_id FROM active_tasks"
        ).fetchall()
    finally:
        conn.close()

    loop = asyncio.get_event_loop()
    current_time = loop.time()
    restored = 0

    for row in rows:
        post_id, deadline, post_link, hours, user_id, reply_chat_id = (
            row["post_id"], row["deadline"], row["post_link"],
            row["hours"], row["user_id"], row["reply_chat_id"]
        )
        if deadline > current_time:
            tasks[post_id] = {
                "deadline": deadline,
                "post_link": post_link,
                "hours": hours,
                "user_id": user_id,
                "reply_chat_id": reply_chat_id,
            }
            loop.create_task(process_post(post_id, deadline, reply_chat_id, post_link, user_id))
            restored += 1
        else:
            _delete_task(post_id)

    print(f"[Restore] Восстановлено задач: {restored}")


def _delete_task(post_id: int):
    conn = get_db_conn()
    try:
        conn.execute("DELETE FROM active_tasks WHERE post_id = ?", (post_id,))
        conn.commit()
    finally:
        conn.close()


# ========== КОМАНДЫ БОТА ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "Бот для автоматической чистки канала\n\n"
        "/login — авторизация\n"
        "/setup — настройка канала и группы\n"
        "/check ссылка часы — запустить проверку\n"
        "/status — активные задачи\n"
        "/cancel ID — отменить задачу\n"
        "/mysettings — текущие настройки\n"
        "/reset_session — сбросить сессию"
    )


# ----- LOGIN -----

@dp.message(Command("login"))
async def cmd_login(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "АВТОРИЗАЦИЯ\n\n"
        "Шаг 1/4: Введите API ID\n"
        "(число с my.telegram.org → API Development Tools)\n\n"
        "Пример: 1234567\n\n"
        "/cancel — отмена"
    )
    await state.set_state(AuthState.waiting_for_api_id)


@dp.message(AuthState.waiting_for_api_id)
async def process_api_id(message: types.Message, state: FSMContext):
    if message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return
    try:
        api_id = int(message.text.strip())
    except ValueError:
        await message.answer("API ID должен быть числом. Попробуйте ещё раз:")
        return
    await state.update_data(api_id=api_id)
    await message.answer(
        "Шаг 2/4: Введите API HASH\n"
        "(строка с my.telegram.org)\n\n"
        "Пример: a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4\n\n"
        "/cancel — отмена"
    )
    await state.set_state(AuthState.waiting_for_api_hash)


@dp.message(AuthState.waiting_for_api_hash)
async def process_api_hash(message: types.Message, state: FSMContext):
    if not message.text:
        return
    if message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено", reply_markup=types.ReplyKeyboardRemove())
        return
    api_hash = message.text.strip()
    data = await state.get_data()
    await state.update_data(api_hash=api_hash)
    save_settings(message.from_user.id, api_id=data["api_id"], api_hash=api_hash)

    phone_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Поделиться номером", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "Шаг 3/4: Отправьте номер телефона\n\n"
        "Нажмите кнопку ниже или введите вручную: +79001234567\n\n"
        "/cancel — отмена",
        reply_markup=phone_keyboard,
    )
    await state.set_state(AuthState.waiting_for_phone)


@dp.message(AuthState.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено", reply_markup=types.ReplyKeyboardRemove())
        return

    if message.contact:
        phone = message.contact.phone_number
        if not phone.startswith('+'):
            phone = '+' + phone
    elif message.text:
        phone = message.text.strip()
        if not phone.startswith('+'):
            await message.answer("Номер должен начинаться с '+'. Попробуйте ещё раз:")
            return
    else:
        await message.answer("Отправьте номер телефона или нажмите кнопку")
        return

    data = await state.get_data()
    api_id   = data["api_id"]
    api_hash = data["api_hash"]

    await message.answer("Отправляю код подтверждения...", reply_markup=types.ReplyKeyboardRemove())

    try:
        client = Client("auth_temp", api_id=api_id, api_hash=api_hash, in_memory=True)
        await client.connect()
        sent_code = await client.send_code(phone)

        token = secrets.token_urlsafe(32)
        auth_sessions[token] = {
            "user_id": message.from_user.id,
            "client": client,
            "phone": phone,
            "phone_code_hash": sent_code.phone_code_hash,
            "api_id": api_id,
            "api_hash": api_hash,
            "expire": time.time() + 600,
        }

        auth_url = f"{RENDER_URL}/auth?token={token}"

        await message.answer(
            "Код отправлен в Telegram!\n\n"
            "Важно: вводить код нужно НЕ здесь, а по ссылке ниже.\n"
            "Это защита от блокировки Telegram.\n\n"
            f"{auth_url}\n\n"
            "Ссылка действует 10 минут.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.clear()

    except FloodWait as e:
        await message.answer(f"Telegram просит подождать {e.value} сек. Попробуйте позже.",
                             reply_markup=types.ReplyKeyboardRemove())
        await state.clear()
    except Exception as e:
        await message.answer(f"Ошибка: {type(e).__name__}: {e}\n\nПроверьте API ID и HASH.",
                             reply_markup=types.ReplyKeyboardRemove())
        await state.clear()


# ----- JOIN GROUP -----

async def find_group_in_dialogs(client: Client, group_id: int):
    """Ищет чат группы в диалогах Pyrogram. Возвращает объект чата или None."""
    try:
        async for dialog in client.get_dialogs():
            if dialog.chat and dialog.chat.id == group_id:
                return dialog.chat
    except Exception as e:
        print(f"[Dialogs] Ошибка поиска в диалогах: {e}")
    return None


@dp.message(Command("join_group"))
async def cmd_join_group(message: types.Message):
    settings = get_settings(message.from_user.id)
    if not settings or not settings.get("session_string"):
        await message.answer("Сначала выполните /login")
        return
    if not settings.get("group_id"):
        await message.answer("Сначала выполните /setup")
        return

    client = await get_pyrogram_client(message.from_user.id)
    if not client:
        await message.answer("Сессия недоступна. Выполните /reset_session и /login")
        return

    await message.answer("Проверяю доступ к группе...")

    # Сначала ищем в диалогах — самый надёжный способ
    group_chat = await find_group_in_dialogs(client, settings["group_id"])

    if group_chat:
        await message.answer(
            f"Аккаунт уже подключён к группе «{group_chat.title}».\n\n"
            f"Всё в порядке, /check должен работать."
        )
        return

    # Не нашли в диалогах — вступаем по инвайт-ссылке
    await message.answer("Группа не найдена в диалогах. Вступаю по инвайт-ссылке...")
    try:
        invite_link = await bot.export_chat_invite_link(settings["group_id"])
        await client.join_chat(invite_link)

        # Проверяем после вступления
        group_chat = await find_group_in_dialogs(client, settings["group_id"])
        if group_chat:
            await message.answer(
                f"Готово! Аккаунт подключён к группе «{group_chat.title}».\n\n"
                f"Теперь /check будет работать корректно."
            )
        else:
            await message.answer(
                "Вступил в группу, но она ещё не появилась в диалогах.\n"
                "Подождите минуту и попробуйте /check."
            )
    except Exception as e:
        err = str(e).lower()
        if "already" in err or "user_already_participant" in err:
            await message.answer(
                "Аккаунт уже в группе, но она не найдена в диалогах.\n"
                "Попробуйте /check — возможно всё уже работает."
            )
        else:
            await message.answer(
                f"Ошибка: {e}\n\n"
                f"Убедитесь, что бот является администратором группы."
            )


# ----- DEBUG -----

@dp.message(Command("debug_session"))
async def debug_session(message: types.Message):
    settings = get_settings(message.from_user.id)
    if not settings:
        await message.answer("Настройки не найдены. Выполните /login")
        return
    ss = settings.get("session_string")
    await message.answer(
        f"Диагностика:\n\n"
        f"Тип session_string: {type(ss).__name__}\n"
        f"Длина: {len(ss) if ss else 0}\n"
        f"Начало: {str(ss)[:60] if ss else 'None'}\n\n"
        f"API ID: {settings.get('api_id')}\n"
        f"API HASH: {'***' + str(settings.get('api_hash', ''))[-4:] if settings.get('api_hash') else 'нет'}\n"
        f"Канал: {settings.get('channel_id') or 'не настроен'}\n"
        f"Группа: {settings.get('group_id') or 'не настроена'}"
    )


@dp.message(Command("test_session"))
async def test_session(message: types.Message):
    settings = get_settings(message.from_user.id)
    if not settings or not settings.get("session_string"):
        await message.answer("Сессия отсутствует. Выполните /login")
        return

    await message.answer("Тестирую сессию...")

    if message.from_user.id in pyrogram_clients:
        try:
            await pyrogram_clients[message.from_user.id].disconnect()
        except Exception:
            pass
        del pyrogram_clients[message.from_user.id]

    client = await get_pyrogram_client(message.from_user.id)
    if client:
        try:
            me = await client.get_me()
            await message.answer(
                f"Сессия рабочая!\n\n"
                f"Пользователь: {me.first_name} {me.last_name or ''}\n"
                f"Username: @{me.username or '—'}\n"
                f"ID: {me.id}"
            )
        except Exception as e:
            await message.answer(f"Клиент запустился, но get_me() упал: {e}")
    else:
        await message.answer("Сессия не работает. Попробуйте /reset_session и /login заново")


@dp.message(Command("reset_session"))
async def reset_session(message: types.Message):
    if message.from_user.id in pyrogram_clients:
        try:
            await pyrogram_clients[message.from_user.id].disconnect()
        except Exception:
            pass
        del pyrogram_clients[message.from_user.id]
    clear_session(message.from_user.id)
    await message.answer("Сессия сброшена. Выполните /login")


# ----- SETUP -----

@dp.message(Command("setup"))
async def cmd_setup(message: types.Message, state: FSMContext):
    settings = get_settings(message.from_user.id)
    if not settings or not settings.get("session_string"):
        await message.answer("Сначала выполните /login")
        return
    await state.clear()
    await message.answer(
        "НАСТРОЙКА\n\n"
        "Шаг 1/2: Введите ID канала\n"
        "(вы и бот должны быть администраторами)\n\n"
        "Узнать ID можно через @Getmyid_bot\n\n"
        "Пример: -1001234567890\n\n"
        "/cancel — отмена"
    )
    await state.set_state(SetupState.waiting_for_channel_link)


@dp.message(SetupState.waiting_for_channel_link)
async def process_channel_link(message: types.Message, state: FSMContext):
    if message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return

    raw = message.text.strip()
    try:
        channel_id = int(raw)
    except ValueError:
        await message.answer(
            "ID должен быть числом, например: -1001234567890\n\n"
            "Узнать ID канала: @Getmyid_bot"
        )
        return

    try:
        chat = await bot.get_chat(channel_id)
    except Exception as e:
        await message.answer(
            f"Не удалось найти канал с ID {channel_id}\n"
            f"Ошибка: {e}\n\n"
            f"Убедитесь, что бот добавлен в канал как администратор"
        )
        return

    try:
        bot_member = await bot.get_chat_member(chat.id, (await bot.get_me()).id)
        if bot_member.status not in ["administrator", "creator"]:
            await message.answer(
                f"Бот не является администратором канала «{chat.title}»\n"
                f"Добавьте бота как админа и попробуйте снова"
            )
            return
    except Exception:
        await message.answer("Не удалось проверить права бота — убедитесь, что бот добавлен в канал")
        return

    try:
        user_member = await bot.get_chat_member(chat.id, message.from_user.id)
        if user_member.status not in ["administrator", "creator"]:
            await message.answer(f"Вы не являетесь администратором канала «{chat.title}»")
            return
    except Exception:
        await message.answer("Не удалось проверить ваши права в канале")
        return

    save_settings(message.from_user.id, channel_id=chat.id)
    await state.set_state(SetupState.waiting_for_group_link)
    await message.answer(
        f"Канал принят: {chat.title}\n"
        f"ID: {chat.id}\n\n"
        f"Шаг 2/2: Введите ID группы обсуждения\n\n"
        f"Узнать ID можно через @Getmyid_bot\n\n"
        f"Пример: -1009876543210\n\n"
        f"/cancel — отмена"
    )


@dp.message(SetupState.waiting_for_group_link)
async def process_group_link(message: types.Message, state: FSMContext):
    if message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("Отменено")
        return

    raw = message.text.strip()
    try:
        group_id = int(raw)
    except ValueError:
        await message.answer(
            "ID должен быть числом, например: -1009876543210\n\n"
            "Узнать ID группы: @Getmyid_bot"
        )
        return

    try:
        chat = await bot.get_chat(group_id)
    except Exception as e:
        await message.answer(
            f"Не удалось найти группу с ID {group_id}\n"
            f"Ошибка: {e}\n\n"
            f"Убедитесь, что бот добавлен в группу как администратор"
        )
        return

    if chat.type not in ["group", "supergroup"]:
        await message.answer("Это не группа. Введите ID группы (не канала)")
        return

    try:
        bot_member = await bot.get_chat_member(chat.id, (await bot.get_me()).id)
        if bot_member.status not in ["administrator", "creator"]:
            await message.answer(
                f"Бот не является администратором группы «{chat.title}»\n"
                f"Добавьте бота как админа и попробуйте снова"
            )
            return
    except Exception:
        await message.answer("Не удалось проверить права бота — убедитесь, что бот добавлен в группу")
        return

    save_settings(message.from_user.id, group_id=chat.id)
    settings = get_settings(message.from_user.id)

    # Получаем название канала
    try:
        channel_chat = await bot.get_chat(settings['channel_id'])
        channel_title = channel_chat.title
    except Exception:
        channel_title = str(settings['channel_id'])

    await state.clear()
    await message.answer(
        f"Настройки сохранены!\n\n"
        f"Канал: {channel_title}\n"
        f"Канал ID: {settings['channel_id']}\n"
        f"Группа: {chat.title}\n"
        f"Группа ID: {chat.id}\n\n"
    )

# ----- MYSETTINGS -----

@dp.message(Command("mysettings"))
async def cmd_mysettings(message: types.Message):
    s = get_settings(message.from_user.id)
    if not s:
        await message.answer("Настройки не найдены. Выполните /login и /setup")
        return
    await message.answer(
        f"Ваши настройки:\n\n"
        f"Канал: {s['channel_id'] or 'не настроен'}\n"
        f"Группа: {s['group_id'] or 'не настроена'}\n"
        f"API ID: {s['api_id'] or 'не указан'}\n"
        f"Авторизация: {'выполнена' if s['session_string'] else 'не выполнена'}"
    )


# ----- CHECK -----

@dp.message(Command("check"))
async def cmd_check(message: types.Message):
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
            "Использование:\n/check ссылка_на_пост часы\n\n"
            "Примеры:\n"
            "/check https://t.me/c/1234567890/456 24\n"
            "/check https://t.me/mychannel/2 0.25"
        )
        return

    post_url = args[1]
    try:
        hours = float(args[2])
    except ValueError:
        await message.answer("Часы должны быть числом (например: 24 или 0.5)")
        return

    channel_id_or_username, post_id = parse_post_url(post_url)
    if channel_id_or_username is None or post_id is None:
        await message.answer("Неверный формат ссылки на пост")
        return

    actual_channel_id = channel_id_or_username
    if isinstance(actual_channel_id, str):
        try:
            chat = await bot.get_chat(f"@{actual_channel_id}")
            actual_channel_id = chat.id
        except Exception as e:
            await message.answer(f"Не удалось определить ID канала: {e}")
            return

    if actual_channel_id != settings["channel_id"]:
        await message.answer(
            f"Пост не из вашего канала.\n"
            f"Ваш канал ID: {settings['channel_id']}\n"
            f"ID в ссылке: {actual_channel_id}"
        )
        return

    deadline = asyncio.get_event_loop().time() + hours * 3600

    tasks[post_id] = {
        "deadline": deadline,
        "post_link": post_url,
        "hours": hours,
        "user_id": message.from_user.id,
        "reply_chat_id": message.chat.id,
    }

    conn = get_db_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO active_tasks "
            "(post_id, deadline, post_link, hours, user_id, reply_chat_id) VALUES (?, ?, ?, ?, ?, ?)",
            (post_id, deadline, post_url, hours, message.from_user.id, message.chat.id)
        )
        conn.commit()
    finally:
        conn.close()

    if hours < 1:
        time_str = f"{int(hours * 60)} минут"
    elif hours == int(hours):
        time_str = f"{int(hours)} ч"
    else:
        time_str = f"{hours} ч"

    tz_msk = timezone(timedelta(hours=3))
    finish_time_msk = datetime.fromtimestamp(datetime.now().timestamp() + hours * 3600, tz=tz_msk)
    time_str_finish = finish_time_msk.strftime("%d.%m %H:%M МСК")

    await message.answer(
        f"Задача создана\n\n"
        f"Пост: {post_url}\n"
        f"Ждём: {time_str}\n"
        f"Проверка в: {time_str_finish}"
    )

    asyncio.create_task(
        process_post(post_id, deadline, message.chat.id, post_url, message.from_user.id)
    )


# ----- STATUS / CANCEL -----

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    user_tasks = {
        pid: d for pid, d in tasks.items()
        if d.get("user_id") == message.from_user.id
    }
    if not user_tasks:
        await message.answer("Нет активных задач")
        return

    lines = []
    now = asyncio.get_event_loop().time()
    for pid, d in user_tasks.items():
        rem = max(0, int(d["deadline"] - now))
        h, m = rem // 3600, (rem % 3600) // 60
        lines.append(f"Пост {pid}: осталось {h}ч {m}м")
    await message.answer("Активные задачи:\n\n" + "\n".join(lines))


@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("Действие отменено")
        return

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
        _delete_task(post_id)
        await message.answer(f"Задача для поста {post_id} отменена")
    else:
        await message.answer(f"Задача {post_id} не найдена")


# ========== КОЛБЭКИ ==========

@dp.callback_query(lambda c: c.data.startswith("edit_"))
async def handle_edit(callback: types.CallbackQuery):
    temp_id = callback.data[5:]
    if temp_id not in pending_cleanups:
        await callback.answer("Данные устарели", show_alert=True)
        return
    pending_cleanups[temp_id]["editing"] = True
    await callback.message.edit_text(
        f"РЕДАКТИРОВАНИЕ СПИСКА\n\n"
        f"Сейчас в списке: {pending_cleanups[temp_id]['total_count']} человек\n\n"
        f"Отправьте ID пользователей, которых нужно исключить из удаления\n"
        f"(через запятую или каждый с новой строки)\n\n"
        f"Пример: 123456789, 987654321\n\n"
        f"/cancel_edit — отмена"
    )
    await callback.answer()


@dp.message(Command("cancel_edit"))
async def cancel_edit(message: types.Message):
    for temp_id, data in pending_cleanups.items():
        if data.get("editing") and data.get("user_id") == message.from_user.id:
            data["editing"] = False
            await message.answer("Редактирование отменено")
            return
    await message.answer("Нет активного редактирования")


@dp.message(lambda m: m.text and not m.text.startswith("/"))
async def process_exclude_list(message: types.Message):
    """Обрабатывает ввод ID для исключения из списка удаления."""
    active_temp_id = None
    active_data = None
    for temp_id, data in pending_cleanups.items():
        if data.get("editing") and data.get("user_id") == message.from_user.id:
            active_temp_id = temp_id
            active_data = data
            break
    if not active_data:
        return

    raw_ids = re.split(r'[\s,;\n]+', message.text.strip())
    exclude_ids = {int(x) for x in raw_ids if x.strip().lstrip('-').isdigit()}

    if not exclude_ids:
        await message.answer("Не найдено корректных ID. Попробуйте ещё раз:")
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
            uname = f" (@{user.username})" if getattr(user, "username", None) else ""
            user_lines.append(f"{uid}{uname}")
        except Exception:
            user_lines.append(str(uid))

    list_text = "\n".join(user_lines)
    if len(new_ids) > 30:
        list_text += f"\n\n... и ещё {len(new_ids) - 30}"

    excluded_count = len(original_ids) - len(new_ids)
    confirm_text = (
        f"ПОДТВЕРЖДЕНИЕ\n\n"
        f"Пост: {active_data['post_link']}\n"
        f"К удалению: {len(new_ids)} (исключено: {excluded_count})\n\n"
        f"Список:\n{list_text}\n\n"
        f"Удалить из канала?"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="ДА",            callback_data=f"confirm_yes_{active_temp_id}"),
        InlineKeyboardButton(text="ИЗМЕНИТЬ ЕЩЁ", callback_data=f"edit_{active_temp_id}"),
        InlineKeyboardButton(text="НЕТ",           callback_data=f"confirm_no_{active_temp_id}"),
    ]])
    await bot.send_message(message.chat.id, confirm_text, reply_markup=keyboard)
    await message.delete()


@dp.callback_query(lambda c: c.data.startswith("confirm_yes_"))
async def handle_confirm_yes(callback: types.CallbackQuery):
    temp_id = callback.data[12:]
    if temp_id not in pending_cleanups:
        await callback.answer("Данные устарели", show_alert=True)
        return

    data = pending_cleanups[temp_id]
    if data["total_count"] == 0:
        await callback.message.edit_text("Список пуст, некого удалять")
        del pending_cleanups[temp_id]
        await callback.answer()
        return

    await callback.message.edit_text("Удаляю из канала...")
    await callback.answer()

    client = await get_pyrogram_client(data["user_id"])
    if not client:
        await callback.message.edit_text(
            "Ошибка: сессия недоступна. Выполните /reset_session и /login заново"
        )
        del pending_cleanups[temp_id]
        return

    settings = get_settings(data["user_id"])

    # Конвертируем channel_id для Pyrogram
    pyrogram_channel_id = to_pyrogram_channel_id(settings["channel_id"])

    async def update_progress(current, total):
        try:
            await callback.message.edit_text(
                f"Удаляю... {current}/{total} ({current * 100 // total}%)"
            )
        except Exception:
            pass

    result = await kick_from_channel(
        client, pyrogram_channel_id, data["user_ids_list"], update_progress
    )

    # Кикаем из группы обсуждения через бота (Bot API, здесь -100 формат нужен)
    kicked_group = 0
    if settings.get("group_id"):
        for uid in data["user_ids_list"]:
            try:
                await bot.ban_chat_member(settings["group_id"], uid)
                await asyncio.sleep(0.1)
                await bot.unban_chat_member(settings["group_id"], uid)
                kicked_group += 1
            except Exception:
                pass

    await callback.message.edit_text(
        f"ГОТОВО\n\n"
        f"Пост: {data['post_link']}\n"
        f"Не отметилось: {data['total_count']}\n"
        f"Удалено из канала: {result['success']}\n"
        f"Удалено из группы: {kicked_group}\n"
        f"Ошибок: {result['errors']}"
    )
    del pending_cleanups[temp_id]


@dp.callback_query(lambda c: c.data.startswith("confirm_no_"))
async def handle_confirm_no(callback: types.CallbackQuery):
    temp_id = callback.data[11:]
    if temp_id in pending_cleanups:
        del pending_cleanups[temp_id]
    await callback.message.edit_text("Удаление из канала отменено")
    await callback.answer()


# ========== ОСНОВНАЯ ЛОГИКА ==========

async def process_post(post_id: int, deadline: float, reply_chat_id: int,
                       post_link: str, user_id: int):
    print(f"[Task] Запущена задача для поста {post_id}")

    wait_seconds = deadline - asyncio.get_event_loop().time()
    if wait_seconds > 0:
        print(f"[Task] Ждём {wait_seconds:.0f} сек для поста {post_id}")
        await asyncio.sleep(wait_seconds)

    tasks.pop(post_id, None)
    _delete_task(post_id)

    settings = get_settings(user_id)
    if not settings:
        await bot.send_message(reply_chat_id, "Ошибка: настройки не найдены")
        return

    await bot.send_message(reply_chat_id, f"Проверяю пост {post_link}...")

    client = await get_pyrogram_client(user_id)
    if not client:
        await bot.send_message(
            reply_chat_id,
            "Ошибка: сессия недоступна.\nВыполните /reset_session и /login заново"
        )
        return

    pyrogram_channel_id = to_pyrogram_channel_id(settings["channel_id"])

    # Резолвим peer — Pyrogram должен "знать" канал перед работой с ним
    try:
        await client.get_chat(pyrogram_channel_id)
        print(f"[Task] Peer {pyrogram_channel_id} успешно резолвнут")
    except Exception as e:
        print(f"[Task] Не удалось резолвнуть через ID ({e}), пробуем вступить по инвайт-ссылке...")
        try:
            invite_link = await bot.export_chat_invite_link(settings["channel_id"])
            await client.join_chat(invite_link)
            print(f"[Task] Вступил в канал через инвайт-ссылку")
            await asyncio.sleep(2)
        except Exception as e2:
            err = str(e2).lower()
            if "already" not in err and "user_already_participant" not in err:
                await bot.send_message(
                    reply_chat_id,
                    f"Ошибка: не удалось получить доступ к каналу.\n{e2}\n\n"
                    f"Попробуйте выполнить /join_group вручную."
                )
                return
            print(f"[Task] Уже в канале, продолжаем")

    # Собираем комментаторов
    commenters = await get_commenters(client, pyrogram_channel_id, post_id)
    await bot.send_message(reply_chat_id, f"Комментаторов: {len(commenters)}")

    # Собираем подписчиков канала
    members = set()
    try:
        async for member in client.get_chat_members(pyrogram_channel_id):
            if member.user and not member.user.is_bot:
                members.add(member.user.id)
    except Exception as e:
        await bot.send_message(
            reply_chat_id,
            f"Ошибка сбора подписчиков канала: {e}\n\n"
            f"Попробуйте выполнить /join_group и повторить /check"
        )
        return

    await bot.send_message(reply_chat_id, f"Подписчиков канала: {len(members)}")

    to_kick = list(members - commenters)

    if not to_kick:
        await bot.send_message(reply_chat_id, f"Все отметились под постом {post_link}")
        return

    user_lines = []
    for uid in to_kick:
        try:
            user = await bot.get_chat(uid)
            uname = f" (@{user.username})" if getattr(user, "username", None) else ""
            user_lines.append(f"{uid}{uname}")
        except Exception:
            user_lines.append(str(uid))

    CHUNK = 80
    chunks = [user_lines[i:i + CHUNK] for i in range(0, len(user_lines), CHUNK)]

    header = (
        f"НЕОТМЕТИВШИЕСЯ\n\n"
        f"Пост: {post_link}\n"
        f"Всего: {len(to_kick)}\n\n"
    )
    for idx, chunk in enumerate(chunks):
        part_text = (header if idx == 0 else f"(продолжение {idx + 1})\n\n")
        part_text += "\n".join(chunk)
        await bot.send_message(reply_chat_id, part_text)

    temp_id = f"{post_id}_{int(asyncio.get_event_loop().time())}"
    pending_cleanups[temp_id] = {
        "post_id": post_id,
        "post_link": post_link,
        "total_count": len(to_kick),
        "user_ids_list": to_kick,
        "user_id": user_id,
    }

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="ДА",              callback_data=f"confirm_yes_{temp_id}"),
        InlineKeyboardButton(text="ИЗМЕНИТЬ СПИСОК", callback_data=f"edit_{temp_id}"),
        InlineKeyboardButton(text="НЕТ",             callback_data=f"confirm_no_{temp_id}"),
    ]])

    await bot.send_message(
        reply_chat_id,
        "Удалить всех из канала?",
        reply_markup=keyboard
    )


def _cleanup_session(token: str):
    """Отключает клиент и удаляет сессию авторизации."""
    session = auth_sessions.pop(token, None)
    if session and session.get('client'):
        client = session['client']
        try:
            if client.is_connected:
                asyncio.run_coroutine_threadsafe(client.disconnect(), bot_loop)
        except Exception:
            pass


# ========== FLASK ==========

AUTH_PAGE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Авторизация</title>
<style>
  body{font-family:sans-serif;max-width:420px;margin:60px auto;padding:0 20px;background:#f5f5f5}
  .card{background:#fff;border-radius:12px;padding:30px;box-shadow:0 2px 12px rgba(0,0,0,.1)}
  h2{margin:0 0 8px;font-size:1.3em}
  p{color:#555;margin:0 0 20px;font-size:.95em;line-height:1.5}
  input{width:100%;box-sizing:border-box;padding:12px;font-size:1em;border:1.5px solid #ddd;border-radius:8px;margin-bottom:14px}
  input:focus{outline:none;border-color:#2196f3}
  button{width:100%;padding:13px;font-size:1em;background:#2196f3;color:#fff;border:none;border-radius:8px;cursor:pointer}
  button:hover{background:#1976d2}
  .msg{margin-top:16px;padding:12px;border-radius:8px;font-size:.95em}
  .ok{background:#e8f5e9;color:#2e7d32}
  .err{background:#ffebee;color:#c62828}
</style>
</head>
<body>
<div class="card">
  <h2>Введите код из Telegram</h2>
  <p>Telegram прислал вам код подтверждения.<br>Введите его ниже. Если есть пароль 2FA — он тоже появится.</p>
  <input id="code" type="text" inputmode="numeric" placeholder="Например: 12345" maxlength="10" autofocus>
  <div id="pass_block" style="display:none">
    <p style="margin:0 0 8px">Введите пароль двухфакторной аутентификации:</p>
    <input id="password" type="password" placeholder="Пароль 2FA">
  </div>
  <button onclick="submit()">Войти</button>
  <div id="msg"></div>
</div>
<script>
const token = new URLSearchParams(location.search).get('token');
async function submit(){
  const code = document.getElementById('code').value.trim();
  const password = document.getElementById('password').value.trim();
  document.getElementById('msg').innerHTML = '<div class="msg">Проверяю...</div>';
  const r = await fetch('/auth/verify', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({token, code, password})
  });
  const d = await r.json();
  if(d.ok){
    document.getElementById('msg').innerHTML = '<div class="msg ok">' + d.message + '</div>';
  } else if(d.need_password){
    document.getElementById('pass_block').style.display = 'block';
    document.getElementById('msg').innerHTML = '<div class="msg err">Требуется пароль 2FA</div>';
  } else {
    document.getElementById('msg').innerHTML = '<div class="msg err">' + d.message + '</div>';
  }
}
document.addEventListener('keydown', e => { if(e.key==='Enter') submit(); });
</script>
</body>
</html>"""


@app.route('/')
@app.route('/health')
def health():
    return jsonify({"status": "ok", "tasks": len(tasks)})


@app.route('/auth')
def auth_page():
    token = request.args.get('token', '')
    if not token or token not in auth_sessions:
        return "<h2>Ссылка недействительна или устарела</h2>", 400
    session = auth_sessions[token]
    if time.time() > session['expire']:
        del auth_sessions[token]
        return "<h2>Ссылка устарела. Начните /login заново</h2>", 400
    return AUTH_PAGE


@app.route('/auth/verify', methods=['POST'])
def auth_verify():
    data     = request.get_json()
    token    = data.get('token', '')
    code     = data.get('code', '').strip()
    password = data.get('password', '').strip()

    if not token or token not in auth_sessions:
        return jsonify(ok=False, message="Ссылка недействительна")

    session = auth_sessions[token]
    if time.time() > session['expire']:
        auth_sessions.pop(token, None)
        return jsonify(ok=False, message="Ссылка устарела. Начните /login заново")

    phone    = session['phone']
    pch      = session['phone_code_hash']
    user_id  = session['user_id']
    api_id   = session['api_id']
    api_hash = session['api_hash']

    if bot_loop is None:
        return jsonify(ok=False, message="Сервер ещё не готов, попробуйте через 5 секунд")

    async def do_auth():
        client = session['client']
        try:
            if not password:
                try:
                    await client.sign_in(phone, pch, code)
                except Exception as e:
                    if "session_password_needed" in str(e).lower():
                        return {"need_password": True}
                    raise
            else:
                await client.check_password(password)

            session_string = str(await client.export_session_string())
            save_settings(user_id, api_id=api_id, api_hash=api_hash, session_string=session_string)
            auth_sessions.pop(token, None)
            try:
                await client.disconnect()
            except Exception:
                pass

            await bot.send_message(
                user_id,
                "Авторизация успешна!\n\n"
                "Следующий шаг:\n"
                "/setup — настройте канал и группу\n"
            )
            return {"ok": True}

        except Exception as e:
            err_lower = str(e).lower()
            if "session_password_needed" in err_lower:
                return {"need_password": True}
            raise

    future = asyncio.run_coroutine_threadsafe(do_auth(), bot_loop)
    try:
        result = future.result(timeout=30)
    except TimeoutError:
        return jsonify(ok=False, message="Таймаут. Попробуйте ещё раз.")
    except Exception as e:
        err = str(e).lower()
        if "phone_code_invalid" in err:
            return jsonify(ok=False, message="Неверный код. Попробуйте ещё раз.")
        if "phone_code_expired" in err:
            _cleanup_session(token)
            return jsonify(ok=False, message="Код истёк. Начните /login заново.")
        if "password" in err and "invalid" in err:
            return jsonify(ok=False, message="Неверный пароль 2FA.")
        _cleanup_session(token)
        return jsonify(ok=False, message=f"Ошибка: {e}")

    if result.get("need_password"):
        return jsonify(ok=False, need_password=True, message="Требуется пароль 2FA")

    return jsonify(ok=True, message="Авторизация успешна! Вернитесь в Telegram.")


def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)


async def self_ping():
    while True:
        await asyncio.sleep(60)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{RENDER_URL}/health",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        print("[Ping] OK")
        except Exception as e:
            print(f"[Ping] Ошибка: {e}")


async def main():
    global bot_loop
    bot_loop = asyncio.get_running_loop()
    print("Бот запускается...")
    restore_tasks_from_db()
    asyncio.create_task(self_ping())
    print("Бот запущен, начинаю polling")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    print("Flask запущен")
    asyncio.run(main())
