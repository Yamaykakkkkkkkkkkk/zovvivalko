"""Telegram-бот периодических упоминаний участников группы + мини Flask-сервер.

Запуск:
    BOT_TOKEN=... PORT=8080 python main.py

Всё состояние (таймеры и известные участники) хранится только в памяти процесса.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import os
import re
import threading

from flask import Flask

from telegram import Update, User
from telegram.constants import ChatMemberStatus, ChatType, ParseMode
from telegram.error import RetryAfter, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ExtBot,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

log = logging.getLogger("group-call")

CHUNK_SIZE = 5        # упоминаний в одном сообщении
SEND_DELAY = 1.5      # пауза между сообщениями одного цикла, сек
MIN_INTERVAL = 1      # минимальный интервал цикла, сек (rate limits учитывает SEND_DELAY)
MAX_INTERVAL = 24 * 60 * 60

INTERVAL_RE = re.compile(r"(\d+)([smh])")

USAGE_TEXT = (
    "Использование: /startcall <число><s|m|h>\n"
    "Примеры: /startcall 30s, /startcall 5m, /startcall 2h\n"
    "Интервал от 1 секунды до 24 часов."
)

START_TEXT = (
    "Привет! Я периодически упоминаю участников этой группы.\n\n"
    "/startcall <интервал> — запустить цикл, например /startcall 10m\n"
    "/stopcall — остановить цикл в этой группе\n"
    "/call — один цикл прямо сейчас\n\n"
    "Интервал задаётся как число + s/m/h (30s, 5m, 2h).\n\n"
    "Важно: Telegram Bot API не позволяет получить полный список "
    "участников группы, поэтому я упоминаю тех, кого реально видел "
    "в чате (сообщения, вход/выход и т.п.).\n"
    "Управление доступно только администраторам группы."
)

NO_MEMBERS_TEXT = (
    "Я ещё никого не знаю в этой группе.\n"
    "Telegram Bot API не отдаёт полный список участников, поэтому бот "
    "запоминает тех, кто пишет в чат. Напишите пару сообщений и повторите."
)

# chat_id -> {"interval": секунды, "task": asyncio.Task | None}
timers: dict[int, dict] = {}
# ИЗВЕСТНЫЕ пользователи, а не полный состав группы: known_members[chat_id][user_id] = User
# (у User есть id, username, first_name). Bot API полный roster не отдаёт.
known_members: dict[int, dict[int, User]] = {}

flask_app = Flask(__name__)


@flask_app.route("/")
def index() -> str:
    return "Hello World"


def parse_interval(raw: str) -> int | None:
    match = INTERVAL_RE.fullmatch(raw.strip().lower())
    if not match:
        return None
    value = int(match.group(1))
    multiplier = {"s": 1, "m": 60, "h": 3600}[match.group(2)]
    return value * multiplier


def humanize_interval(seconds: int) -> str:
    if seconds % 3600 == 0 and seconds > 60:
        return f"{seconds // 3600} ч"
    if seconds % 60 == 0:
        return f"{seconds // 60} мин"
    return f"{seconds} с"


def get_timer_state(chat_id: int) -> dict:
    return timers.setdefault(chat_id, {"interval": 0, "task": None})


async def cancel_task(task: asyncio.Task | None) -> None:
    if task and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def remember_user(chat_id: int, user: User | None) -> None:
    """Обновляем сведения о пользователе при каждом событии; не ботов не удаляем по таймауту."""
    if user is None or user.is_bot:
        return
    known_members.setdefault(chat_id, {})[user.id] = user


async def ensure_group_admin(update: Update) -> bool:
    """Разрешаем команду только админам группы (включая анонимных админов)."""
    chat = update.effective_chat
    message = update.effective_message

    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.reply_text("Эта команда работает только в группах.")
        return False

    # Анонимный админ пишет от имени самого чата.
    if message.sender_chat is not None and message.sender_chat.id == chat.id:
        return True

    user = update.effective_user
    if user is None:
        await message.reply_text("Команда доступна только администраторам группы.")
        return False

    try:
        member = await chat.get_member(user.id)
    except TelegramError as error:
        log.warning("Не удалось проверить права в %s: %s", chat.id, error)
        await message.reply_text("Не удалось проверить права. Попробуйте позже.")
        return False

    if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
        await message.reply_text("Команда доступна только администраторам группы.")
        return False
    return True


def format_mention(user: User) -> str:
    if user.username:
        return f"@{user.username}"
    name = user.full_name or str(user.id)
    return f'<a href="tg://user?id={user.id}">{html.escape(name)}</a>'


async def send_chunks(bot: ExtBot, chat_id: int, chunks: list, total: int) -> None:
    for index, chunk in enumerate(chunks):
        start = index * CHUNK_SIZE + 1
        end = start + len(chunk) - 1
        header = (
            f"📣 Зову известных участников {start}–{end} из {total}:"
            if len(chunks) > 1
            else f"📣 Зову известных участников ({total}):"
        )
        text = f"{header}\n" + ", ".join(format_mention(user) for user in chunk)

        try:
            await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)
        except RetryAfter as error:
            delay = getattr(error, "retry_after", 5)
            log.warning("FloodWait %ss в чате %s", delay, chat_id)
            await asyncio.sleep(delay + 1)
            try:
                await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)
            except TelegramError as retry_error:
                log.warning("Повтор не удался (чат %s): %s", chat_id, retry_error)
                break
        except TelegramError as error:
            log.warning("Отправка не удалась (чат %s): %s", chat_id, error)
            break

        if index < len(chunks) - 1:
            await asyncio.sleep(SEND_DELAY)


async def run_cycle(bot: ExtBot, chat_id: int) -> None:
    # Берём только известных боту пользователей этого chat_id.
    users = list(known_members.get(chat_id, {}).values())
    if not users:
        await bot.send_message(chat_id, NO_MEMBERS_TEXT)
        return

    users.sort(key=lambda user: (user.full_name or "").lower())
    chunks = [users[i : i + CHUNK_SIZE] for i in range(0, len(users), CHUNK_SIZE)]
    await send_chunks(bot, chat_id, chunks, total=len(users))


def report_task_error(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception():
        log.error("Фоновая задача упала", exc_info=task.exception())


async def timer_loop(bot: ExtBot, chat_id: int) -> None:
    log.info("Таймер запущен в чате %s", chat_id)
    try:
        while True:
            try:
                await run_cycle(bot, chat_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Ошибка цикла в чате %s", chat_id)
            await asyncio.sleep(timers.get(chat_id, {}).get("interval", MAX_INTERVAL))
    finally:
        log.info("Таймер остановлен в чате %s", chat_id)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(START_TEXT)


async def cmd_startcall(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    message = update.effective_message

    if not await ensure_group_admin(update):
        return

    interval = parse_interval(context.args[0]) if len(context.args) == 1 else None
    if interval is None or not MIN_INTERVAL <= interval <= MAX_INTERVAL:
        await message.reply_text(USAGE_TEXT)
        return

    state = get_timer_state(chat.id)
    state["interval"] = interval
    old_task, state["task"] = state["task"], None
    await cancel_task(old_task)
    state["task"] = asyncio.create_task(timer_loop(context.bot, chat.id))

    known_count = len(known_members.get(chat.id, {}))
    try:
        # Официальный метод Bot API: только ЧИСЛО участников, не их список.
        total_count = await context.bot.get_chat_member_count(chat.id)
        total_note = f" Всего в группе по данным Telegram: {total_count}."
    except TelegramError:
        total_note = ""

    suffix = "" if known_count else "\nИзвестных пока нет — напишите что-нибудь в чат, чтобы бот вас запомнил."
    await message.reply_text(
        f"Цикл запущен: упоминания каждые {humanize_interval(interval)}. "
        f"Известных участников: {known_count}.{total_note}{suffix}"
    )


async def cmd_stopcall(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_group_admin(update):
        return

    state = timers.pop(update.effective_chat.id, None)
    was_running = bool(state and state["task"] and not state["task"].done())
    await cancel_task(state["task"]) if state else None

    await update.effective_message.reply_text(
        "Цикл остановлен." if was_running else "Активного цикла в этой группе нет."
    )


async def cmd_call(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_group_admin(update):
        return

    await update.effective_message.reply_text("Запускаю один цикл упоминаний…")
    task = asyncio.create_task(run_cycle(context.bot, update.effective_chat.id))
    task.add_done_callback(report_task_error)


async def migrate_chat_state(context: ContextTypes.DEFAULT_TYPE, old_id: int, new_id: int) -> None:
    """При превращении группы в супергруппу Telegram меняет chat_id — переносим состояние."""
    if old_id == new_id:
        return
    log.info("Чат %s мигрировал в %s, переношу состояние", old_id, new_id)

    if old_id in known_members:
        known_members[new_id] = known_members.pop(old_id)

    state = timers.pop(old_id, None)
    if state is None:
        return

    was_running = bool(state["task"] and not state["task"].done())
    await cancel_task(state["task"])
    timers[new_id] = state
    if was_running:
        state["task"] = asyncio.create_task(timer_loop(context.bot, new_id))


async def track_members(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bot API не умеет перечислять участников — собираем всех, кто виден в апдейтах.

    Пополняем реестр при каждом событии (from_user, new_chat_members, pinned,
    forward), удаляем только реально покинувших чат. По таймауту никого не вычищаем.
    """
    chat = update.effective_chat
    message = update.effective_message

    # Группа стала супергруппой: у чата новый id, состояние нужно перенести.
    if message.migrate_to_chat_id:
        await migrate_chat_state(context, chat.id, message.migrate_to_chat_id)
        return

    remember_user(chat.id, message.from_user)
    for user in message.new_chat_members or []:
        remember_user(chat.id, user)
    if message.pinned_message is not None:
        remember_user(chat.id, message.pinned_message.from_user)
    remember_user(chat.id, getattr(message.forward_origin, "sender_user", None))

    left = message.left_chat_member
    if left and not left.is_bot:
        known_members.get(chat.id, {}).pop(left.id, None)


async def on_shutdown(application: Application) -> None:
    for state in timers.values():
        await cancel_task(state["task"])


def main() -> None:
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise SystemExit("Переменная окружения BOT_TOKEN не задана.")
    port = int(os.environ.get("PORT", "8080"))

    application = (
        ApplicationBuilder()
        .token(token)
        .post_shutdown(on_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("startcall", cmd_startcall))
    application.add_handler(CommandHandler("stopcall", cmd_stopcall))
    application.add_handler(CommandHandler("call", cmd_call))
    application.add_handler(MessageHandler(filters.ChatType.GROUPS, track_members))

    threading.Thread(
        target=lambda: flask_app.run(host="0.0.0.0", port=port, use_reloader=False),
        daemon=True,
    ).start()

    # Python >= 3.12/3.14: run_polling ожидает уже существующий event loop.
    asyncio.set_event_loop(asyncio.new_event_loop())

    log.info("Запуск бота, Flask слушает порт %s", port)
    application.run_polling()


if __name__ == "__main__":
    main()
