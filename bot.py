import asyncio
import os
import time
import uuid
from urllib.parse import urlparse, unquote

import aiomysql
import httpx
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ============================================================
#  КОНФИГ — берётся из переменных окружения (GitHub Secrets)
# ============================================================
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
GIGACHAT_AUTH_KEY = os.environ["GIGACHAT_AUTH_KEY"]
GIGACHAT_SCOPE    = os.environ.get("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")

# DSN вида mysql://user:pass@host:port/dbname
DATABASE_URL = os.environ["DATABASE_URL"]


def parse_dsn(dsn: str) -> dict:
    """Разбирает mysql://user:pass@host:port/dbname в параметры для aiomysql."""
    parsed = urlparse(dsn)
    if parsed.scheme not in ("mysql", "mysql+aiomysql"):
        raise ValueError(f"Неподдерживаемая схема DSN: {parsed.scheme!r}")

    if not parsed.hostname:
        raise ValueError("В DSN не указан host")
    if not parsed.path or parsed.path == "/":
        raise ValueError("В DSN не указано имя базы данных")

    return {
        "host":     parsed.hostname,
        "port":     parsed.port or 3306,
        "user":     unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "db":       parsed.path.lstrip("/"),
        "charset":  "utf8mb4",
        "autocommit": True,
    }


DB_CONFIG = parse_dsn(DATABASE_URL)

# Как часто обновлять токен GigaChat
TOKEN_REFRESH_INTERVAL = 30 * 60
TOKEN_STALE_AFTER      = 25 * 60

GIGACHAT_OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
GIGACHAT_API_URL   = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

# ============================================================
#  Глобальное состояние токена
# ============================================================
_token_lock = asyncio.Lock()
_token = {
    "value": None,
    "issued_at": 0.0,
}


async def refresh_token() -> str:
    headers = {
        "Authorization": f"Basic {GIGACHAT_AUTH_KEY}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "RqUID": str(uuid.uuid4()),
    }
    data = {"scope": GIGACHAT_SCOPE}

    async with httpx.AsyncClient(verify=False, timeout=30) as client:
        resp = await client.post(GIGACHAT_OAUTH_URL, headers=headers, data=data)
        resp.raise_for_status()
        payload = resp.json()

    _token["value"] = payload["access_token"]
    _token["issued_at"] = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] GigaChat token обновлён")
    return _token["value"]


async def get_token() -> str:
    async with _token_lock:
        age = time.time() - _token["issued_at"]
        if not _token["value"] or age > TOKEN_STALE_AFTER:
            await refresh_token()
        return _token["value"]


async def token_refresher_loop():
    try:
        async with _token_lock:
            await refresh_token()
    except Exception as e:
        print(f"Не удалось получить токен при старте: {e}")

    while True:
        await asyncio.sleep(TOKEN_REFRESH_INTERVAL)
        try:
            async with _token_lock:
                await refresh_token()
        except Exception as e:
            print(f"Ошибка обновления токена: {e}")


# ============================================================
#  База данных
# ============================================================
_db_pool: aiomysql.Pool | None = None


async def init_db_pool():
    """Создаёт пул соединений и таблицу messages, если её нет."""
    global _db_pool
    _db_pool = await aiomysql.create_pool(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        db=DB_CONFIG["db"],
        charset=DB_CONFIG["charset"],
        autocommit=True,
        minsize=1,
        maxsize=5,
    )

    async with _db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id           INT AUTO_INCREMENT PRIMARY KEY,
                    user_id      BIGINT       NOT NULL,
                    username     VARCHAR(255) NULL,
                    first_name   VARCHAR(255) NULL,
                    last_name    VARCHAR(255) NULL,
                    chat_id      BIGINT       NULL,
                    chat_title   VARCHAR(255) NULL,
                    message_text MEDIUMTEXT   NULL,
                    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user (user_id),
                    INDEX idx_created (created_at)
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            """)
    print(f"Пул БД готов: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}")


async def close_db_pool():
    global _db_pool
    if _db_pool:
        _db_pool.close()
        await _db_pool.wait_closed()
        print("Пул БД закрыт")


async def save_message(
    user_id: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    chat_id: int,
    chat_title: str | None,
    message_text: str | None,
):
    if _db_pool is None:
        print("Пул БД не инициализирован, пропускаю запись")
        return
    try:
        async with _db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO messages
                        (user_id, username, first_name, last_name,
                         chat_id, chat_title, message_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        username,
                        first_name,
                        last_name,
                        chat_id,
                        chat_title,
                        message_text,
                    ),
                )
    except Exception as e:
        print(f"Ошибка записи в БД: {e}")


# ============================================================
#  GigaChat API
# ============================================================
async def ask_gigachat(prompt: str) -> str:
    async def _do_request(token: str):
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        body = {
            "model": "GigaChat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
        }
        async with httpx.AsyncClient(verify=False, timeout=60) as client:
            return await client.post(GIGACHAT_API_URL, headers=headers, json=body)

    token = await get_token()
    resp = await _do_request(token)

    if resp.status_code == 401:
        async with _token_lock:
            await refresh_token()
            token = _token["value"]
        resp = await _do_request(token)

    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


# ============================================================
#  Telegram handlers
# ============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Я бот на GigaChat. Напиши мне что-нибудь.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user = msg.from_user

    asyncio.create_task(
        save_message(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            chat_id=msg.chat_id,
            chat_title=msg.chat.title,
            message_text=msg.text,
        )
    )

    await msg.chat.send_action("typing")
    try:
        answer = await ask_gigachat(msg.text)
        await msg.reply_text(answer)
    except httpx.HTTPStatusError as e:
        await msg.reply_text(
            f"GigaChat {e.response.status_code}:\n{e.response.text[:300]}"
        )
    except Exception as e:
        await msg.reply_text(f"Ошибка: {e}")


# ============================================================
#  Startup / Shutdown
# ============================================================
async def on_startup(app):
    app.bot_data["token_task"] = asyncio.create_task(token_refresher_loop())
    print("Фоновая задача обновления токена запущена")

    try:
        await init_db_pool()
    except Exception as e:
        print(f"Не удалось подключиться к БД: {e}")


async def on_shutdown(app):
    task = app.bot_data.get("token_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await close_db_pool()


def main():
    app = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()
