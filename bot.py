import asyncio
import time
import uuid
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
#  КОНФИГ — впишите свои значения прямо здесь
#  ВНИМАНИЕ: держите репозиторий ПРИВАТНЫМ!
# ============================================================
TELEGRAM_TOKEN    = "8947786647:AAHhruAgEoUPtqpW9BinunsQg4qMaEmGBa0"
GIGACHAT_AUTH_KEY = "MDFhMGIzNzctZTBjNS03YjhmLWI2ZGYtMzI5MWJkZjYxOTU3OjE5N2NlMTRmLWYxYmItNDA0Ni04ODllLWNiNmVlYzE5YTI2NQ=="   # Base64 из Sber Studio
GIGACHAT_SCOPE    = "GIGACHAT_API_PERS"

# Как часто обновлять токен (в секундах)
TOKEN_REFRESH_INTERVAL = 30 * 60      # обновляем каждые 30 минут
TOKEN_STALE_AFTER      = 25 * 60      # считаем токен устаревшим через 25 минут

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
    """Запрашивает новый access_token у GigaChat (Basic-авторизация)."""
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
    """Возвращает актуальный токен, обновляя его при необходимости."""
    async with _token_lock:
        age = time.time() - _token["issued_at"]
        if not _token["value"] or age > TOKEN_STALE_AFTER:
            await refresh_token()
        return _token["value"]


async def token_refresher_loop():
    """Фоновая задача: обновляет токен каждые 30 минут."""
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

    # если токен внезапно протух — принудительно обновляем и повторяем один раз
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
    await update.message.chat.send_action("typing")
    try:
        answer = await ask_gigachat(update.message.text)
        await update.message.reply_text(answer)
    except httpx.HTTPStatusError as e:
        await update.message.reply_text(
            f"GigaChat {e.response.status_code}:\n{e.response.text[:300]}"
        )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def on_startup(app):
    app.bot_data["token_task"] = asyncio.create_task(token_refresher_loop())
    print("Фоновая задача обновления токена запущена")


async def on_shutdown(app):
    task = app.bot_data.get("token_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


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
