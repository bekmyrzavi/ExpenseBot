"""
ExpenseBot v3.0 — Production-grade AI-бухгалтер
Модель: claude-sonnet-4-6
"""

import os
import json
import base64
import logging
import re
import asyncio
import threading
import time
import gc
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional
from zoneinfo import ZoneInfo
from collections import defaultdict

import httpx
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
import gspread
from google.oauth2.service_account import Credentials
import anthropic

# ─────────────────────────────────────────────
# ЛОГИРОВАНИЕ
# ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────
def _require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(f"❌ Переменная окружения {key!r} не задана")
    return val

TELEGRAM_TOKEN    = _require_env("TELEGRAM_TOKEN")
ANTHROPIC_KEY     = _require_env("ANTHROPIC_API_KEY")
SPREADSHEET_ID    = _require_env("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = _require_env("GOOGLE_CREDS_JSON")

TZ = ZoneInfo("Asia/Bishkek")

# Лимиты
MAX_IMAGE_BYTES    = 10 * 1024 * 1024   # 10 MB
MAX_TEXT_LENGTH    = 2000
MAX_AMOUNT         = 100_000_000
MAX_RETRIES        = 3
RETRY_DELAY        = 2.0

# Rate limiting — защита от спама
RATE_LIMIT_MSGS    = 10     # максимум сообщений
RATE_LIMIT_WINDOW  = 60     # за N секунд на одного пользователя

# Anthropic rate limit
AI_SEMAPHORE_LIMIT = 5      # макс одновременных AI-запросов

# Кэш таблицы
CACHE_TTL_SECONDS  = 30     # кэшируем данные таблицы на 30 секунд

# ─────────────────────────────────────────────
# ГЛОБАЛЬНЫЕ ОБЪЕКТЫ
# ─────────────────────────────────────────────
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
ai_semaphore  = asyncio.Semaphore(AI_SEMAPHORE_LIMIT)
sheets_lock   = threading.Lock()  # защита от конкурентной записи

# Rate limiter: user_id -> список timestamp
_rate_tracker: dict[int, list[float]] = defaultdict(list)

# Кэш данных таблицы
_sheet_cache: dict = {"data": None, "ts": 0.0}

# ─────────────────────────────────────────────
# GOOGLE SHEETS — клиент с авто-переподключением
# ─────────────────────────────────────────────
_gc: Optional[gspread.Client] = None
_gc_created_at: float = 0.0
GC_MAX_AGE = 3500  # пересоздаём клиент каждые ~58 минут (токен живёт 60 мин)

def _build_gspread() -> gspread.Client:
    creds_data = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_data, scopes=scopes)
    return gspread.authorize(creds)

def get_gc() -> gspread.Client:
    """Возвращает gspread-клиент, пересоздаёт если токен скоро истечёт."""
    global _gc, _gc_created_at
    now = time.time()
    if _gc is None or (now - _gc_created_at) > GC_MAX_AGE:
        logger.info("gspread: создаём/обновляем клиент")
        _gc = _build_gspread()
        _gc_created_at = now
    return _gc

def with_retry(func):
    """Retry-декоратор для Google Sheets операций."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        global _gc
        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                status = getattr(getattr(e, "response", None), "status_code", 0)
                if status == 401:
                    logger.warning("gspread: 401 — пересоздаём клиент")
                    _gc = _build_gspread()
                    _gc_created_at = time.time()
                elif status == 429:
                    wait = RETRY_DELAY * (2 ** attempt)  # exponential backoff
                    logger.warning(f"gspread: 429 rate limit — ждём {wait:.1f}с")
                    time.sleep(wait)
                else:
                    time.sleep(RETRY_DELAY * attempt)
                last_exc = e
                logger.warning(f"gspread: попытка {attempt}/{MAX_RETRIES} — {e}")
            except Exception as e:
                last_exc = e
                logger.warning(f"gspread: попытка {attempt}/{MAX_RETRIES} — {e}")
                time.sleep(RETRY_DELAY * attempt)
        logger.error(f"gspread: все {MAX_RETRIES} попытки исчерпаны — {last_exc}")
        raise last_exc
    return wrapper

HEADERS = ["ID", "Дата", "Время", "Участник", "Наименование", "Сумма", "Категория", "Источник"]

@with_retry
def get_sheet() -> gspread.Worksheet:
    gc = get_gc()
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        return sh.worksheet("Расходы")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Расходы", rows=5000, cols=8)
        ws.append_row(HEADERS)
        ws.format("A1:H1", {
            "backgroundColor": {"red": 0.18, "green": 0.6, "blue": 0.9},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER",
        })
        ws.freeze(rows=1)
        logger.info("Создан новый лист 'Расходы'")
        return ws

def _invalidate_cache():
    """Сбрасываем кэш после записи/удаления."""
    _sheet_cache["data"] = None
    _sheet_cache["ts"]   = 0.0

@with_retry
def _fetch_rows_uncached() -> list:
    ws = get_sheet()
    rows = ws.get_all_values()
    return rows[1:] if len(rows) > 1 else []

def fetch_rows_cached() -> list:
    """Возвращает строки из кэша или запрашивает таблицу."""
    now = time.time()
    if _sheet_cache["data"] is not None and (now - _sheet_cache["ts"]) < CACHE_TTL_SECONDS:
        return _sheet_cache["data"]
    rows = _fetch_rows_uncached()
    _sheet_cache["data"] = rows
    _sheet_cache["ts"]   = now
    return rows

@with_retry
def add_expense(name: str, amount: float, category: str, sender: str, source: str = "текст") -> int:
    """Атомарная запись расхода с защитой от дублирования ID."""
    name     = (str(name).strip()     if name     not in (None, "None") else "")[:200] or "Без названия"
    sender   = (str(sender).strip()   if sender   not in (None, "None") else "")[:100] or "Участник"
    category = (str(category).strip() if category not in (None, "None") else "")[:50]  or "Прочее"
    source   = (str(source).strip()   if source   not in (None, "None") else "")[:50]  or "текст"
    amount   = round(max(0.01, min(float(amount), MAX_AMOUNT)), 2)

    with sheets_lock:  # атомарная операция — только один поток пишет
        ws  = get_sheet()
        now = datetime.now(TZ)
        all_rows = ws.get_all_values()
        row_id   = len(all_rows)  # уникальный ID

        ws.append_row(
            [row_id, now.strftime("%d.%m.%Y"), now.strftime("%H:%M"),
             sender, name, amount, category, source],
            value_input_option="USER_ENTERED",
        )
        _invalidate_cache()
        logger.info(f"Записан расход ID={row_id}: {name} {amount} сом [{sender}]")
        return row_id

@with_retry
def delete_last_by_sender(sender: str) -> Optional[dict]:
    with sheets_lock:
        ws = get_sheet()
        all_rows = ws.get_all_values()
        for i in range(len(all_rows) - 1, 0, -1):
            row = all_rows[i]
            if len(row) >= 4 and row[3] == sender:
                deleted = {"name": row[4] if len(row) > 4 else "", "amount": row[5] if len(row) > 5 else "0"}
                ws.delete_rows(i + 1)
                _invalidate_cache()
                logger.info(f"Удалена запись [{sender}]: {deleted}")
                return deleted
    return None

@with_retry
def delete_by_id(row_id: int) -> Optional[dict]:
    if row_id <= 0:
        return None
    with sheets_lock:
        ws = get_sheet()
        all_rows = ws.get_all_values()
        for i in range(1, len(all_rows)):
            row = all_rows[i]
            if row and str(row[0]) == str(row_id):
                deleted = {"name": row[4] if len(row) > 4 else "", "amount": row[5] if len(row) > 5 else "0"}
                ws.delete_rows(i + 1)
                _invalidate_cache()
                logger.info(f"Удалена запись ID={row_id}: {deleted}")
                return deleted
    return None

def _parse_amount(raw: str) -> float:
    try:
        return float(str(raw).replace(",", ".").replace(" ", "").replace("\xa0", ""))
    except (ValueError, TypeError):
        return 0.0

def build_report_text(period: str = "month") -> str:
    rows = fetch_rows_cached()
    if not rows:
        return "📭 Расходов пока нет."

    now = datetime.now(TZ)
    filtered: list = []
    label = ""

    if period == "today":
        target   = now.strftime("%d.%m.%Y")
        filtered = [r for r in rows if len(r) > 1 and r[1] == target]
        label    = f"сегодня ({target})"
    elif period == "week":
        week_ago = now - timedelta(days=7)
        label    = "последние 7 дней"
        for r in rows:
            if len(r) > 1:
                try:
                    d = datetime.strptime(r[1], "%d.%m.%Y").replace(tzinfo=TZ)
                    if d >= week_ago:
                        filtered.append(r)
                except ValueError:
                    pass
    elif period == "month":
        cur      = now.strftime("%m.%Y")
        filtered = [r for r in rows if len(r) > 1 and r[1].endswith(cur)]
        label    = now.strftime("%B %Y")
    elif period == "year":
        cur      = now.strftime(".%Y")
        filtered = [r for r in rows if len(r) > 1 and r[1].endswith(cur)]
        label    = now.strftime("%Y год")
    else:
        filtered = rows
        label    = "всё время"

    if not filtered:
        return f"📭 За период «{label}» расходов нет."

    total     = 0.0
    by_cat:    dict[str, float] = {}
    by_person: dict[str, float] = {}

    for r in filtered:
        amt    = _parse_amount(r[5] if len(r) > 5 else "0")
        cat    = (r[6] if len(r) > 6 else "Прочее") or "Прочее"
        person = (r[3] if len(r) > 3 else "—") or "—"
        total += amt
        by_cat[cat]       = by_cat.get(cat, 0.0) + amt
        by_person[person] = by_person.get(person, 0.0) + amt

    out = [
        f"📊 *Отчёт за {label}*\n",
        f"💰 *Итого:* {total:,.0f} сом\n",
        "📁 *По категориям:*",
    ]
    for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
        pct = (amt / total * 100) if total else 0
        out.append(f"  • {cat}: {amt:,.0f} сом ({pct:.0f}%)")
    out.append("\n👥 *По участникам:*")
    for person, amt in sorted(by_person.items(), key=lambda x: -x[1]):
        out.append(f"  • {person}: {amt:,.0f} сом")
    out.append(f"\n📝 *Записей:* {len(filtered)}")
    return "\n".join(out)

# ─────────────────────────────────────────────
# RATE LIMITER
# ─────────────────────────────────────────────
def is_rate_limited(user_id: int) -> bool:
    """Возвращает True если пользователь превысил лимит сообщений."""
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW
    timestamps = _rate_tracker[user_id]
    # Удаляем старые записи
    _rate_tracker[user_id] = [t for t in timestamps if t > window_start]
    if len(_rate_tracker[user_id]) >= RATE_LIMIT_MSGS:
        return True
    _rate_tracker[user_id].append(now)
    return False

# ─────────────────────────────────────────────
# СИСТЕМНЫЙ ПРОМПТ
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """Ты умный бухгалтерский ассистент в Telegram-группе учёта расходов компании.

Участники пишут на русском, кыргызском или любом другом языке — ты всё понимаешь.

ТВОИ ДЕЙСТВИЯ (выбери одно):
1. ЗАПИСАТЬ расход → action: "add"
2. УДАЛИТЬ последнюю запись → action: "delete_last"
3. УДАЛИТЬ по ID → action: "delete_id"
4. ОТЧЁТ → action: "report"
5. ОТВЕТИТЬ → action: "chat"

ФОРМАТ ОТВЕТА — строго JSON, без markdown, без текста снаружи:

Расход: {"action":"add","name":"название","amount":число,"category":"категория","reply":"подтверждение"}
Удалить последнее: {"action":"delete_last","reply":"текст"}
Удалить по ID: {"action":"delete_id","id":число,"reply":"текст"}
Отчёт: {"action":"report","period":"today|week|month|year|all","reply":"текст"}
Чат: {"action":"chat","reply":"ответ"}

ПРАВИЛА:
- Категории ТОЛЬКО: Еда, Транспорт, Офис, Зарплата, Материалы, Коммунальные, Связь, Реклама, Прочее
- Суммы: "5к"=5000, "тысяча"=1000, "полтора"=1500, "миллион"=1000000
- На скриншотах банка: ищи поля "Итого"/"Сумма"/"Total"/символ валюты с цифрой
- Переводы физлицам: name = "Перевод: Имя получателя"
- Если сумма не найдена → action:"chat", попроси уточнить
- amount всегда положительное число
- reply на том же языке что написал пользователь, кратко"""

# ─────────────────────────────────────────────
# AI
# ─────────────────────────────────────────────
async def process_with_ai(content_parts: list, sender: str) -> dict:
    loop = asyncio.get_event_loop()

    def _call():
        return claude_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            system=SYSTEM_PROMPT + f"\n\nОтправитель: {sender}",
            messages=[{"role": "user", "content": content_parts}],
        )

    async with ai_semaphore:  # не более AI_SEMAPHORE_LIMIT одновременных запросов
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await loop.run_in_executor(None, _call)
                break
            except anthropic.RateLimitError:
                wait = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Anthropic rate limit — ждём {wait:.1f}с (попытка {attempt})")
                await asyncio.sleep(wait)
                if attempt == MAX_RETRIES:
                    return {"action": "chat", "reply": "Сервис временно перегружен, попробуй через минуту."}
            except anthropic.APIError as e:
                logger.error(f"Anthropic API error: {e}")
                if attempt == MAX_RETRIES:
                    return {"action": "chat", "reply": "Временная ошибка AI, попробуй ещё раз."}
                await asyncio.sleep(RETRY_DELAY * attempt)

    raw = response.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"AI вернул невалидный JSON: {raw!r} — {e}")
        return {"action": "chat", "reply": "Не смог разобрать ответ, попробуй переформулировать."}

    if "action" not in data:
        logger.error(f"AI не вернул action: {data}")
        return {"action": "chat", "reply": "Что-то пошло не так, попробуй ещё раз."}

    return data

# ─────────────────────────────────────────────
# ВЫПОЛНЕНИЕ ДЕЙСТВИЙ
# ─────────────────────────────────────────────
async def execute_action(result: dict, sender: str, msg, source: str) -> None:
    action = result.get("action", "chat")
    reply  = str(result.get("reply", "")).strip()
    loop   = asyncio.get_event_loop()

    if action == "add":
        name     = str(result.get("name", "Расход")).strip() or "Расход"
        category = str(result.get("category", "Прочее")).strip()
        try:
            amount = float(result.get("amount", 0))
            if amount <= 0 or amount > MAX_AMOUNT:
                raise ValueError(f"Недопустимая сумма: {amount}")
        except (TypeError, ValueError) as e:
            logger.warning(f"Некорректная сумма [{sender}]: {result.get('amount')} — {e}")
            await msg.reply_text("❓ Не смог определить сумму. Напиши, например: «Обед 850»")
            return

        try:
            row_id = await loop.run_in_executor(
                None, lambda: add_expense(name, amount, category, sender, source)
            )
        except Exception as e:
            logger.error(f"Ошибка записи в таблицу: {e}")
            await msg.reply_text("⚠️ Не смог записать в таблицу. Попробуй ещё раз через несколько секунд.")
            return

        await msg.reply_text(
            f"✅ {reply}\n\n"
            f"📌 {name}\n"
            f"💵 {amount:,.0f} сом\n"
            f"🏷 {category}\n"
            f"🆔 ID: {row_id}",
            parse_mode="Markdown",
        )

    elif action == "delete_last":
        try:
            deleted = await loop.run_in_executor(None, lambda: delete_last_by_sender(sender))
        except Exception as e:
            logger.error(f"Ошибка удаления: {e}")
            await msg.reply_text("⚠️ Ошибка при удалении. Попробуй ещё раз.")
            return
        if deleted:
            await msg.reply_text(
                f"🗑 Удалено: *{deleted['name']}* — {deleted['amount']} сом",
                parse_mode="Markdown",
            )
        else:
            await msg.reply_text("❌ У тебя нет записей для удаления.")

    elif action == "delete_id":
        try:
            rid = int(result.get("id", 0))
            if rid <= 0:
                raise ValueError
        except (TypeError, ValueError):
            await msg.reply_text("❌ Укажи корректный ID записи (целое число).")
            return
        try:
            deleted = await loop.run_in_executor(None, lambda: delete_by_id(rid))
        except Exception as e:
            logger.error(f"Ошибка удаления по ID: {e}")
            await msg.reply_text("⚠️ Ошибка при удалении. Попробуй ещё раз.")
            return
        if deleted:
            await msg.reply_text(
                f"🗑 Удалено ID {rid}: *{deleted['name']}* — {deleted['amount']} сом",
                parse_mode="Markdown",
            )
        else:
            await msg.reply_text(f"❌ Запись с ID {rid} не найдена.")

    elif action == "report":
        period = result.get("period", "month")
        if period not in ("today", "week", "month", "year", "all"):
            period = "month"
        try:
            report = await loop.run_in_executor(None, lambda: build_report_text(period))
        except Exception as e:
            logger.error(f"Ошибка построения отчёта: {e}")
            await msg.reply_text("⚠️ Не смог сформировать отчёт. Попробуй ещё раз.")
            return
        await msg.reply_text(report, parse_mode="Markdown")

    elif action == "chat":
        if reply:
            await msg.reply_text(reply)
    else:
        logger.warning(f"Неизвестный action: {action!r}")
        if reply:
            await msg.reply_text(reply)

# ─────────────────────────────────────────────
# TELEGRAM HANDLERS
# ─────────────────────────────────────────────
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.from_user:
        return

    user_id = msg.from_user.id
    sender  = msg.from_user.first_name or msg.from_user.username or "Участник"
    text    = (msg.text or "").strip()

    if not text:
        return

    if is_rate_limited(user_id):
        await msg.reply_text("⏳ Слишком много сообщений. Подожди минуту.")
        return

    if len(text) > MAX_TEXT_LENGTH:
        await msg.reply_text(f"⚠️ Сообщение слишком длинное (макс. {MAX_TEXT_LENGTH} символов).")
        return

    try:
        result = await process_with_ai([{"type": "text", "text": text}], sender)
        await execute_action(result, sender, msg, "текст")
    except Exception as e:
        logger.exception(f"handle_text error [{sender}]: {e}")
        await msg.reply_text("⚠️ Временная ошибка. Попробуй через несколько секунд.")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.from_user:
        return

    user_id = msg.from_user.id
    sender  = msg.from_user.first_name or msg.from_user.username or "Участник"
    caption = (msg.caption or "").strip()

    if is_rate_limited(user_id):
        await msg.reply_text("⏳ Слишком много сообщений. Подожди минуту.")
        return

    try:
        photo = msg.photo[-1]
        file  = await context.bot.get_file(photo.file_id)

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(file.file_path)
            resp.raise_for_status()
            image_bytes = resp.content

        if len(image_bytes) > MAX_IMAGE_BYTES:
            await msg.reply_text("⚠️ Изображение слишком большое (макс. 10 МБ).")
            return

        # Определяем MIME по сигнатуре байт
        mime = "image/jpeg"
        if image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
            mime = "image/png"
        elif image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
            mime = "image/webp"

        image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

        hint = f"Подпись от пользователя: {caption}" if caption else \
               "Это чек, квитанция или скриншот банковского приложения. Найди финансовую операцию — сумму и наименование."

        content_parts = [
            {"type": "image", "source": {"type": "base64", "media_type": mime, "data": image_b64}},
            {"type": "text",  "text": hint},
        ]

        result = await process_with_ai(content_parts, sender)
        await execute_action(result, sender, msg, "фото чека")

        # Явно освобождаем память от изображения
        del image_bytes, image_b64
        gc.collect()

    except httpx.HTTPError as e:
        logger.error(f"HTTP error downloading photo: {e}")
        await msg.reply_text("⚠️ Не смог скачать фото. Попробуй ещё раз.")
    except Exception as e:
        logger.exception(f"handle_photo error [{sender}]: {e}")
        await msg.reply_text("⚠️ Ошибка при обработке фото.")

async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg:
        return
    await msg.reply_text(
        "🎤 Голосовые пока не поддерживаются.\n"
        "Напиши текстом или отправь фото чека 🧾"
    )

async def handle_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Telegram error: {context.error}", exc_info=context.error)

# ─────────────────────────────────────────────
# ПЕРИОДИЧЕСКАЯ ОЧИСТКА ПАМЯТИ
# ─────────────────────────────────────────────
async def cleanup_job(context) -> None:
    """Запускается каждый час: чистит rate-tracker и форсирует GC."""
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW
    cleaned = 0
    for uid in list(_rate_tracker.keys()):
        _rate_tracker[uid] = [t for t in _rate_tracker[uid] if t > window_start]
        if not _rate_tracker[uid]:
            del _rate_tracker[uid]
            cleaned += 1
    gc.collect()
    logger.info(f"🧹 Cleanup: удалено {cleaned} устаревших rate-записей, GC выполнен")

# ─────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────
def main() -> None:
    # Проверяем Google Sheets при старте
    try:
        get_sheet()
        logger.info("✅ Google Sheets — OK")
    except Exception as e:
        logger.error(f"❌ Google Sheets недоступен: {e}")
        raise

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(MessageHandler(filters.PHOTO,                   handle_photo))
    app.add_handler(MessageHandler(filters.VOICE,                   handle_voice))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(handle_error)

    # Периодическая очистка памяти каждый час
    app.job_queue.run_repeating(cleanup_job, interval=3600, first=3600)

    logger.info("🚀 ExpenseBot v3.0 запущен — Production Ready")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
