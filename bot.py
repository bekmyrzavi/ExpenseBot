"""
ExpenseBot v4.0 — Production AI-бухгалтер
- Решительный: действует сразу, не переспрашивает
- Контекст диалога: помнит последние 3 сообщения
- Голос: транскрипция через Whisper (OpenAI)
- Экономия API: короткие сообщения фильтруются
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
import tempfile
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional
from zoneinfo import ZoneInfo
from collections import defaultdict, deque

import httpx
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
import gspread
from google.oauth2.service_account import Credentials
import anthropic

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
OPENAI_KEY        = os.environ.get("OPENAI_API_KEY", "")  # опционально для голоса

TZ = ZoneInfo("Asia/Bishkek")

MAX_IMAGE_BYTES   = 10 * 1024 * 1024
MAX_TEXT_LENGTH   = 1000
MAX_AMOUNT        = 100_000_000
MAX_RETRIES       = 3
RETRY_DELAY       = 2.0
RATE_LIMIT_MSGS   = 15
RATE_LIMIT_WINDOW = 60
AI_SEMAPHORE      = 5
CACHE_TTL         = 30
GC_MAX_AGE        = 3500

# Контекст диалога: последние N сообщений на чат
CONTEXT_SIZE = 4

# ─────────────────────────────────────────────
# ГЛОБАЛЬНЫЕ ОБЪЕКТЫ
# ─────────────────────────────────────────────
claude_client  = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
ai_semaphore   = asyncio.Semaphore(AI_SEMAPHORE)
sheets_lock    = threading.Lock()
_rate_tracker: dict[int, list[float]] = defaultdict(list)
_sheet_cache: dict = {"data": None, "ts": 0.0}

# Контекст диалога: chat_id -> deque of (role, text)
_dialog_ctx: dict[int, deque] = defaultdict(lambda: deque(maxlen=CONTEXT_SIZE))

# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────
_gc: Optional[gspread.Client] = None
_gc_created_at: float = 0.0

def _build_gspread() -> gspread.Client:
    creds_data = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_data, scopes=scopes)
    return gspread.authorize(creds)

def get_gc() -> gspread.Client:
    global _gc, _gc_created_at
    now = time.time()
    if _gc is None or (now - _gc_created_at) > GC_MAX_AGE:
        _gc = _build_gspread()
        _gc_created_at = now
    return _gc

def with_retry(func):
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
                    _gc = _build_gspread()
                    _gc_created_at = time.time()
                wait = RETRY_DELAY * (2 ** attempt) if status == 429 else RETRY_DELAY * attempt
                logger.warning(f"gspread {status}: попытка {attempt}/{MAX_RETRIES}, ждём {wait:.1f}с")
                time.sleep(wait)
                last_exc = e
            except Exception as e:
                last_exc = e
                time.sleep(RETRY_DELAY * attempt)
        raise last_exc
    return wrapper

@with_retry
def get_sheet() -> gspread.Worksheet:
    gc = get_gc()
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        return sh.worksheet("Расходы")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Расходы", rows=5000, cols=8)
        ws.append_row(["ID", "Дата", "Время", "Участник", "Наименование", "Сумма", "Категория", "Источник"])
        ws.format("A1:H1", {
            "backgroundColor": {"red": 0.18, "green": 0.6, "blue": 0.9},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER",
        })
        ws.freeze(rows=1)
        return ws

def _invalidate_cache():
    _sheet_cache["data"] = None
    _sheet_cache["ts"] = 0.0

@with_retry
def _fetch_rows_raw() -> list:
    ws = get_sheet()
    rows = ws.get_all_values()
    return rows[1:] if len(rows) > 1 else []

def fetch_rows() -> list:
    now = time.time()
    if _sheet_cache["data"] is not None and (now - _sheet_cache["ts"]) < CACHE_TTL:
        return _sheet_cache["data"]
    rows = _fetch_rows_raw()
    _sheet_cache["data"] = rows
    _sheet_cache["ts"] = now
    return rows

@with_retry
def add_expense(name: str, amount: float, category: str, sender: str, source: str = "текст") -> int:
    name     = (str(name).strip()     if name     not in (None, "None") else "")[:200] or "Без названия"
    sender   = (str(sender).strip()   if sender   not in (None, "None") else "")[:100] or "Участник"
    category = (str(category).strip() if category not in (None, "None") else "")[:50]  or "Прочее"
    source   = (str(source).strip()   if source   not in (None, "None") else "")[:50]  or "текст"
    amount   = round(max(0.01, min(float(amount), MAX_AMOUNT)), 2)
    with sheets_lock:
        ws = get_sheet()
        now = datetime.now(TZ)
        all_rows = ws.get_all_values()
        row_id = len(all_rows)
        ws.append_row(
            [row_id, now.strftime("%d.%m.%Y"), now.strftime("%H:%M"),
             sender, name, amount, category, source],
            value_input_option="USER_ENTERED",
        )
        _invalidate_cache()
        logger.info(f"✅ ID={row_id}: {name} {amount}с [{sender}]")
        return row_id

@with_retry
def delete_last_by_sender(sender: str) -> Optional[dict]:
    with sheets_lock:
        ws = get_sheet()
        rows = ws.get_all_values()
        for i in range(len(rows) - 1, 0, -1):
            row = rows[i]
            if len(row) >= 4 and row[3] == sender:
                deleted = {"name": row[4] if len(row) > 4 else "", "amount": row[5] if len(row) > 5 else "0"}
                ws.delete_rows(i + 1)
                _invalidate_cache()
                return deleted
    return None

@with_retry
def delete_by_id(row_id: int) -> Optional[dict]:
    if row_id <= 0:
        return None
    with sheets_lock:
        ws = get_sheet()
        rows = ws.get_all_values()
        for i in range(1, len(rows)):
            row = rows[i]
            if row and str(row[0]) == str(row_id):
                deleted = {"name": row[4] if len(row) > 4 else "", "amount": row[5] if len(row) > 5 else "0"}
                ws.delete_rows(i + 1)
                _invalidate_cache()
                return deleted
    return None

def _parse_amount(raw: str) -> float:
    try:
        return float(str(raw).replace(",", ".").replace(" ", "").replace("\xa0", ""))
    except:
        return 0.0

def build_report(period: str = "month") -> str:
    rows = fetch_rows()
    if not rows:
        return "📭 Расходов пока нет."
    now = datetime.now(TZ)
    filtered, label = [], ""
    if period == "today":
        t = now.strftime("%d.%m.%Y")
        filtered = [r for r in rows if len(r) > 1 and r[1] == t]
        label = f"сегодня ({t})"
    elif period == "week":
        ago = now - timedelta(days=7)
        label = "последние 7 дней"
        for r in rows:
            if len(r) > 1:
                try:
                    if datetime.strptime(r[1], "%d.%m.%Y").replace(tzinfo=TZ) >= ago:
                        filtered.append(r)
                except ValueError:
                    pass
    elif period == "month":
        cur = now.strftime("%m.%Y")
        filtered = [r for r in rows if len(r) > 1 and r[1].endswith(cur)]
        label = now.strftime("%B %Y")
    elif period == "year":
        cur = now.strftime(".%Y")
        filtered = [r for r in rows if len(r) > 1 and r[1].endswith(cur)]
        label = now.strftime("%Y год")
    else:
        filtered, label = rows, "всё время"

    if not filtered:
        return f"📭 За «{label}» расходов нет."

    total, by_cat, by_person = 0.0, {}, {}
    for r in filtered:
        amt    = _parse_amount(r[5] if len(r) > 5 else "0")
        cat    = (r[6] if len(r) > 6 else "Прочее") or "Прочее"
        person = (r[3] if len(r) > 3 else "—") or "—"
        total += amt
        by_cat[cat]       = by_cat.get(cat, 0.0) + amt
        by_person[person] = by_person.get(person, 0.0) + amt

    out = [f"📊 *Отчёт за {label}*\n", f"💰 *Итого:* {total:,.0f} сом\n", "📁 *По категориям:*"]
    for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
        out.append(f"  • {cat}: {amt:,.0f} сом ({amt/total*100:.0f}%)")
    out.append("\n👥 *По участникам:*")
    for p, amt in sorted(by_person.items(), key=lambda x: -x[1]):
        out.append(f"  • {p}: {amt:,.0f} сом")
    out.append(f"\n📝 *Записей:* {len(filtered)}")
    return "\n".join(out)

# ─────────────────────────────────────────────
# RATE LIMITER
# ─────────────────────────────────────────────
def is_rate_limited(user_id: int) -> bool:
    now = time.time()
    ws = now - RATE_LIMIT_WINDOW
    _rate_tracker[user_id] = [t for t in _rate_tracker[user_id] if t > ws]
    if len(_rate_tracker[user_id]) >= RATE_LIMIT_MSGS:
        return True
    _rate_tracker[user_id].append(now)
    return False

# ─────────────────────────────────────────────
# СИСТЕМНЫЙ ПРОМПТ — решительный, без лишних вопросов
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """Ты бухгалтерский бот в Telegram-группе. Отвечаешь на русском или кыргызском.

ГЛАВНОЕ ПРАВИЛО: ДЕЙСТВУЙ СРАЗУ. Не переспрашивай без крайней необходимости.

ЛОГИКА ПРИНЯТИЯ РЕШЕНИЙ:

1. Если есть сумма И название → action:"add", записывай немедленно
2. Если есть только сумма (без названия) → action:"add", name="Расход", записывай
3. Если есть только название (без суммы) → action:"chat", спроси только сумму одним словом
4. "удали", "отмени", "убери" → action:"delete_last" немедленно
5. "удали ID N" или "удали N" → action:"delete_id", id:N немедленно  
6. "удали суши/обед/[название]" → action:"delete_last" (удаляй последнюю, не уточняй)
7. "отчёт", "сколько", "итого", "расходы" → action:"report"
8. Просто "да", "ок", "хорошо" после вопроса о записи → action:"add" с тем что обсуждалось
9. Одиночное число (200, 500, 1000) без контекста → action:"add", name="Расход", amount=число

НА СКРИНШОТАХ БАНКА:
- Ищи: "Итого", "Сумма", "Total", символ валюты с числом, минус перед суммой
- Получатель платежа = название расхода  
- Перевод физлицу → name:"Перевод: [имя]"
- Даже если изображение нечёткое — попробуй найти сумму

КАТЕГОРИИ: Еда, Транспорт, Офис, Зарплата, Материалы, Коммунальные, Связь, Реклама, Прочее
СУММЫ: 5к=5000, тысяча=1000, полтора=1500, млн=1000000

ФОРМАТ ОТВЕТА — только JSON:
{"action":"add","name":"...","amount":число,"category":"...","reply":"короткое подтверждение"}
{"action":"delete_last","reply":"..."}
{"action":"delete_id","id":число,"reply":"..."}
{"action":"report","period":"today|week|month|year|all","reply":"..."}
{"action":"chat","reply":"короткий вопрос или ответ"}

ЗАПРЕЩЕНО: переспрашивать категорию, переспрашивать подтверждение записи, задавать несколько вопросов."""

# ─────────────────────────────────────────────
# AI — с контекстом диалога
# ─────────────────────────────────────────────
async def process_with_ai(content_parts: list, sender: str, chat_id: int) -> dict:
    loop = asyncio.get_event_loop()

    # Строим историю диалога для контекста
    ctx = list(_dialog_ctx[chat_id])
    history_text = ""
    if ctx:
        history_text = "\n\nПоследние сообщения в чате:\n"
        for role, text in ctx:
            prefix = f"{sender}" if role == "user" else "Бот"
            history_text += f"{prefix}: {text}\n"

    system = SYSTEM_PROMPT + f"\n\nОтправитель: {sender}" + history_text

    def _call():
        return claude_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,  # снизили с 512 до 300 — экономия API
            system=system,
            messages=[{"role": "user", "content": content_parts}],
        )

    async with ai_semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await loop.run_in_executor(None, _call)
                break
            except anthropic.RateLimitError:
                wait = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Anthropic rate limit, ждём {wait:.1f}с")
                await asyncio.sleep(wait)
                if attempt == MAX_RETRIES:
                    return {"action": "chat", "reply": "Перегружен, попробуй через минуту."}
            except anthropic.APIError as e:
                logger.error(f"Anthropic error: {e}")
                if attempt == MAX_RETRIES:
                    return {"action": "chat", "reply": "Временная ошибка, попробуй ещё раз."}
                await asyncio.sleep(RETRY_DELAY * attempt)

    raw = response.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.error(f"Невалидный JSON от AI: {raw!r}")
        return {"action": "chat", "reply": "Не понял, попробуй ещё раз."}

    if "action" not in data:
        return {"action": "chat", "reply": "Что-то пошло не так."}

    return data

# ─────────────────────────────────────────────
# ГОЛОС — транскрипция через Whisper
# ─────────────────────────────────────────────
async def transcribe_voice(ogg_bytes: bytes) -> Optional[str]:
    """Транскрибирует голосовое через OpenAI Whisper."""
    if not OPENAI_KEY:
        return None
    try:
        import openai
        client = openai.AsyncOpenAI(api_key=OPENAI_KEY)
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
            f.write(ogg_bytes)
            tmp_path = f.name
        with open(tmp_path, "rb") as audio_file:
            result = await client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                language="ru",
            )
        os.unlink(tmp_path)
        return result.text.strip()
    except Exception as e:
        logger.error(f"Whisper error: {e}")
        return None

# ─────────────────────────────────────────────
# ВЫПОЛНЕНИЕ ДЕЙСТВИЙ
# ─────────────────────────────────────────────
async def execute_action(result: dict, sender: str, msg, source: str, chat_id: int) -> None:
    action = result.get("action", "chat")
    reply  = str(result.get("reply", "")).strip()
    loop   = asyncio.get_event_loop()

    # Сохраняем ответ бота в контекст
    if reply:
        _dialog_ctx[chat_id].append(("bot", reply[:200]))

    if action == "add":
        name     = str(result.get("name", "Расход")).strip() or "Расход"
        category = str(result.get("category", "Прочее")).strip() or "Прочее"
        try:
            amount = float(result.get("amount", 0))
            if amount <= 0 or amount > MAX_AMOUNT:
                raise ValueError(f"Некорректная сумма: {amount}")
        except (TypeError, ValueError) as e:
            logger.warning(f"Некорректная сумма [{sender}]: {e}")
            await msg.reply_text("❓ Укажи сумму — например: «Обед 850»")
            return

        try:
            row_id = await loop.run_in_executor(
                None, lambda: add_expense(name, amount, category, sender, source)
            )
        except Exception as e:
            logger.error(f"Ошибка записи: {e}")
            await msg.reply_text("⚠️ Не смог записать. Попробуй ещё раз.")
            return

        await msg.reply_text(
            f"✅ Записано!\n\n"
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
            await msg.reply_text("⚠️ Ошибка при удалении.")
            return
        if deleted:
            await msg.reply_text(f"🗑 Удалено: *{deleted['name']}* — {deleted['amount']} сом", parse_mode="Markdown")
        else:
            await msg.reply_text("❌ Нет твоих записей для удаления.")

    elif action == "delete_id":
        try:
            rid = int(result.get("id", 0))
            if rid <= 0: raise ValueError
        except (TypeError, ValueError):
            await msg.reply_text("❌ Укажи корректный ID числом.")
            return
        try:
            deleted = await loop.run_in_executor(None, lambda: delete_by_id(rid))
        except Exception as e:
            logger.error(f"Ошибка удаления ID: {e}")
            await msg.reply_text("⚠️ Ошибка при удалении.")
            return
        if deleted:
            await msg.reply_text(f"🗑 Удалено ID {rid}: *{deleted['name']}* — {deleted['amount']} сом", parse_mode="Markdown")
        else:
            await msg.reply_text(f"❌ Запись ID {rid} не найдена.")

    elif action == "report":
        period = result.get("period", "month")
        if period not in ("today", "week", "month", "year", "all"):
            period = "month"
        try:
            report = await loop.run_in_executor(None, lambda: build_report(period))
        except Exception as e:
            logger.error(f"Ошибка отчёта: {e}")
            await msg.reply_text("⚠️ Не смог сформировать отчёт.")
            return
        await msg.reply_text(report, parse_mode="Markdown")

    elif action == "chat":
        if reply:
            await msg.reply_text(reply)

    else:
        if reply:
            await msg.reply_text(reply)

# ─────────────────────────────────────────────
# HANDLERS
# ─────────────────────────────────────────────
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.from_user:
        return

    user_id = msg.from_user.id
    chat_id = msg.chat_id
    sender  = msg.from_user.first_name or msg.from_user.username or "Участник"
    text    = (msg.text or "").strip()

    if not text or len(text) > MAX_TEXT_LENGTH:
        return
    if is_rate_limited(user_id):
        await msg.reply_text("⏳ Слишком много сообщений. Подожди минуту.")
        return

    # Сохраняем сообщение пользователя в контекст
    _dialog_ctx[chat_id].append(("user", text[:200]))

    try:
        result = await process_with_ai([{"type": "text", "text": text}], sender, chat_id)
        await execute_action(result, sender, msg, "текст", chat_id)
    except Exception as e:
        logger.exception(f"handle_text error: {e}")
        await msg.reply_text("⚠️ Временная ошибка. Попробуй ещё раз.")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.from_user:
        return

    user_id = msg.from_user.id
    chat_id = msg.chat_id
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
            await msg.reply_text("⚠️ Фото слишком большое (макс. 10 МБ).")
            return

        mime = "image/jpeg"
        if image_bytes[:8] == b"\x89PNG\r\n\x1a\n":
            mime = "image/png"
        elif image_bytes[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
            mime = "image/webp"

        image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")
        hint = f"Подпись: {caption}" if caption else \
               "Чек, квитанция или скриншот банковского приложения. Найди сумму и получателя/назначение."

        _dialog_ctx[chat_id].append(("user", f"[фото] {caption}" if caption else "[фото чека]"))

        content_parts = [
            {"type": "image", "source": {"type": "base64", "media_type": mime, "data": image_b64}},
            {"type": "text",  "text": hint},
        ]
        result = await process_with_ai(content_parts, sender, chat_id)
        await execute_action(result, sender, msg, "фото чека", chat_id)

        del image_bytes, image_b64
        gc.collect()

    except httpx.HTTPError as e:
        logger.error(f"HTTP error: {e}")
        await msg.reply_text("⚠️ Не смог скачать фото.")
    except Exception as e:
        logger.exception(f"handle_photo error: {e}")
        await msg.reply_text("⚠️ Ошибка при обработке фото.")


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.from_user:
        return

    user_id = msg.from_user.id
    chat_id = msg.chat_id
    sender  = msg.from_user.first_name or msg.from_user.username or "Участник"

    if is_rate_limited(user_id):
        await msg.reply_text("⏳ Слишком много сообщений.")
        return

    if not OPENAI_KEY:
        await msg.reply_text(
            "🎤 Голосовые не настроены.\n"
            "Добавь OPENAI_API_KEY в Railway → Variables для включения."
        )
        return

    await msg.reply_text("🎤 Слушаю...")

    try:
        file = await context.bot.get_file(msg.voice.file_id)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(file.file_path)
            ogg_bytes = resp.content

        transcript = await transcribe_voice(ogg_bytes)

        if not transcript:
            await msg.reply_text("❓ Не смог распознать голос. Напиши текстом.")
            return

        logger.info(f"Голос [{sender}]: {transcript!r}")
        _dialog_ctx[chat_id].append(("user", f"[голос] {transcript[:200]}"))

        await msg.reply_text(f"🗣 _{transcript}_", parse_mode="Markdown")

        result = await process_with_ai([{"type": "text", "text": transcript}], sender, chat_id)
        await execute_action(result, sender, msg, "голосовое", chat_id)

    except Exception as e:
        logger.exception(f"handle_voice error: {e}")
        await msg.reply_text("⚠️ Ошибка при обработке голосового.")


async def handle_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Telegram error: {context.error}", exc_info=context.error)


# ─────────────────────────────────────────────
# ПЕРИОДИЧЕСКАЯ ОЧИСТКА
# ─────────────────────────────────────────────
async def cleanup_job(context) -> None:
    now = time.time()
    ws  = now - RATE_LIMIT_WINDOW
    cleaned_rate = sum(1 for uid in list(_rate_tracker) if not _rate_tracker[uid])
    for uid in list(_rate_tracker):
        _rate_tracker[uid] = [t for t in _rate_tracker[uid] if t > ws]
        if not _rate_tracker[uid]:
            del _rate_tracker[uid]
    gc.collect()
    logger.info(f"🧹 Cleanup: rate_tracker очищен, GC выполнен")


# ─────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────
def main() -> None:
    try:
        get_sheet()
        logger.info("✅ Google Sheets — OK")
    except Exception as e:
        logger.error(f"❌ Google Sheets недоступен: {e}")
        raise

    if OPENAI_KEY:
        logger.info("✅ OpenAI Whisper — голосовые включены")
    else:
        logger.info("⚠️  OPENAI_API_KEY не задан — голосовые отключены")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(MessageHandler(filters.PHOTO,                   handle_photo))
    app.add_handler(MessageHandler(filters.VOICE,                   handle_voice))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(handle_error)
    app.job_queue.run_repeating(cleanup_job, interval=3600, first=3600)

    logger.info("🚀 ExpenseBot v4.0 — решительный AI, контекст диалога, голос")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
