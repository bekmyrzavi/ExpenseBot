import os
import json
import base64
import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import httpx
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes
)
import gspread
from google.oauth2.service_account import Credentials
import anthropic

logging.basicConfig(
    format="%(asctime)s │ %(levelname)s │ %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
ANTHROPIC_KEY     = os.environ["ANTHROPIC_API_KEY"]
SPREADSHEET_ID    = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"]

claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)


def get_sheet():
    creds_data = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_data, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet("Расходы")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Расходы", rows=1000, cols=8)
        ws.append_row(["ID", "Дата", "Время", "Участник", "Наименование", "Сумма", "Категория", "Источник"])
        ws.format("A1:H1", {
            "backgroundColor": {"red": 0.18, "green": 0.6, "blue": 0.9},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER"
        })
    return ws


async def analyze_text(text: str, sender: str) -> Optional[dict]:
    prompt = f"""Ты бухгалтерский ассистент. Из сообщения участника группы извлеки данные о расходе.

Сообщение от {sender}: "{text}"

Если это расход — верни JSON:
{{"name": "название расхода", "amount": числовая сумма, "category": "Еда|Транспорт|Офис|Зарплата|Материалы|Коммунальные|Связь|Реклама|Прочее"}}

Если это НЕ расход (разговор, вопрос) — верни: {{"is_expense": false}}

Сумму ищи в тексте (2500 сом, 2.5к, тысяча=1000). Только JSON."""

    response = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = re.sub(r"```json|```", "", response.content[0].text.strip()).strip()
    data = json.loads(raw)
    if data.get("is_expense") is False or "amount" not in data:
        return None
    data["source"] = "текст"
    return data


async def analyze_image(image_bytes: bytes, mime_type: str, sender: str) -> Optional[dict]:
    image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")
    response = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=500,
        messages=[{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": mime_type, "data": image_b64}},
            {"type": "text", "text": f"""Чек от {sender}. Извлеки данные.
Верни JSON: {{"name": "что куплено", "amount": итого числом, "category": "Еда|Транспорт|Офис|Зарплата|Материалы|Коммунальные|Связь|Реклама|Прочее"}}
Если не чек: {{"is_expense": false}}. Только JSON."""}
        ]}]
    )
    raw = re.sub(r"```json|```", "", response.content[0].text.strip()).strip()
    data = json.loads(raw)
    if data.get("is_expense") is False or "amount" not in data:
        return None
    data["source"] = "фото чека"
    return data


async def analyze_voice_text(transcribed: str, sender: str) -> Optional[dict]:
    return await analyze_text(transcribed, sender)


def add_expense_to_sheet(expense: dict, sender: str) -> int:
    ws = get_sheet()
    now = datetime.now()
    all_rows = ws.get_all_values()
    row_id = len(all_rows)
    ws.append_row([
        row_id,
        now.strftime("%d.%m.%Y"),
        now.strftime("%H:%M"),
        sender,
        expense.get("name", ""),
        expense.get("amount", 0),
        expense.get("category", "Прочее"),
        expense.get("source", "текст"),
    ])
    return row_id


def delete_last_expense(sender: str) -> Optional[dict]:
    ws = get_sheet()
    all_rows = ws.get_all_values()
    for i in range(len(all_rows) - 1, 0, -1):
        row = all_rows[i]
        if len(row) >= 4 and row[3] == sender:
            deleted = {"name": row[4] if len(row) > 4 else "", "amount": row[5] if len(row) > 5 else ""}
            ws.delete_rows(i + 1)
            return deleted
    return None


def get_report(period: str = "month") -> str:
    ws = get_sheet()
    all_rows = ws.get_all_values()
    if len(all_rows) <= 1:
        return "📭 Расходов пока нет."
    now = datetime.now()
    rows = all_rows[1:]
    filtered = []
    period_label = ""
    if period == "today":
        target = now.strftime("%d.%m.%Y")
        filtered = [r for r in rows if len(r) > 1 and r[1] == target]
        period_label = f"сегодня ({target})"
    elif period == "week":
        week_ago = now - timedelta(days=7)
        period_label = "последние 7 дней"
        for r in rows:
            if len(r) > 1:
                try:
                    if datetime.strptime(r[1], "%d.%m.%Y") >= week_ago:
                        filtered.append(r)
                except:
                    pass
    elif period == "month":
        cur_month = now.strftime("%m.%Y")
        filtered = [r for r in rows if len(r) > 1 and r[1].endswith(cur_month)]
        period_label = now.strftime("%B %Y")
    else:
        filtered = rows
        period_label = "всё время"
    if not filtered:
        return f"📭 За период «{period_label}» расходов нет."
    total = 0
    by_category = {}
    by_person = {}
    for r in filtered:
        try:
            amount = float(str(r[5]).replace(",", ".").replace(" ", ""))
        except:
            amount = 0
        cat = r[6] if len(r) > 6 else "Прочее"
        person = r[3] if len(r) > 3 else "—"
        total += amount
        by_category[cat] = by_category.get(cat, 0) + amount
        by_person[person] = by_person.get(person, 0) + amount
    lines = [f"📊 *Отчёт за {period_label}*\n", f"💰 *Итого:* {total:,.0f} сом\n", "📁 *По категориям:*"]
    for cat, amt in sorted(by_category.items(), key=lambda x: -x[1]):
        pct = (amt / total * 100) if total else 0
        lines.append(f"  • {cat}: {amt:,.0f} сом ({pct:.0f}%)")
    lines.append("\n👥 *По участникам:*")
    for person, amt in sorted(by_person.items(), key=lambda x: -x[1]):
        lines.append(f"  • {person}: {amt:,.0f} сом")
    lines.append(f"\n📝 *Записей:* {len(filtered)}")
    return "\n".join(lines)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    text = msg.text or ""
    if text.startswith("/"):
        return
    sender = msg.from_user.first_name or msg.from_user.username or "Участник"
    try:
        expense = await analyze_text(text, sender)
        if expense:
            row_id = add_expense_to_sheet(expense, sender)
            await msg.reply_text(
                f"✅ *Записано!*\n📌 {expense['name']}\n💵 {expense['amount']:,.0f} сом\n🏷 {expense['category']}\n🆔 ID: {row_id}",
                parse_mode="Markdown"
            )
    except Exception as e:
        logger.error(f"Text error: {e}")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    sender = msg.from_user.first_name or msg.from_user.username or "Участник"
    await msg.reply_text("🔍 Читаю чек...")
    try:
        photo = msg.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        async with httpx.AsyncClient() as client:
            resp = await client.get(file.file_path)
            image_bytes = resp.content
        expense = await analyze_image(image_bytes, "image/jpeg", sender)
        if expense:
            row_id = add_expense_to_sheet(expense, sender)
            await msg.reply_text(
                f"✅ *Чек распознан!*\n📌 {expense['name']}\n💵 {expense['amount']:,.0f} сом\n🏷 {expense['category']}\n🆔 ID: {row_id}",
                parse_mode="Markdown"
            )
        else:
            await msg.reply_text("❓ Расход в изображении не найден.")
    except Exception as e:
        logger.error(f"Photo error: {e}")
        await msg.reply_text("⚠️ Ошибка при обработке фото.")


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    sender = msg.from_user.first_name or msg.from_user.username or "Участник"
    await msg.reply_text("🎤 Слушаю голосовое...")
    try:
        file = await context.bot.get_file(msg.voice.file_id)
        async with httpx.AsyncClient() as client:
            resp = await client.get(file.file_path)
            ogg_bytes = resp.content
        # Транскрипция через Whisper-подобный подход через Telegram file → OpenAI или просто скажем что нет поддержки
        # Пока используем заглушку с уведомлением
        await msg.reply_text(
            "🎤 Голосовые сообщения: напиши текстом или отправь фото чека.\n"
            "_(Голосовая транскрипция требует Whisper API — см. инструкцию)_",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Voice error: {e}")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Бот учёта расходов активен!*\n\n"
        "Отправляй:\n• Текст: `Обед 850`\n• Фото чека 🧾\n\n"
        "/otchet — отчёт за месяц
/otchet segodnya
/otchet nedelya
/otchet vse
/udalit — удалить последнюю запись
/pomosh",
        parse_mode="Markdown"
    )


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    period = "month"
    if args:
        a = args[0].lower()
        if a in ["сегодня", "today", "segodnya"]: period = "today"
        elif a in ["неделя", "week", "nedelya"]: period = "week"
        elif a in ["всё", "все", "all", "всего", "vse"]: period = "all"
    await update.message.reply_text("⏳ Формирую отчёт...")
    await update.message.reply_text(get_report(period), parse_mode="Markdown")


async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sender = update.message.from_user.first_name or update.message.from_user.username or "Участник"
    deleted = delete_last_expense(sender)
    if deleted:
        await update.message.reply_text(f"🗑 Удалено: *{deleted['name']}* — {deleted['amount']} сом", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Нет записей для удаления.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Команды бота:*\n\n"
        "`/otchet` — текущий месяц\n"
        "`/otchet segodnya` — за сегодня\n"
        "`/otchet nedelya` — за 7 дней\n"
        "`/otchet vse` — за всё время\n"
        "`/udalit` — удалить мою последнюю запись\n\n"
        "*Добавление:* просто пиши текст или кидай фото чека.",
        parse_mode="Markdown"
    )


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("otchet",   cmd_report))
    app.add_handler(CommandHandler("report",  cmd_report))
    app.add_handler(CommandHandler("udalit", cmd_delete))
    app.add_handler(CommandHandler("delete",  cmd_delete))
    app.add_handler(CommandHandler("pomosh",  cmd_help))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    logger.info("🚀 Бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
