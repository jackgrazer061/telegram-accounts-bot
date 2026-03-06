import os
import json
import logging
from datetime import datetime
from flask import Flask, request, jsonify
import requests
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# =========================
# ENV
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

SHEET_ACCOUNTS = "База_личек"
SHEET_ISSUES = "Простые лички 26"

LIMIT_OPTIONS = ['-250', '250-500', '500-1200', '1200-1500', 'unlim']
THRESHOLD_OPTIONS = ['0-49', '50-99', '100-199', '200-499', '500+']
GMT_OPTIONS = ['-10', '-9', '-8', '-7', '-6', '-5', '-4', '-3', '-2', '-1', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']

MENU_ACCOUNTS = 'Лички'
MENU_CANCEL = 'Отмена'

SUBMENU_GET = 'Выдать лички'
SUBMENU_ADD = 'Добавить лички'
SUBMENU_FREE = 'Свободные лички'
SUBMENU_RETURN = 'Вернуть личку'
SUBMENU_SEARCH = 'Поиск лички'
BTN_BACK_TO_MENU = 'В меню'

BTN_ISSUE_CONFIRM = 'Выдать'
BTN_ISSUE_NEXT = 'Другая'
BTN_RETURN_CONFIRM = 'Подтвердить бан'

# Память состояний пользователей (для старта хватит)
user_states = {}


# =========================
# GOOGLE SHEETS
# =========================
def get_gspread_client():
    data = json.loads(SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(data, scopes=scopes)
    return gspread.authorize(creds)


def get_sheet(sheet_name):
    client = get_gspread_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet(sheet_name)


# =========================
# TELEGRAM
# =========================
def tg_send_message(chat_id, text, keyboard=None):
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    if keyboard:
        payload["reply_markup"] = {
            "keyboard": keyboard,
            "resize_keyboard": True,
            "one_time_keyboard": False
        }

    requests.post(f"{BASE_URL}/sendMessage", json=payload, timeout=30)


def send_main_menu(chat_id, text="Главное меню:"):
    keyboard = [
        [{"text": MENU_ACCOUNTS}],
        [{"text": MENU_CANCEL}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_accounts_menu(chat_id, text="Меню личек:"):
    keyboard = [
        [{"text": SUBMENU_GET}, {"text": SUBMENU_ADD}],
        [{"text": SUBMENU_FREE}, {"text": SUBMENU_RETURN}],
        [{"text": SUBMENU_SEARCH}, {"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)


def send_simple_options(chat_id, title, options):
    rows = []
    for i in range(0, len(options), 2):
        row = [{"text": options[i]}]
        if i + 1 < len(options):
            row.append({"text": options[i + 1]})
        rows.append(row)
    rows.append([{"text": MENU_CANCEL}])
    tg_send_message(chat_id, title, rows)


# =========================
# HELPERS
# =========================
def parse_date(value):
    value = str(value).strip()
    for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    return None


def format_date_for_user(value):
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y")
    if hasattr(value, "strftime"):
        return value.strftime("%d/%m/%Y")
    return str(value)


def parse_price(value):
    s = str(value).strip().replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def get_state(user_id):
    return user_states.get(str(user_id), {})


def set_state(user_id, data):
    user_states[str(user_id)] = data


def clear_state(user_id):
    user_states.pop(str(user_id), None)

def find_account_in_base(account_number):
    sheet = get_sheet(SHEET_ACCOUNTS)
    rows = sheet.get_all_values()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 11:
            continue
        if str(row[0]).strip() == str(account_number).strip():
            return {
                "row_index": idx,
                "row": row
            }
    return None


def find_last_issue_row(account_number):
    sheet = get_sheet(SHEET_ISSUES)
    rows = sheet.get_all_values()

    last_match = None
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 7:
            continue
        if str(row[0]).strip() == str(account_number).strip():
            last_match = {
                "row_index": idx,
                "row": row
            }
    return last_match


def is_banned_account(base_row, issue_row=None):
    base_target = ""
    if base_row and len(base_row) >= 10:
        base_target = str(base_row[9]).strip().lower()

    issue_target = ""
    if issue_row and len(issue_row) >= 7:
        issue_target = str(issue_row[6]).strip().lower()

    return base_target == "ban" or issue_target == "ban"


def return_account_to_ban(account_number):
    base_info = find_account_in_base(account_number)
    issue_info = find_last_issue_row(account_number)

    if not base_info:
        return False, "Личка не найдена в базе."

    base_sheet = get_sheet(SHEET_ACCOUNTS)

    # J колонка = кому выдали
    base_sheet.update(f"J{base_info['row_index']}", [["ban"]])

    if issue_info:
        issue_sheet = get_sheet(SHEET_ISSUES)
        # G колонка = кому передали
        issue_sheet.update(f"G{issue_info['row_index']}", [["ban"]])

    return True, "Личка переведена в ban."


def build_account_search_text(account_number):
    base_info = find_account_in_base(account_number)
    if not base_info:
        return None

    issue_info = find_last_issue_row(account_number)

    row = base_info["row"]
    issue_row = issue_info["row"] if issue_info else None

    price = row[2] if len(row) > 2 else ""
    warehouses = row[7] if len(row) > 7 else ""
    date_taken = row[10] if len(row) > 10 else ""
    for_whom = row[9] if len(row) > 9 else ""

    banned = is_banned_account(row, issue_row)

    who_took = "неизвестно"
    if issue_row:
        # у нас пока в таблицу выдачи "кто взял" не пишется
        who_took = "не хранится в таблице"

    text = (
        f"Номер: {account_number}\n"
        f"Статус: {'ban' if banned else 'активна'}\n"
        f"Склады: {warehouses}\n"
        f"Цена: {price}\n"
        f"Дата взятия: {date_taken}\n"
        f"Кто взял: {who_took}\n"
    )

    if not banned:
        text += f"Для кого взял: {for_whom}\n"

    return text


# =========================
# FREE ACCOUNTS
# =========================
def send_free_accounts(chat_id):
    sheet = get_sheet(SHEET_ACCOUNTS)
    rows = sheet.get_all_values()

    if len(rows) < 2:
        tg_send_message(chat_id, "В базе пока нет личек.")
        return

    free_rows = []
    for row in rows[1:]:
        if len(row) < 11:
            continue
        status = str(row[8]).strip().lower()
        if status == "free":
            free_rows.append(row)

    if not free_rows:
        tg_send_message(chat_id, "Свободных личек сейчас нет.")
        return

    lines = []
    max_to_show = 20
    for i, row in enumerate(free_rows[:max_to_show], start=1):
        acc = row[0]
        limit_val = row[4]
        threshold = row[5]
        gmt = row[6]
        warehouses = row[7]
        lines.append(f"{i}. {acc} | {limit_val} | {threshold} | {gmt} | {warehouses}")

    text = f"Свободные лички: {len(free_rows)}\n\n" + "\n".join(lines)
    if len(free_rows) > max_to_show:
        text += f"\n\nПоказаны первые {max_to_show}."

    tg_send_message(chat_id, text)


# =========================
# ADD ACCOUNTS
# =========================
def send_bulk_add_instructions(chat_id):
    text = (
        "Отправь список личек, каждая с новой строки.\n\n"
        "Какой есть лимит:\n"
        "• -250\n"
        "• 250-500\n"
        "• 500-1200\n"
        "• 1200-1500\n"
        "• unlim\n\n"
        "Какой есть трешхолд:\n"
        "• 0-49\n"
        "• 50-99\n"
        "• 100-199\n"
        "• 200-499\n"
        "• 500+\n\n"
        "GMT:\n"
        "• -10\n"
        "• -9\n"
        "• -8\n"
        "• -7\n"
        "• -6\n"
        "• -5\n"
        "• -4\n"
        "• -3\n"
        "• -2\n"
        "• -1\n"
        "• 0\n"
        "• 1\n"
        "• 2\n"
        "• 3\n"
        "• 4\n"
        "• 5\n"
        "• 6\n"
        "• 7\n"
        "• 8\n"
        "• 9\n"
        "• 10\n\n"
        "Формат:\n"
        "номер лички; дата покупки; цена; у кого купили; лимит; трешхолд; GMT; склады\n\n"
        "Пример:\n"
        "RK001; 15/02/2026; 300; WD; 250-500; 50-99; 3; sklad1,sklad2\n"
        "RK002; 16/02/2026; 500; WD; unlim; 100-199; 2; sklad3"
    )
    tg_send_message(chat_id, text)


def add_accounts_from_text(text):
    sheet = get_sheet(SHEET_ACCOUNTS)
    existing_rows = sheet.get_all_values()
    existing_accounts = set()

    for row in existing_rows[1:]:
        if row and row[0].strip():
            existing_accounts.add(row[0].strip())

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    to_append = []
    errors = []
    duplicates = 0

    for i, line in enumerate(lines, start=1):
        fields = [x.strip() for x in line.split(";")]
        if len(fields) != 8:
            errors.append(f"Строка {i}: должно быть 8 полей через ';'")
            continue

        account_number, purchase_date_raw, price_raw, supplier, limit_val, threshold_val, gmt_val, warehouses = fields

        if not account_number:
            errors.append(f"Строка {i}: пустой номер лички")
            continue

        if account_number in existing_accounts:
            duplicates += 1
            continue

        purchase_date = parse_date(purchase_date_raw)
        if not purchase_date:
            errors.append(f"Строка {i}: неверная дата покупки '{purchase_date_raw}'")
            continue

        price = parse_price(price_raw)
        if price is None:
            errors.append(f"Строка {i}: неверная цена '{price_raw}'")
            continue

        if limit_val not in LIMIT_OPTIONS:
            errors.append(f"Строка {i}: неверный лимит '{limit_val}'")
            continue

        if threshold_val not in THRESHOLD_OPTIONS:
            errors.append(f"Строка {i}: неверный трешхолд '{threshold_val}'")
            continue

        if gmt_val not in GMT_OPTIONS:
            errors.append(f"Строка {i}: неверный GMT '{gmt_val}'")
            continue

        to_append.append([
            account_number,
            purchase_date.strftime("%d/%m/%Y"),
            price,
            supplier,
            limit_val,
            threshold_val,
            gmt_val,
            warehouses,
            "free",
            "",
            ""
        ])
        existing_accounts.add(account_number)

    if to_append:
        sheet.append_rows(to_append, value_input_option="USER_ENTERED")

    message = (
        f"Готово.\n"
        f"Добавлено: {len(to_append)}\n"
        f"Дубликатов пропущено: {duplicates}\n"
        f"Ошибок: {len(errors)}"
    )

    if errors:
        message += "\n\nОшибки:\n" + "\n".join(errors[:10])
        if len(errors) > 10:
            message += f"\n... и ещё {len(errors) - 10}"

    return message


# =========================
# ISSUE FLOW
# =========================
def find_matching_free_account(limit_val, threshold_val, gmt_val, exclude_account=None):
    sheet = get_sheet(SHEET_ACCOUNTS)
    rows = sheet.get_all_values()

    candidates = []
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 11:
            continue

        status = str(row[8]).strip().lower()
        if status != "free":
            continue
        if str(row[4]).strip() != limit_val:
            continue
        if str(row[5]).strip() != threshold_val:
            continue
        if str(row[6]).strip() != gmt_val:
            continue
        if exclude_account and str(row[0]).strip() == exclude_account:
            continue

        purchase_date = parse_date(row[1]) or datetime.max
        candidates.append((idx, purchase_date, row))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1])
    row_idx, _, row = candidates[0]
    return {
        "row_index": row_idx,
        "account_number": row[0],
        "purchase_date": row[1],
        "price": row[2],
        "supplier": row[3],
        "warehouses": row[7],
    }


def show_found_account(chat_id, user_id, found):
    state = get_state(user_id)
    state["mode"] = "account_found"
    state["found_row"] = found["row_index"]
    state["found_account"] = found["account_number"]
    set_state(user_id, state)

    text = (
        "Найдена личка:\n\n"
        f"Номер: {found['account_number']}\n"
        f"Склады: {found['warehouses']}\n"
        f"Дата покупки: {found['purchase_date']}\n"
        f"Цена: {found['price']}\n\n"
        f"Кому передали: {state['for_whom']}"
    )

    keyboard = [
        [{"text": BTN_ISSUE_CONFIRM}, {"text": BTN_ISSUE_NEXT}],
        [{"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)


def append_issue_row(account_number, purchase_date, price, transfer_date, supplier, for_whom):
    sheet = get_sheet(SHEET_ISSUES)
    sheet.append_row([
        account_number,
        "РК",
        purchase_date,
        price,
        transfer_date,
        supplier,
        for_whom
    ], value_input_option="USER_ENTERED")


def confirm_issue(chat_id, user_id, username):
    state = get_state(user_id)
    if state.get("mode") != "account_found":
        send_main_menu(chat_id, "Сначала найди личку.")
        return

    row_index = state.get("found_row")
    if not row_index:
        send_main_menu(chat_id, "Не нашёл выбранную личку. Начни заново.")
        return

    sheet = get_sheet(SHEET_ACCOUNTS)
    row = sheet.row_values(row_index)

    if len(row) < 11:
        row = row + [''] * (11 - len(row))

    status = str(row[8]).strip().lower()
    if status != "free":
        tg_send_message(chat_id, "Эта личка уже занята. Ищу другую...")
        found = find_matching_free_account(
            state["limit"],
            state["threshold"],
            state["gmt"],
            exclude_account=state.get("found_account")
        )
        if not found:
            clear_state(user_id)
            send_main_menu(chat_id, "Подходящих свободных личек больше нет.")
            return

        show_found_account(chat_id, user_id, found)
        return

    account_number = row[0]
    purchase_date = row[1]
    price = row[2]
    supplier = row[3]
    today = datetime.now().strftime("%d/%m/%Y")

    sheet.update(f"I{row_index}:K{row_index}", [["taken", state["for_whom"], today]])
    append_issue_row(account_number, purchase_date, price, today, supplier, state["for_whom"])

    clear_state(user_id)

    who_took_text = f"@{username}" if username else "без username"

    tg_send_message(
        chat_id,
        f"Готово ✅\n\n"
        f"Выдана личка: {account_number}\n"
        f"Кому передали: {state['for_whom']}\n"
        f"Кто взял в боте: {who_took_text}"
    )
    send_main_menu(chat_id, "Выбери следующее действие:")


# =========================
# MESSAGE HANDLER
# =========================
def handle_message(msg):
    chat_id = msg["chat"]["id"]
    user_id = msg["from"]["id"]
    username = msg["from"].get("username", "")
    text = str(msg.get("text", "")).strip()

    state = get_state(user_id)

    if text in ["/start", "/menu"]:
        clear_state(user_id)
        send_main_menu(chat_id)
        return

    if text == "/help":
        clear_state(user_id)
        tg_send_message(
            chat_id,
            "/start — открыть меню\n"
            "/menu — открыть меню\n"
            "/help — помощь"
        )
        send_main_menu(chat_id)
        return

    if text == MENU_CANCEL:
        clear_state(user_id)
        send_main_menu(chat_id, "Действие отменено.")
        return

    if text == MENU_ACCOUNTS:
        clear_state(user_id)
        send_accounts_menu(chat_id)
        return

    if text == BTN_BACK_TO_MENU:
        clear_state(user_id)
        send_main_menu(chat_id)
        return

    if text == SUBMENU_ADD:
        set_state(user_id, {"mode": "awaiting_bulk_add"})
        send_bulk_add_instructions(chat_id)
        return

    if text == SUBMENU_FREE:
        clear_state(user_id)
        send_free_accounts(chat_id)
        send_accounts_menu(chat_id, "Выбери следующее действие:")
        return

    if text == SUBMENU_GET:
        set_state(user_id, {"mode": "awaiting_issue_for_whom"})
        tg_send_message(chat_id, "Напиши, для кого берешь личку.")
        return

    if text == SUBMENU_RETURN:
        set_state(user_id, {"mode": "awaiting_return_account"})
        tg_send_message(chat_id, "Впиши номер лички, которую нужно отправить в ban.")
        return

    if text == SUBMENU_SEARCH:
        set_state(user_id, {"mode": "awaiting_search_account"})
        tg_send_message(chat_id, "Впиши номер лички для поиска.")
        return

    if text == BTN_ISSUE_CONFIRM:
        confirm_issue(chat_id, user_id, username)
        return

    if text == BTN_ISSUE_NEXT:
        if not state:
            send_main_menu(chat_id, "Начни заново.")
            return
        found = find_matching_free_account(
            state["limit"],
            state["threshold"],
            state["gmt"],
            exclude_account=state.get("found_account")
        )
        if not found:
            clear_state(user_id)
            send_accounts_menu(chat_id, "Подходящих свободных личек больше нет.")
            return
        show_found_account(chat_id, user_id, found)
        return

    if text == BTN_RETURN_CONFIRM:
        if state.get("mode") != "awaiting_return_confirm":
            send_accounts_menu(chat_id, "Сначала выбери действие заново.")
            return

        account_number = state.get("return_account_number", "")
        ok, message = return_account_to_ban(account_number)
        clear_state(user_id)
        send_accounts_menu(chat_id, message)
        return

    if state.get("mode") == "awaiting_bulk_add":
        result = add_accounts_from_text(text)
        clear_state(user_id)
        tg_send_message(chat_id, result)
        send_accounts_menu(chat_id, "Готово. Выбери следующее действие:")
        return

    if state.get("mode") == "awaiting_return_account":
        account_number = text.strip()
        if not account_number:
            tg_send_message(chat_id, "Впиши номер лички.")
            return

        found = find_account_in_base(account_number)
        if not found:
            clear_state(user_id)
            send_accounts_menu(chat_id, "Личка не найдена.")
            return

        set_state(user_id, {
            "mode": "awaiting_return_confirm",
            "return_account_number": account_number
        })

        keyboard = [
            [{"text": BTN_RETURN_CONFIRM}, {"text": MENU_CANCEL}]
        ]

        tg_send_message(
            chat_id,
            f"Внимание: личка {account_number} будет перемещена в ban.\nПодтвердить?",
            keyboard
        )
        return

    if state.get("mode") == "awaiting_search_account":
        account_number = text.strip()
        if not account_number:
            tg_send_message(chat_id, "Впиши номер лички.")
            return

        result = build_account_search_text(account_number)
        clear_state(user_id)

        if not result:
          send_accounts_menu(chat_id, "Личка не найдена.")
          return

        tg_send_message(chat_id, result)
        send_accounts_menu(chat_id, "Выбери следующее действие:")
        return

    if state.get("mode") == "awaiting_issue_for_whom":
        if not text:
            tg_send_message(chat_id, "Напиши, для кого берешь личку.")
            return
        set_state(user_id, {
            "mode": "awaiting_issue_limit",
            "for_whom": text
        })
        send_simple_options(chat_id, "Выбери лимит:", LIMIT_OPTIONS)
        return

    if state.get("mode") == "awaiting_issue_limit":
        if text not in LIMIT_OPTIONS:
            send_simple_options(chat_id, "Нужно выбрать лимит кнопкой:", LIMIT_OPTIONS)
            return
        state["mode"] = "awaiting_issue_threshold"
        state["limit"] = text
        set_state(user_id, state)
        send_simple_options(chat_id, "Выбери трешхолд:", THRESHOLD_OPTIONS)
        return

    if state.get("mode") == "awaiting_issue_threshold":
        if text not in THRESHOLD_OPTIONS:
            send_simple_options(chat_id, "Нужно выбрать трешхолд кнопкой:", THRESHOLD_OPTIONS)
            return
        state["mode"] = "awaiting_issue_gmt"
        state["threshold"] = text
        set_state(user_id, state)
        send_simple_options(chat_id, "Выбери GMT:", GMT_OPTIONS)
        return

    if state.get("mode") == "awaiting_issue_gmt":
        if text not in GMT_OPTIONS:
            send_simple_options(chat_id, "Нужно выбрать GMT кнопкой:", GMT_OPTIONS)
            return
        state["mode"] = "searching_account"
        state["gmt"] = text
        set_state(user_id, state)

        found = find_matching_free_account(state["limit"], state["threshold"], state["gmt"])
        if not found:
            clear_state(user_id)
            send_accounts_menu(chat_id, "Подходящих свободных личек не найдено.")
            return

        show_found_account(chat_id, user_id, found)
        return

    send_main_menu(chat_id, "Не понял команду. Выбери кнопку из меню:")

# =========================
# FLASK
# =========================
@app.route("/", methods=["GET"])
def index():
    return "ok", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    update = request.get_json(silent=True) or {}
    msg = update.get("message") or update.get("edited_message")
    if msg and msg.get("text"):
        handle_message(msg)
    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
