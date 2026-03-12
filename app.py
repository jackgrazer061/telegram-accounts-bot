import os
import json
import logging
from datetime import datetime
import time
import re
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
import requests
import gspread
from google.oauth2.service_account import Credentials
import threading

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# =========================
# ENV
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
BACKUP_SPREADSHEET_ID = os.environ.get("BACKUP_SPREADSHEET_ID", "")
OCR_SPACE_API_KEY = os.environ.get("OCR_SPACE_API_KEY", "")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан")

if not SPREADSHEET_ID:
    raise RuntimeError("SPREADSHEET_ID не задан")

if not SERVICE_ACCOUNT_JSON:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON не задан")

if not BACKUP_SPREADSHEET_ID:
    raise RuntimeError("BACKUP_SPREADSHEET_ID не задан")

if not OCR_SPACE_API_KEY:
    raise RuntimeError("OCR_SPACE_API_KEY не задан")
# =========================
# ACCESS CONTROL
# =========================

ADMINS = {
    7573650707,  # jack
    7681133609,  # cilian
}

OPERATORS = {
    7953116439,   # willem
    8334712952,   # ariana
    7851493919,   # cate
    
}

def is_admin(user_id):
    return user_id in ADMINS

def is_operator(user_id):
    return user_id in OPERATORS

def has_access(user_id):
    return is_admin(user_id) or is_operator(user_id)

def touch_request_heartbeat():
    global last_request_time
    last_request_time = time.time()

def touch_background_heartbeat():
    global last_background_time
    last_background_time = time.time()

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

SHEET_ACCOUNTS = "База_личек"
SHEET_ISSUES = "Простые лички 26"
SHEET_KINGS = "База_кингов"
SHEET_BMS = "База_БМ"

LIMIT_OPTIONS = ['-250', '250-500', '500-1200', '1200-1500', 'unlim']
THRESHOLD_OPTIONS = ['0-49', '50-99', '100-199', '200-499', '500+']
GMT_OPTIONS = ['-10', '-9', '-8', '-7', '-6', '-5', '-4', '-3', '-2', '-1', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
ACCOUNT_CURRENCY_COL = 12  # M колонка в База_личек

MENU_ACCOUNTS = 'Лички'
MENU_KINGS = 'Кинги'
MENU_BMS = 'БМ'
MENU_STATS = 'Статистика'
MENU_MANAGER_STATS = 'Статистика менеджера'
MENU_ADMIN = 'Admin'
MENU_CANCEL = 'Отмена'

DEPT_CRYPTO = 'Крипта'
DEPT_GAMBLA = 'Гембла'

CRYPTO_NAMES = [
    'dasha', 'mark', 'misha', 'vladimir1', 'andrey',
    'alex', 'anton', 'vladimir2', 'danilacc', 'aleksandr2',
    'maksim3', 'nikita3', 'anton2', 'yan', 'nikita'
]

GAMBLA_NAMES = [
    'artem', 'ivan', 'sergei', 'ilya', 'maksim1',
    'denis', 'kirill', 'ivansh', 'evgen', 'asim',
    'maksim2', 'alex_gambl', 'daniil', 'semen', 'ivan2',
    'andrey2', 'vitaliy', 'gleb', 'dasha2', 'Vladimir3'
]

SUBMENU_GET = 'Выдать лички'
SUBMENU_QUICK_GET = 'Быстро выдать личку'
SUBMENU_FREE = 'Свободные лички'
SUBMENU_RETURN = 'Вернуть личку'
SUBMENU_SEARCH = 'Поиск лички'

SUBMENU_FREE_KINGS = 'Свободные кинги'
SUBMENU_GET_KINGS = 'Получить кинг'
SUBMENU_RETURN_KING = 'Вернуть кинг'
SUBMENU_SEARCH_KING = 'Поиск кинга'

SUBMENU_GET_BM = 'Получить БМ'
SUBMENU_FREE_BMS = 'Свободные БМы'
SUBMENU_SEARCH_BM = 'Поиск БМа'

ADMIN_BACKUP = 'Бэкап таблиц'
ADMIN_ADD_ACCOUNTS = 'Добавить лички'
ADMIN_ADD_KINGS = 'Добавить кинги'
ADMIN_ADD_BMS = 'Добавить БМы'
ADMIN_IMPORT_SCREEN = 'Импорт из скрина'
BTN_BM_CONFIRM = 'Выдать БМ'
BTN_BM_NEXT = 'Другой БМ'
BTN_OCR_CONFIRM = 'Подтвердить обновление'
BTN_OCR_REJECT = 'Отмена OCR'
BTN_BACK_FROM_ADMIN = 'Назад из Admin'

BTN_BACK_TO_MENU = 'В меню'

# кнопки выдачи личек
BTN_ISSUE_CONFIRM = 'Выдать личку'
BTN_ISSUE_NEXT = 'Другая личка'
BTN_RETURN_CONFIRM = 'Подтвердить бан'

# кнопки выдачи кингов
BTN_KING_CONFIRM = 'Выдать кинг'
BTN_KING_NEXT = 'Другой кинг'
BTN_KING_BAN_CONFIRM = 'Подтвердить ban'

# Память состояний пользователей (для старта хватит)
user_states = {}
issue_lock = threading.Lock()

backup_lock = threading.Lock()
last_backup_date = None
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

last_user_action = {}
ACTION_COOLDOWN = 1

STATE_TTL = 600  # 10 минут

last_request_time = time.time()
last_background_time = time.time()
WATCHDOG_TIMEOUT = 300  # 5 минут

gspread_client = None
sheet_cache = {}

google_error_until = 0
GOOGLE_ERROR_COOLDOWN = 5
google_error_count = 0

def reset_google_cache():
    global gspread_client, sheet_cache, table_cache
    gspread_client = None
    sheet_cache = {}

    with table_cache_lock:
        table_cache = {
            SHEET_ACCOUNTS: {"rows": None, "updated_at": 0},
            SHEET_ISSUES: {"rows": None, "updated_at": 0},
            SHEET_KINGS: {"rows": None, "updated_at": 0},
            SHEET_BMS: {"rows": None, "updated_at": 0},
        }

    with table_cache_lock:
        table_cache = {
            SHEET_ACCOUNTS: {"rows": None, "updated_at": 0},
            SHEET_ISSUES: {"rows": None, "updated_at": 0},
            SHEET_KINGS: {"rows": None, "updated_at": 0},
            SHEET_BMS: {"rows": None, "updated_at": 0},
        }

def reset_table_cache():
    global table_cache
    with table_cache_lock:
        table_cache = {
            SHEET_ACCOUNTS: {"rows": None, "updated_at": 0},
            SHEET_ISSUES: {"rows": None, "updated_at": 0},
            SHEET_KINGS: {"rows": None, "updated_at": 0},
            SHEET_BMS: {"rows": None, "updated_at": 0},
        }

def check_google_available():
    global google_error_until

    if time.time() < google_error_until:
        wait_left = int(google_error_until - time.time()) + 1
        raise RuntimeError(
            f"Google Sheets временно перегружен, попробуй через {wait_left} сек."
        )


def invalidate_stats_cache():
    pass

# =========================
# AUTO CACHE FOR SHEETS
# =========================
TABLE_CACHE_TTL = 3  # сек; можно 3-5

table_cache = {
    SHEET_ACCOUNTS: {"rows": None, "updated_at": 0},
    SHEET_ISSUES: {"rows": None, "updated_at": 0},
    SHEET_KINGS: {"rows": None, "updated_at": 0},
    SHEET_BMS: {"rows": None, "updated_at": 0},
}

table_cache_lock = threading.Lock()


def refresh_sheet_cache(sheet_name):
    sheet = get_sheet(sheet_name)
    rows = sheet.get_all_values()

    with table_cache_lock:
        table_cache[sheet_name]["rows"] = rows
        table_cache[sheet_name]["updated_at"] = time.time()

    return rows


def get_sheet_rows_cached(sheet_name, force=False):
    now = time.time()

    with table_cache_lock:
        cache = table_cache.get(sheet_name)
        if cache:
            is_fresh = (
                cache["rows"] is not None
                and (now - cache["updated_at"] < TABLE_CACHE_TTL)
            )
            if is_fresh and not force:
                return cache["rows"]

    return refresh_sheet_cache(sheet_name)


def sheet_update_and_refresh(sheet_name, cell_range, values):
    sheet = get_sheet(sheet_name)
    sheet.update(cell_range, values)
    refresh_sheet_cache(sheet_name)


def sheet_append_row_and_refresh(sheet_name, row, value_input_option="USER_ENTERED"):
    sheet = get_sheet(sheet_name)
    sheet.append_row(row, value_input_option=value_input_option)
    refresh_sheet_cache(sheet_name)


def sheet_append_rows_and_refresh(sheet_name, rows, value_input_option="USER_ENTERED"):
    sheet = get_sheet(sheet_name)
    sheet.append_rows(rows, value_input_option=value_input_option)
    refresh_sheet_cache(sheet_name)


# =========================
# GOOGLE SHEETS
# =========================
def get_gspread_client():
    global gspread_client

    if gspread_client is not None:
        return gspread_client

    try:
        data = json.loads(SERVICE_ACCOUNT_JSON)

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = Credentials.from_service_account_info(data, scopes=scopes)
        gspread_client = gspread.authorize(creds)

        return gspread_client

    except Exception as e:
        logging.error(f"get_gspread_client error: {e}")
        gspread_client = None
        raise

def get_sheet(sheet_name):
    global sheet_cache, google_error_until, google_error_count

    check_google_available()

    try:
        if sheet_name in sheet_cache:
            return sheet_cache[sheet_name]

        client = get_gspread_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        start_time = time.time()
        sheet = spreadsheet.worksheet(sheet_name)

        if time.time() - start_time > 10:
            logging.warning(f"Google Sheets slow response for '{sheet_name}'")

        sheet_cache[sheet_name] = sheet
        google_error_count = 0
        google_error_until = 0
        return sheet

    except Exception as e:
        logging.error(f"get_sheet first error for '{sheet_name}': {e}")

        google_error_count += 1

        if google_error_count >= 5:
            cooldown = 30
        elif google_error_count >= 3:
            cooldown = 15
        else:
            cooldown = 5

        google_error_until = time.time() + cooldown
        reset_google_cache()
        time.sleep(1)

        try:
            client = get_gspread_client()
            spreadsheet = client.open_by_key(SPREADSHEET_ID)
            sheet = spreadsheet.worksheet(sheet_name)

            sheet_cache[sheet_name] = sheet
            google_error_count = 0
            google_error_until = 0
            return sheet

        except Exception as e2:
            logging.error(f"get_sheet second error for '{sheet_name}': {e2}")

            google_error_count += 1

            if google_error_count >= 5:
                cooldown = 30
            elif google_error_count >= 3:
                cooldown = 15
            else:
                cooldown = 5

            google_error_until = time.time() + cooldown
            reset_google_cache()
            raise


# =========================
# TELEGRAM
# =========================
def tg_send_message(chat_id, text, keyboard=None):
    try:
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

        resp = requests.post(
            f"{BASE_URL}/sendMessage",
            json=payload,
            timeout=10
        )

        if resp.status_code != 200:
            logging.warning(f"Telegram send failed: {resp.text}")

    except Exception as e:
        logging.error(f"tg_send_message error: {e}")

def send_main_menu(chat_id, text="Главное меню:", user_id=None):
    if user_id is not None and is_admin(user_id):
        keyboard = [
            [{"text": MENU_ACCOUNTS}, {"text": MENU_KINGS}],
            [{"text": MENU_BMS}],
            [{"text": MENU_STATS}, {"text": MENU_MANAGER_STATS}],
            [{"text": MENU_ADMIN}],
            [{"text": MENU_CANCEL}]
        ]
    else:
        keyboard = [
            [{"text": MENU_ACCOUNTS}, {"text": MENU_KINGS}],
            [{"text": MENU_BMS}],
            [{"text": MENU_STATS}, {"text": MENU_MANAGER_STATS}],
            [{"text": MENU_CANCEL}]
        ]

    tg_send_message(chat_id, text, keyboard)

def send_accounts_menu(chat_id, text="Меню личек:"):
    keyboard = [
        [{"text": SUBMENU_GET}, {"text": SUBMENU_QUICK_GET}],
        [{"text": SUBMENU_FREE}, {"text": SUBMENU_RETURN}],
        [{"text": SUBMENU_SEARCH}, {"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)
    
def send_kings_menu(chat_id, text="Меню кингов:"):
    keyboard = [
        [{"text": SUBMENU_GET_KINGS}],
        [{"text": SUBMENU_FREE_KINGS}],
        [{"text": SUBMENU_RETURN_KING}],
        [{"text": SUBMENU_SEARCH_KING}],
        [{"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_bms_menu(chat_id, text="Меню БМов:"):
    keyboard = [
        [{"text": SUBMENU_GET_BM}],
        [{"text": SUBMENU_FREE_BMS}],
        [{"text": SUBMENU_SEARCH_BM}],
        [{"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_admin_menu(chat_id, text="Меню Admin:"):
    keyboard = [
        [{"text": ADMIN_BACKUP}],
        [{"text": ADMIN_ADD_ACCOUNTS}, {"text": ADMIN_ADD_KINGS}],
        [{"text": ADMIN_ADD_BMS}],
        [{"text": ADMIN_IMPORT_SCREEN}],
        [{"text": BTN_BACK_FROM_ADMIN}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_add_kings_instructions(chat_id):
    text = (
        "Пришли txt файл.\n\n"
        "Формат каждого блока такой:\n\n"
        "1) 15/02/2026; 300; WD; usa\n"
        "login - example1\n"
        "password - 12345\n"
        "cookie - abcdef\n\n"
        "2) 16/02/2026; 500; WD; uk\n"
        "login - example2\n"
        "password - 67890\n"
        "cookie - ghijkl\n\n"
        "3) 17/02/2026; 250; WD; italy\n"
        "login - example3\n"
        "password - 11111\n"
        "cookie - zzzzzz\n\n"
        "Первая строка блока:\n"
        "номер) дата покупки; цена; поставщик; гео\n"
        "Ниже — данные кинга.\n\n"
    )
    tg_send_message(chat_id, text)

def send_add_bms_instructions(chat_id):
    text = (
        "Пришли БМы сообщением.\n\n"
        "Формат каждого блока:\n\n"
        "1) 123456789; 15/02/2026; 300; WD\n"
        "login - example\n"
        "password - 12345\n"
        "2fa - qwerty\n\n"
        "2) 987654321; 18/02/2026; 500; TT\n"
        "login - example2\n"
        "password - 99999\n"
        "2fa - zzzzzz\n\n"
        "Первая строка блока:\n"
        "номер) id БМа; дата покупки; цена; у кого купили\n"
        "Ниже — данные БМа."
    )
    tg_send_message(chat_id, text)

def get_free_king_geos():
    rows = get_sheet_rows_cached(SHEET_KINGS)

    geos = []
    seen = set()

    for row in rows[1:]:
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

        status = str(row[4]).strip().lower()
        geo = str(row[7]).strip()

        if status == "free" and geo and geo not in seen:
            geos.append(geo)
            seen.add(geo)

    return geos

def send_king_geo_options(chat_id):
    geos = get_free_king_geos()

    if not geos:
        send_kings_menu(chat_id, "Нет свободных кингов ни по одному GEO.")
        return

    keyboard = []
    for geo in geos:
        keyboard.append([{"text": geo}])

    keyboard.append([{"text": MENU_CANCEL}])

    tg_send_message(chat_id, "Какое нужно гео?", keyboard)

def tg_get_file_path(file_id):
    resp = requests.get(
        f"{BASE_URL}/getFile",
        params={"file_id": file_id},
        timeout=30
    )
    data = resp.json()

    if not data.get("ok"):
        return None

    return data["result"]["file_path"]


def tg_download_file_content(file_id):
    file_path = tg_get_file_path(file_id)
    if not file_path:
        return None

    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"

    resp = requests.get(file_url, timeout=60)

    if resp.status_code != 200:
        return None

    return resp.content

def tg_download_photo_content(photo_list):
    if not photo_list:
        return None

    biggest_photo = photo_list[-1]
    file_id = biggest_photo.get("file_id")

    if not file_id:
        return None

    return tg_download_file_content(file_id)


def extract_digits(text):
    return re.sub(r"\D", "", str(text or ""))


def normalize_limit_to_bucket(limit_value):
    try:
        value = float(str(limit_value).replace(",", ".").strip())
    except Exception:
        return None

    if value < 250:
        return "-250"
    elif 250 <= value < 500:
        return "250-500"
    elif 500 <= value < 1200:
        return "500-1200"
    elif 1200 <= value < 1500:
        return "1200-1500"
    else:
        return "unlim"


def normalize_threshold_to_bucket(threshold_value):
    try:
        value = float(str(threshold_value).replace(",", ".").strip())
    except Exception:
        return None

    if 0 <= value <= 49:
        return "0-49"
    elif 50 <= value <= 99:
        return "50-99"
    elif 100 <= value <= 199:
        return "100-199"
    elif 200 <= value <= 499:
        return "200-499"
    elif value >= 500:
        return "500+"
    return None


def normalize_gmt_value(raw_value):
    if raw_value is None:
        return None

    text = str(raw_value).strip()

    # убираем лишние пробелы
    text = re.sub(r"\s+", " ", text)

    # 1) Account Time Zone / GMT / UTC + число
    match = re.search(r'(?:ACCOUNT TIME ZONE|TIME ZONE|GMT|UTC)[^+\-\d]*([+-]\d{1,2})', text, re.IGNORECASE)
    if match:
        return match.group(1).replace("+", "")

    # 2) Формат вида Europe/Rome | +1
    match = re.search(r'[A-Za-z_]+/[A-Za-z_]+(?:\s*\|\s*|\s+)([+-]\d{1,2})', text)
    if match:
        return match.group(1).replace("+", "")

    # 3) Просто любое число со знаком
    match = re.search(r'([+-]\d{1,2})', text)
    if match:
        return match.group(1).replace("+", "")

    # 4) Просто число без знака
    match = re.search(r'\b(\d{1,2})\b', text)
    if match:
        return match.group(1)

    return None


def parse_smit_ocr_text(parsed_text):
    lines = [line.strip() for line in parsed_text.splitlines() if line.strip()]
    full_text = "\n".join(lines)

    account_number = None
    threshold_raw = None
    limit_raw = None
    gmt_raw = None

    for line in lines:
        if not account_number:
            digits = extract_digits(line)
            if 6 <= len(digits) <= 20:
                account_number = digits
                break

    threshold_match = re.search(r'Threshold[^0-9]*([0-9]+(?:[.,][0-9]+)?)', full_text, re.IGNORECASE)
    if threshold_match:
        threshold_raw = threshold_match.group(1)

    remain_match = re.search(r'Remain Threshold[^0-9]*([0-9]+(?:[.,][0-9]+)?)', full_text, re.IGNORECASE)
    if remain_match and threshold_raw is None:
        threshold_raw = remain_match.group(1)

    limit_match = re.search(r'Limit[^0-9]*([0-9]+(?:[.,][0-9]+)?)', full_text, re.IGNORECASE)
    if limit_match:
        limit_raw = limit_match.group(1)

        # Сначала ищем строку с названием колонки / GMT / UTC
    gmt_match = re.search(r'(?:Account Time Zone|Time Zone|GMT|UTC)[^\n]*', full_text, re.IGNORECASE)
    if gmt_match:
        gmt_raw = gmt_match.group(0)

    # Если не нашли — ищем строку формата Europe/Rome | +1
    if not gmt_raw:
        for line in lines:
            if re.search(r'[A-Za-z_]+/[A-Za-z_]+', line) and re.search(r'[+-]\d{1,2}', line):
                gmt_raw = line
                break

    limit_bucket = normalize_limit_to_bucket(limit_raw) if limit_raw else None
    threshold_bucket = normalize_threshold_to_bucket(threshold_raw) if threshold_raw else None
    gmt_value = normalize_gmt_value(gmt_raw) if gmt_raw else None

    if not gmt_value:
        for line in lines:
            test_gmt = normalize_gmt_value(line)
            if test_gmt in GMT_OPTIONS:
                gmt_raw = line
                gmt_value = test_gmt
                break
                
    return {
        "account_number": account_number,
        "limit_raw": limit_raw,
        "limit_bucket": limit_bucket,
        "threshold_raw": threshold_raw,
        "threshold_bucket": threshold_bucket,
        "gmt_raw": gmt_raw,
        "gmt_value": gmt_value,
        "ocr_text": full_text
    }


def run_ocr_space(image_bytes):
    url = "https://api.ocr.space/parse/image"

    files = {
        "filename": ("smit.png", image_bytes)
    }

    data = {
        "apikey": OCR_SPACE_API_KEY,
        "language": "eng",
        "isOverlayRequired": "false",
        "OCREngine": "2",
        "scale": "true"
    }

    resp = requests.post(url, files=files, data=data, timeout=60)
    resp.raise_for_status()

    result = resp.json()

    if result.get("IsErroredOnProcessing"):
        errors = result.get("ErrorMessage") or result.get("ErrorDetails") or ["OCR error"]
        raise RuntimeError(f"OCR ошибка: {'; '.join(map(str, errors))}")

    parsed_results = result.get("ParsedResults") or []
    if not parsed_results:
        raise RuntimeError("OCR не вернул текст")

    parsed_text = "\n".join(
        item.get("ParsedText", "") for item in parsed_results if item.get("ParsedText")
    ).strip()

    if not parsed_text:
        raise RuntimeError("На скриншоте не удалось распознать текст")

    return parsed_text

def is_king_header_line(line):
    line = line.strip()
    return re.match(r'^\d+[.)]\s+', line) is not None


def parse_kings_txt(text):
    lines = text.splitlines()

    blocks = []
    current_header = None
    current_data_lines = []

    for raw_line in lines:
        line = raw_line.rstrip()

        if not line.strip():
            if current_header is not None:
                current_data_lines.append("")
            continue

        if is_king_header_line(line):
            if current_header is not None:
                blocks.append((current_header, current_data_lines))

            current_header = line.strip()
            current_data_lines = []

        else:
            if current_header is not None:
                current_data_lines.append(line)

    if current_header is not None:
        blocks.append((current_header, current_data_lines))

    parsed = []
    errors = []

    for idx, (header, data_lines) in enumerate(blocks, start=1):

        header_clean = re.sub(r'^\d+[.)]\s*', '', header).strip()
        parts = [x.strip() for x in header_clean.split(';')]

        if len(parts) != 4:
            errors.append(
                f"Блок {idx}: нужно 4 поля: дата; цена; поставщик; гео"
            )
            continue

        purchase_date_raw, price_raw, supplier, geo = parts

        purchase_date = parse_date(purchase_date_raw)
        if not purchase_date:
            errors.append(f"Блок {idx}: неверная дата '{purchase_date_raw}'")
            continue

        price = parse_price(price_raw)
        if price is None:
            errors.append(f"Блок {idx}: неверная цена '{price_raw}'")
            continue

        data_text = "\n".join(data_lines).strip()

        if not data_text:
            errors.append(f"Блок {idx}: нет данных кинга")
            continue

        parsed.append({
            "purchase_date": purchase_date.strftime("%d/%m/%Y"),
            "price": price,
            "supplier": supplier,
            "geo": geo,
            "data_text": data_text
        })

    return parsed, errors

def is_bm_header_line(line):
    line = line.strip()
    return re.match(r'^\d+[.)]\s+', line) is not None


def parse_bms_txt(text):
    lines = text.splitlines()

    blocks = []
    current_header = None
    current_data_lines = []

    for raw_line in lines:
        line = raw_line.rstrip()

        if not line.strip():
            if current_header is not None:
                current_data_lines.append("")
            continue

        if is_bm_header_line(line):
            if current_header is not None:
                blocks.append((current_header, current_data_lines))

            current_header = line.strip()
            current_data_lines = []
        else:
            if current_header is not None:
                current_data_lines.append(line)

    if current_header is not None:
        blocks.append((current_header, current_data_lines))

    parsed = []
    errors = []

    for idx, (header, data_lines) in enumerate(blocks, start=1):
        header_clean = re.sub(r'^\d+[.)]\s*', '', header).strip()
        parts = [x.strip() for x in header_clean.split(";")]

        if len(parts) != 4:
            errors.append(f"Блок {idx}: нужно 4 поля: id БМа; дата покупки; цена; у кого купили")
            continue

        bm_id, purchase_date_raw, price_raw, supplier = parts

        purchase_date = parse_date(purchase_date_raw)
        if not purchase_date:
            errors.append(f"Блок {idx}: неверная дата '{purchase_date_raw}'")
            continue

        price = parse_price(price_raw)
        if price is None:
            errors.append(f"Блок {idx}: неверная цена '{price_raw}'")
            continue

        if not bm_id:
            errors.append(f"Блок {idx}: пустой id БМа")
            continue

        if not supplier:
            errors.append(f"Блок {idx}: не указан поставщик")
            continue

        data_text = "\n".join(data_lines).strip()

        if not data_text:
            errors.append(f"Блок {idx}: нет данных БМа")
            continue

        parsed.append({
            "bm_id": bm_id,
            "purchase_date": purchase_date.strftime("%d/%m/%Y"),
            "price": price,
            "supplier": supplier,
            "data_text": data_text
        })

    return parsed, errors

def add_kings_from_txt_content(file_text):
    rows, errors = parse_kings_txt(file_text)

    if not rows:
        if errors:
            return "Ничего не добавил.\n\nОшибки:\n" + "\n".join(errors[:10])
        return "Ничего не добавил. Не удалось разобрать файл."

    to_append = []
    for item in rows:
        to_append.append([
            "",                     # A название кинга — пока пусто
            item["purchase_date"],  # B дата покупки
            item["price"],          # C цена
            item["supplier"],       # D у кого купили
            "free",                 # E статус
            "",                     # F кому выдали
            "",                     # G дата взятия
            item["geo"],            # H гео
            "",                     # I кто взял
            item["data_text"]       # J данные
        ])

    sheet_append_rows_and_refresh(SHEET_KINGS, to_append)
    invalidate_stats_cache()

    message = (
        f"Готово ✅\n"
        f"Добавлено кингов: {len(to_append)}\n"
        f"Ошибок: {len(errors)}"
    )

    if errors:
        message += "\n\nОшибки:\n" + "\n".join(errors[:10])
        if len(errors) > 10:
            message += f"\n... и ещё {len(errors) - 10}"

    return message

def add_bms_from_txt_content(file_text):
    parsed_rows, errors = parse_bms_txt(file_text)

    if not parsed_rows:
        if errors:
            return "Ничего не добавил.\n\nОшибки:\n" + "\n".join(errors[:10])
        return "Ничего не добавил. Не удалось разобрать текст."

    existing_rows = get_sheet_rows_cached(SHEET_BMS)
    existing_ids = set()

    for row in existing_rows[1:]:
        if row and row[0].strip():
            existing_ids.add(row[0].strip())

    to_append = []
    duplicates = 0

    for item in parsed_rows:
        bm_id = item["bm_id"]

        if bm_id in existing_ids:
            duplicates += 1
            continue

        to_append.append([
            item["bm_id"],           # A id БМа
            item["purchase_date"],   # B дата покупки
            item["price"],           # C цена
            item["supplier"],        # D у кого купили
            "free",                  # E статус
            "",                      # F для кого
            "",                      # G кто взял
            "",                      # H дата выдачи
            item["data_text"]        # I данные
        ])
        existing_ids.add(bm_id)

    if to_append:
        sheet_append_rows_and_refresh(SHEET_BMS, to_append)
        invalidate_stats_cache()

    message = (
        f"Готово ✅\n"
        f"Добавлено БМов: {len(to_append)}\n"
        f"Дубликатов пропущено: {duplicates}\n"
        f"Ошибок: {len(errors)}"
    )

    if errors:
        message += "\n\nОшибки:\n" + "\n".join(errors[:10])
        if len(errors) > 10:
            message += f"\n... и ещё {len(errors) - 10}"

    return message

def handle_document_message(msg):
    try:
        touch_request_heartbeat()

        chat_id = msg["chat"]["id"]
        user_id = msg["from"]["id"]

        if not has_access(user_id):
            tg_send_message(
                chat_id,
                f"⛔ У вас нет доступа.\n\nВаш Telegram ID:\n{user_id}"
            )
            return

        state = get_state(user_id)
        mode = state.get("mode")

        if mode not in ["awaiting_kings_txt", "awaiting_bms_text"]:
            tg_send_message(
                chat_id,
                "Я сейчас не жду файл. Сначала открой Admin → Добавить кинги или Admin → Добавить БМы."
            )
            return

        document = msg.get("document", {})
        file_name = document.get("file_name", "")
        file_id = document.get("file_id", "")

        if not file_id:
            tg_send_message(chat_id, "Не удалось получить файл. Попробуй ещё раз.")
            return

        if file_name and not file_name.lower().endswith(".txt"):
            tg_send_message(chat_id, "Нужен именно txt файл.")
            return

        content = tg_download_file_content(file_id)
        if not content:
            tg_send_message(chat_id, "Не удалось скачать файл. Попробуй ещё раз.")
            return

        try:
            file_text = content.decode("utf-8")
        except UnicodeDecodeError:
            try:
                file_text = content.decode("utf-8-sig")
            except UnicodeDecodeError:
                try:
                    file_text = content.decode("cp1251")
                except UnicodeDecodeError:
                    tg_send_message(
                        chat_id,
                        "Не удалось прочитать txt файл. Сохрани его в UTF-8 или ANSI."
                    )
                    return

        if mode == "awaiting_kings_txt":
            result_message = add_kings_from_txt_content(file_text)
        elif mode == "awaiting_bms_text":
            result_message = add_bms_from_txt_content(file_text)
        else:
            tg_send_message(chat_id, "Неизвестный режим загрузки файла.")
            return

        clear_state(user_id)
        tg_send_message(chat_id, result_message)

        if is_admin(user_id):
            send_admin_menu(chat_id, "Выбери следующее действие:")
        else:
            send_main_menu(chat_id, "Выбери следующее действие:", user_id=user_id)

    except Exception as e:
        logging.exception("handle_document_message crashed")
        try:
            error_text = str(e)

            if "Google Sheets временно перегружен" in error_text:
                tg_send_message(msg["chat"]["id"], error_text)
            else:
                tg_send_message(msg["chat"]["id"], "Ошибка обработки файла. Попробуй ещё раз.")
        except Exception:
            pass

def send_simple_options(chat_id, title, options):
    rows = []
    for i in range(0, len(options), 2):
        row = [{"text": options[i]}]
        if i + 1 < len(options):
            row.append({"text": options[i + 1]})
        rows.append(row)
    rows.append([{"text": MENU_CANCEL}])
    tg_send_message(chat_id, title, rows)

def get_available_currencies(limit_val, threshold_val, gmt_val):
    rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

    currencies = []
    seen = set()

    for row in rows[1:]:
        if len(row) <= ACCOUNT_CURRENCY_COL:
            continue

        status = str(row[8]).strip().lower()
        row_limit = str(row[4]).strip()
        row_threshold = str(row[5]).strip()
        row_gmt = str(row[6]).strip()
        currency = str(row[ACCOUNT_CURRENCY_COL]).strip()

        if status != "free":
            continue
        if row_limit != limit_val:
            continue
        if row_threshold != threshold_val:
            continue
        if row_gmt != gmt_val:
            continue
        if not currency:
            continue

        if currency not in seen:
            seen.add(currency)
            currencies.append(currency)

    return currencies


def send_currency_options(chat_id, currencies):
    if not currencies:
        tg_send_message(chat_id, "Нет свободных личек с такими параметрами.")
        return

    keyboard = []
    for currency in currencies:
        keyboard.append([{"text": currency}])

    keyboard.append([{"text": MENU_CANCEL}])

    tg_send_message(chat_id, "Выбери валюту:", keyboard)

def send_department_menu(chat_id, title="Выбери отдел:"):
    keyboard = [
        [{"text": DEPT_CRYPTO}, {"text": DEPT_GAMBLA}],
        [{"text": MENU_CANCEL}]
    ]
    tg_send_message(chat_id, title, keyboard)

def send_person_menu(chat_id, department):
    if department == DEPT_CRYPTO:
        names = CRYPTO_NAMES
        title = "Выбери человека из отдела Крипта:"
    elif department == DEPT_GAMBLA:
        names = GAMBLA_NAMES
        title = "Выбери человека из отдела Гембла:"
    else:
        tg_send_message(chat_id, "Неизвестный отдел.")
        return

    keyboard = []
    row = []

    for name in names:
        row.append({"text": name})
        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([{"text": MENU_CANCEL}])

    tg_send_message(chat_id, title, keyboard)

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

def parse_sheet_date(value):
    value = str(value).strip()
    for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    return None


def get_manager_stats_period():
    now = datetime.now()

    if now.day >= 10:
        start_date = datetime(now.year, now.month, 10)
        if now.month == 12:
            end_date = datetime(now.year + 1, 1, 10)
        else:
            end_date = datetime(now.year, now.month + 1, 10)
    else:
        if now.month == 1:
            start_date = datetime(now.year - 1, 12, 10)
        else:
            start_date = datetime(now.year, now.month - 1, 10)

        end_date = datetime(now.year, now.month, 10)

    return start_date, end_date

def build_manager_stats_text(username):
    if not username:
        return "У тебя не установлен username в Telegram, поэтому я не могу собрать личную статистику."

    username = username.strip().lstrip("@").lower()
    target_username = f"@{username}"

    start_date, end_date = get_manager_stats_period()

    # ---------- ЛИЧКИ ----------
    accounts_rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

    accounts_lines = []
    for row in accounts_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        account_number = str(row[0]).strip()
        transfer_date_raw = str(row[10]).strip()
        who_took = str(row[11]).strip().lower()
        for_whom = str(row[9]).strip()

        if who_took != target_username:
            continue

        transfer_date = parse_sheet_date(transfer_date_raw)
        if not transfer_date:
            continue

        if not (start_date <= transfer_date < end_date):
            continue

        accounts_lines.append(
            f"{account_number} | {transfer_date.strftime('%d/%m/%Y')} | {for_whom}"
        )

    # ---------- КИНГИ ----------
    kings_rows = get_sheet_rows_cached(SHEET_KINGS)

    kings_lines = []
    for row in kings_rows[1:]:
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

        king_name = str(row[0]).strip()
        transfer_date_raw = str(row[6]).strip()
        for_whom = str(row[5]).strip()
        who_took = str(row[8]).strip().lower()

        if who_took != target_username:
            continue

        transfer_date = parse_sheet_date(transfer_date_raw)
        if not transfer_date:
            continue

        if not (start_date <= transfer_date < end_date):
            continue

        kings_lines.append(
            f"{king_name} | {transfer_date.strftime('%d/%m/%Y')} | {for_whom}"
        )

    # ---------- БМы ----------
    bms_rows = get_sheet_rows_cached(SHEET_BMS)

    bms_lines = []
    for row in bms_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        bm_id = str(row[0]).strip()
        transfer_date_raw = str(row[7]).strip()
        for_whom = str(row[5]).strip()
        who_took = str(row[6]).strip().lower()

        if who_took != target_username:
            continue

        transfer_date = parse_sheet_date(transfer_date_raw)
        if not transfer_date:
            continue

        if not (start_date <= transfer_date < end_date):
            continue

        bms_lines.append(
            f"{bm_id} | {transfer_date.strftime('%d/%m/%Y')} | {for_whom}"
        )

    period_text = (
        f"Период: {start_date.strftime('%d/%m/%Y')} - "
        f"{(end_date).strftime('%d/%m/%Y')}"
    )

    text_parts = [f"Статистика менеджера {target_username}", period_text, ""]

    text_parts.append(f"Кинги: {len(kings_lines)}")
    if kings_lines:
        text_parts.extend(kings_lines)
    else:
        text_parts.append("нет выдач")

    text_parts.append("")
    text_parts.append(f"Лички: {len(accounts_lines)}")
    if accounts_lines:
        text_parts.extend(accounts_lines)
    else:
        text_parts.append("нет выдач")

    text_parts.append("")
    text_parts.append(f"БМы: {len(bms_lines)}")
    if bms_lines:
        text_parts.extend(bms_lines)
    else:
        text_parts.append("нет выдач")

    return "\n".join(text_parts)


def get_state(user_id):
    state = user_states.get(str(user_id))

    if not state:
        return {}

    state_time = state.get("_time", 0)

    if time.time() - state_time > STATE_TTL:
        clear_state(user_id)
        return {}

    return state

def cleanup_states():
    now = time.time()
    to_delete = []

    for uid, state in user_states.items():
        if now - state.get("_time", 0) > STATE_TTL:
            to_delete.append(uid)

    for uid in to_delete:
        user_states.pop(uid, None)


def set_state(user_id, data):
    data["_time"] = time.time()
    user_states[str(user_id)] = data


def clear_state(user_id):
    user_states.pop(str(user_id), None)

def find_account_in_base(account_number):
    rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 11:
            continue
        if str(row[0]).strip() == str(account_number).strip():
            return {
                "row_index": idx,
                "row": row
            }
    return None

def update_account_from_ocr(account_number, limit_bucket, threshold_bucket, gmt_value):
    found = find_account_in_base(account_number)

    if not found:
        return False, f"Личка {account_number} не найдена в таблице."

    row_index = found["row_index"]
    sheet_update_and_refresh(
        SHEET_ACCOUNTS,
        f"E{row_index}:G{row_index}",
        [[limit_bucket, threshold_bucket, gmt_value]]
    )

    invalidate_stats_cache()
    return True, (
        f"Обновлено ✅\n\n"
        f"Личка: {account_number}\n"
        f"Лимит: {limit_bucket}\n"
        f"Трешхолд: {threshold_bucket}\n"
        f"GMT: {gmt_value}"
    )

def find_last_issue_row(account_number):
    rows = get_sheet_rows_cached(SHEET_ISSUES)

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

    row = base_info["row"]

    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    status = str(row[8]).strip().lower()

    if status == "ban":
        return False, "Эта личка уже в ban."

    sheet_update_and_refresh(
        SHEET_ACCOUNTS,
        f"J{base_info['row_index']}",
        [["ban"]]
    )

    if issue_info:
        sheet_update_and_refresh(
            SHEET_ISSUES,
            f"G{issue_info['row_index']}",
            [["ban"]]
        )

    invalidate_stats_cache()
    return True, "Личка переведена в ban."


def build_account_search_text(account_number):
    base_info = find_account_in_base(account_number)
    if not base_info:
        return None

    issue_info = find_last_issue_row(account_number)

    row = base_info["row"]
    issue_row = issue_info["row"] if issue_info else None

    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    price = row[2] if len(row) > 2 else ""
    warehouses = row[7] if len(row) > 7 else ""
    date_taken = row[10] if len(row) > 10 else ""
    for_whom = row[9] if len(row) > 9 else ""
    who_took = row[11] if len(row) > 11 else ""

    banned = is_banned_account(row, issue_row)

    if not who_took:
        who_took = "не указано"

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
    rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

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
        "Формат:\n"
        "номер;дата покупки;цена;поставщик;склады\n\n"
        "Пример:\n"
        "RK001; 15/02/2026; 300; WD; sklad1,sklad2\n"
        "RK002; 16/02/2026; 500; WD; sklad3"
    )
    tg_send_message(chat_id, text)


def add_accounts_from_text(text):
    existing_rows = get_sheet_rows_cached(SHEET_ACCOUNTS)
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
        if len(fields) != 5:
            errors.append(f"Строка {i}: должно быть 5 полей через ';'")
            continue

        account_number, purchase_date_raw, price_raw, supplier, warehouses = fields

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

        to_append.append([
            account_number,                         # A номер
            purchase_date.strftime("%d/%m/%Y"),    # B дата покупки
            price,                                 # C цена
            supplier,                              # D поставщик
            "",                                    # E лимит
            "",                                    # F трешхолд
            "",                                    # G GMT
            warehouses,                            # H склады
            "free",                                # I статус
            "",                                    # J кому выдали
            "",                                    # K дата взятия
            ""                                     # L кто взял
        ])
        existing_accounts.add(account_number)

    if to_append:
        sheet_append_rows_and_refresh(SHEET_ACCOUNTS, to_append)
        invalidate_stats_cache()

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

def find_oldest_free_account(exclude_account=None):
    rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

    candidates = []

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        status = str(row[8]).strip().lower()

        if status != "free":
            continue

        if exclude_account and str(row[0]).strip() == exclude_account:
            continue

        purchase_date = parse_date(row[1]) or datetime.max
        currency = ""
        if len(row) > ACCOUNT_CURRENCY_COL:
            currency = str(row[ACCOUNT_CURRENCY_COL]).strip()

        candidates.append((idx, purchase_date, row, currency))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1])
    row_idx, _, row, currency = candidates[0]

    return {
        "row_index": row_idx,
        "account_number": row[0],
        "purchase_date": row[1],
        "price": row[2],
        "supplier": row[3],
        "warehouses": row[7],
        "currency": currency,
    }

# =========================
# ISSUE FLOW
# =========================
def find_matching_free_account(limit_val, threshold_val, gmt_val, currency, exclude_account=None):
    rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

    candidates = []
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) <= ACCOUNT_CURRENCY_COL:
            continue

        status = str(row[8]).strip().lower()
        row_currency = str(row[ACCOUNT_CURRENCY_COL]).strip()

        if status != "free":
            continue
        if str(row[4]).strip() != limit_val:
            continue
        if str(row[5]).strip() != threshold_val:
            continue
        if str(row[6]).strip() != gmt_val:
            continue
        if row_currency != currency:
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
        "currency": row[ACCOUNT_CURRENCY_COL],
    }


def show_found_account(chat_id, user_id, found):
    state = get_state(user_id)

    # если это быстрая выдача — сохраняем быстрый режим
    if state.get("mode") == "quick_account_found":
        state["mode"] = "quick_account_found"
    else:
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
        f"Валюта: {found.get('currency', '')}\n\n"
        f"Кому передали: {state['for_whom']}"
    )

    keyboard = [
        [{"text": BTN_ISSUE_CONFIRM}, {"text": BTN_ISSUE_NEXT}],
        [{"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)


def append_issue_row(account_number, purchase_date, price, transfer_date, supplier, for_whom):
    sheet_append_row_and_refresh(
        SHEET_ISSUES,
        [
            account_number,
            "РК",
            purchase_date,
            price,
            transfer_date,
            supplier,
            for_whom
        ]
    )


def confirm_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)
            
            if state.get("mode") not in ["account_found", "quick_account_found"]:
                send_main_menu(chat_id, "Сначала найди личку.", user_id=user_id)
                return

            row_index = state.get("found_row")
            if not row_index:
                send_main_menu(chat_id, "Не нашёл выбранную личку. Начни заново.", user_id=user_id)
                return

            rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_main_menu(chat_id, "Личка не найдена в таблице. Начни заново.", user_id=user_id)
                return

            row = rows[row_index - 1]

            if len(row) < 12:
                row = row + [''] * (12 - len(row))

            status = str(row[8]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_main_menu(chat_id, "Эта личка уже занята.", user_id=user_id)
                return

            if status == "ban":
                clear_state(user_id)
                send_main_menu(chat_id, "Эта личка уже в ban.", user_id=user_id)
                return

            if status != "free":
                clear_state(user_id)
                send_main_menu(chat_id, "Эта личка недоступна.", user_id=user_id)
                return

            account_number = row[0]
            purchase_date = row[1]
            price = row[2]
            supplier = row[3]
            today = datetime.now().strftime("%d/%m/%Y")

            who_took_text = f"@{username}" if username else "без username"

            sheet_update_and_refresh(
                SHEET_ACCOUNTS,
                f"I{row_index}:L{row_index}",
                [["taken", state["for_whom"], today, who_took_text]]
            )

            append_issue_row(
                account_number,
                purchase_date,
                price,
                today,
                supplier,
                state["for_whom"]
            )
            invalidate_stats_cache()

            clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"Выдана личка: {account_number}\n"
            f"Кому передали: {state['for_whom']}\n"
            f"Кто взял в боте: {who_took_text}"
        )
        send_main_menu(chat_id, "Выбери следующее действие:", user_id=user_id)

    except Exception as e:
        logging.error(f"confirm_issue error: {e}")
        tg_send_message(chat_id, "Ошибка выдачи лички. Попробуй ещё раз.")
        send_main_menu(chat_id, "Главное меню:", user_id=user_id)

def king_name_exists(king_name):
    rows = get_sheet_rows_cached(SHEET_KINGS)

    target = str(king_name).strip().lower()
    if not target:
        return False

    for row in rows[1:]:
        existing_name = str(row[0]).strip().lower() if len(row) > 0 else ""
        if existing_name == target:
            return True

    return False


def find_free_king_by_geo(geo, exclude_row=None):
    rows = get_sheet_rows_cached(SHEET_KINGS)

    candidates = []

    for idx, row in enumerate(rows[1:], start=2):

        if len(row) < 10:
            row = row + [''] * (10 - len(row))

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()

        if status != "free":
            continue

        if row_geo != geo:
            continue

        if exclude_row and idx == exclude_row:
            continue

        purchase_date = parse_date(row[1]) or datetime.max

        candidates.append({
            "row_index": idx,
            "purchase_date_obj": purchase_date,
            "purchase_date": row[1],
            "price": row[2],
            "supplier": row[3],
            "geo": row[7],
            "data_text": row[9]
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[0]

def find_free_bm(exclude_bm_id=None):
    rows = get_sheet_rows_cached(SHEET_BMS)

    candidates = []

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        bm_id = str(row[0]).strip()
        purchase_date_raw = str(row[1]).strip()
        status = str(row[4]).strip().lower()
        data_text = str(row[8]).strip()

        if status != "free":
            continue

        if exclude_bm_id and bm_id == exclude_bm_id:
            continue

        purchase_date = parse_date(purchase_date_raw) or datetime.max

        candidates.append({
            "row_index": idx,
            "bm_id": bm_id,
            "purchase_date_obj": purchase_date,
            "purchase_date": purchase_date_raw,
            "price": row[2],
            "supplier": row[3],
            "data_text": data_text
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[0]

def count_free_bms():
    rows = get_sheet_rows_cached(SHEET_BMS)

    count = 0
    for row in rows[1:]:
        if len(row) < 5:
            row = row + [''] * (5 - len(row))

        status = str(row[4]).strip().lower()  # E колонка
        if status == "free":
            count += 1

    return count

def find_bm_in_base(bm_id):
    rows = get_sheet_rows_cached(SHEET_BMS)

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        if str(row[0]).strip() == str(bm_id).strip():
            return {
                "row_index": idx,
                "row": row
            }

    return None


def build_bm_search_text(bm_id):
    bm_info = find_bm_in_base(bm_id)

    if not bm_info:
        return None

    row = bm_info["row"]

    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    bm_id = row[0]
    purchase_date = row[1] or "не указана"
    price = row[2] or "не указана"
    status = row[4] or "не указан"
    for_whom = row[5] or "не указано"
    who_took = row[6] or "не указано"
    issue_date = row[7] or "не указана"
    data_text = row[8] or "нет данных"

    text = (
        f"ID БМа: {bm_id}\n"
        f"Дата покупки: {purchase_date}\n"
        f"Цена: {price}\n"
        f"Статус: {status}\n"
        f"Для кого: {for_whom}\n"
        f"Кто взял: {who_took}\n"
        f"Дата выдачи: {issue_date}\n\n"
        f"Данные:\n{data_text}"
    )

    return text


def show_found_bm(chat_id, user_id, found):
    state = get_state(user_id)
    state["mode"] = "bm_found"
    state["bm_row"] = found["row_index"]
    state["found_bm_id"] = found["bm_id"]
    set_state(user_id, state)

    text = (
        "Найден БМ:\n\n"
        f"ID БМа: {found['bm_id']}\n"
        f"Для кого: {state['bm_for_whom']}"
    )

    keyboard = [
        [{"text": BTN_BM_CONFIRM}, {"text": BTN_BM_NEXT}],
        [{"text": BTN_BACK_TO_MENU}]
    ]

    tg_send_message(chat_id, text, keyboard)


def confirm_bm_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)

            if state.get("mode") != "bm_found":
                send_bms_menu(chat_id, "Сначала выбери БМ заново.")
                return

            row_index = state.get("bm_row")
            if not row_index:
                send_bms_menu(chat_id, "Не найден выбранный БМ. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_BMS)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_bms_menu(chat_id, "БМ не найден в таблице. Начни заново.")
                return

            row = rows[row_index - 1]

            if len(row) < 9:
                row = row + [''] * (9 - len(row))

            # A id БМа
            # B дата покупки
            # C цена
            # D у кого купили
            # E статус
            # F для кого
            # G кто взял
            # H дата выдачи
            # I данные

            status = str(row[4]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_bms_menu(chat_id, "Этот БМ уже занят.")
                return

            if status != "free":
                clear_state(user_id)
                send_bms_menu(chat_id, "Этот БМ недоступен.")
                return

            bm_id = row[0]
            purchase_date = row[1]
            price = row[2]
            supplier = row[3]
            today = datetime.now().strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            sheet_update_and_refresh(
                SHEET_BMS,
                f"E{row_index}:H{row_index}",
                [[
                    "taken",
                    state["bm_for_whom"],
                    who_took_text,
                    today
                ]]
            )

            sheet_append_row_and_refresh(
                SHEET_ISSUES,
                [
                    bm_id,
                    "БМ",
                    purchase_date,
                    price,
                    today,
                    supplier,
                    state["bm_for_whom"]
                ]
            )

            data_text = row[8] if len(row) > 8 else ""
            invalidate_stats_cache()
            clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"БМ выдан.\n"
            f"ID БМа: {bm_id}\n"
            f"Для кого: {state['bm_for_whom']}\n"
            f"Кто взял в боте: {who_took_text}"
        )

        if data_text:
            tg_send_message(chat_id, data_text)
        else:
            tg_send_message(chat_id, "Данные БМа не найдены.")

        send_bms_menu(chat_id, "Выбери следующее действие:")

    except Exception as e:
        logging.exception("confirm_bm_issue crashed")
        tg_send_message(chat_id, "Ошибка выдачи БМа. Попробуй ещё раз.")
        send_bms_menu(chat_id, "Меню БМов:")

    
def show_found_king(chat_id, user_id, found):
    state = get_state(user_id)

    state["mode"] = "king_found"
    state["king_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "Найден кинг:\n\n"
        f"Дата покупки: {found['purchase_date']}\n"
        f"Цена: {found['price']}\n"
        f"Гео: {found['geo']}\n"
        f"Для кого: {state['king_for_whom']}\n"
        f"Название: {state['king_name']}"
    )

    keyboard = [
        [{"text": BTN_KING_CONFIRM}, {"text": BTN_KING_NEXT}],
        [{"text": MENU_CANCEL}]
    ]

    tg_send_message(chat_id, text, keyboard)

def confirm_king_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)

            if state.get("mode") != "king_found":
                send_kings_menu(chat_id, "Сначала выбери кинга заново.")
                return

            row_index = state.get("king_row")
            if not row_index:
                send_kings_menu(chat_id, "Не найден выбранный кинг. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_KINGS)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_kings_menu(chat_id, "Кинг не найден в таблице. Начни заново.")
                return

            row = rows[row_index - 1]

            if len(row) < 10:
                row = row + [''] * (10 - len(row))

            status = str(row[4]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот кинг уже занят.")
                return

            if status == "ban":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот кинг уже в ban.")
                return

            if status != "free":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот кинг недоступен.")
                return

            king_name = state["king_name"].strip()

            current_name_in_row = str(row[0]).strip()
            if not current_name_in_row and king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое название.")
                state["mode"] = "awaiting_king_name"
                set_state(user_id, state)
                return

            today = datetime.now().strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            sheet_update_and_refresh(
                SHEET_KINGS,
                f"A{row_index}:I{row_index}",
                [[
                    king_name,
                    row[1],
                    row[2],
                    row[3],
                    "taken",
                    state["king_for_whom"],
                    today,
                    row[7],
                    who_took_text
                ]]
            )

            append_king_to_issues_sheet(
                king_name=king_name,
                purchase_date=row[1],
                price=row[2],
                transfer_date=today,
                supplier=row[3],
                for_whom=state["king_for_whom"]
            )
            invalidate_stats_cache()

            data_text = row[9] if len(row) > 9 else ""

            clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"Кинг выдан.\n"
            f"Название: {king_name}\n"
            f"Для кого: {state['king_for_whom']}\n"
            f"Гео: {row[7]}"
        )

        if data_text:
            tg_send_message(chat_id, data_text)
        else:
            tg_send_message(chat_id, "Данные кинга не найдены.")

        send_kings_menu(chat_id, "Выбери следующее действие:")

    except Exception as e:
        logging.error(f"confirm_king_issue error: {e}")
        tg_send_message(chat_id, "Ошибка выдачи кинга. Попробуй ещё раз.")
        send_kings_menu(chat_id, "Меню кингов:")

def append_king_to_issues_sheet(king_name, purchase_date, price, transfer_date, supplier, for_whom):
    sheet_append_row_and_refresh(
        SHEET_ISSUES,
        [
            king_name,
            "KING",
            purchase_date,
            price,
            transfer_date,
            supplier,
            for_whom
        ]
    )

def find_last_king_issue_row(king_name):
    rows = get_sheet_rows_cached(SHEET_ISSUES)

    last_match = None
    target = str(king_name).strip().lower()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 7:
            row = row + [''] * (7 - len(row))

        issue_name = str(row[0]).strip().lower()
        issue_type = str(row[1]).strip().lower()

        if issue_name == target and issue_type == "king":
            last_match = {
                "row_index": idx,
                "row": row
            }

    return last_match


def find_king_in_base_by_name(king_name):
    rows = get_sheet_rows_cached(SHEET_KINGS)

    target = str(king_name).strip().lower()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

        existing_name = str(row[0]).strip().lower()

        if existing_name == target:
            return {
                "row_index": idx,
                "row": row
            }

    return None


def return_king_to_ban(king_name):
    base_info = find_king_in_base_by_name(king_name)
    if not base_info:
        return False, "Кинг не найден в База_кингов."

    row = base_info["row"]

    if len(row) < 10:
        row = row + [''] * (10 - len(row))

    status = str(row[4]).strip().lower()

    if status == "ban":
        return False, "Этот кинг уже в ban."


    # E = статус, F = кому выдали
    sheet_update_and_refresh(
        SHEET_KINGS,
        f"E{base_info['row_index']}:F{base_info['row_index']}",
        [["ban", "ban"]]
    )

    issue_info = find_last_king_issue_row(king_name)
    if issue_info:
        sheet_update_and_refresh(
            SHEET_ISSUES,
            f"G{issue_info['row_index']}",
            [["ban"]]
        )

    invalidate_stats_cache()
    return True, f"Кинг '{king_name}' переведён в ban."

def build_king_search_text(king_name):
    king_info = find_king_in_base_by_name(king_name)

    if not king_info:
        return None

    row = king_info["row"]

    if len(row) < 10:
        row = row + [''] * (10 - len(row))

    name = row[0]
    price = row[2]
    status = row[4]
    for_whom = row[5]
    taken_date = row[6]
    who_took = row[8]
    data_text = row[9]

    if not who_took:
        who_took = "не указано"

    if not for_whom:
        for_whom = "не указано"

    if not taken_date:
        taken_date = "не указана"

    if not status:
        status = "не указан"

    if not data_text:
        data_text = "нет данных"

    text = (
        f"Название: {name}\n"
        f"Статус: {status}\n"
        f"Цена: {price}\n"
        f"Дата взятия: {taken_date}\n"
        f"Кто взял: {who_took}\n"
        f"Для кого взял: {for_whom}\n\n"
        f"Данные:\n{data_text}"
    )

    return text

def build_stats_text():

    # ---------- КИНГИ ----------
    kings_rows = get_sheet_rows_cached(SHEET_KINGS)

    kings_free = 0
    kings_taken = 0
    kings_ban = 0
    kings_geo_stats = {}

    for row in kings_rows[1:]:
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

        status = str(row[4]).strip().lower()
        geo = str(row[7]).strip()

        if status == "free":
            kings_free += 1
            if geo:
                kings_geo_stats[geo] = kings_geo_stats.get(geo, 0) + 1
        elif status == "taken":
            kings_taken += 1
        elif status == "ban":
            kings_ban += 1

    # ---------- ЛИЧКИ ----------
    accounts_rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

    accounts_free = 0
    accounts_taken = 0
    accounts_ban = 0

    limit_stats = {
        "-250": 0,
        "250-500": 0,
        "500-1200": 0,
        "1200-1500": 0,
        "unlim": 0
    }

    for row in accounts_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        status = str(row[8]).strip().lower()
        target = str(row[9]).strip().lower()
        limit_val = str(row[4]).strip()

        if target == "ban" or status == "ban":
            accounts_ban += 1
        elif status == "free":
            accounts_free += 1
            if limit_val in limit_stats:
                limit_stats[limit_val] += 1
        elif status == "taken":
            accounts_taken += 1

    # ---------- БМы ----------
    bms_rows = get_sheet_rows_cached(SHEET_BMS)

    bms_free = 0
    bms_taken = 0

    for row in bms_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        status = str(row[4]).strip().lower()

        if status == "free":
            bms_free += 1
        elif status == "taken":
            bms_taken += 1

    geo_lines = []
    for geo, count in sorted(kings_geo_stats.items()):
        geo_lines.append(f"{geo}: {count}")

    if not geo_lines:
        geo_lines.append("нет свободных GEO")

    limit_lines = []
    for limit_name in ["-250", "250-500", "500-1200", "1200-1500", "unlim"]:
        limit_lines.append(f"{limit_name}: {limit_stats[limit_name]}")

    text = (
        "Кинги:\n\n"
        f"Свободные: {kings_free}\n"
        f"Выдано: {kings_taken}\n"
        f"В бане: {kings_ban}\n\n"
        "По GEO:\n\n"
        + "\n".join(geo_lines)
        + "\n\n"
        "Лички:\n\n"
        f"Свободные: {accounts_free}\n"
        f"Выдано: {accounts_taken}\n"
        f"В бане: {accounts_ban}\n\n"
        "По лимиту:\n"
        + "\n".join(limit_lines)
        + "\n\n"
        "БМы:\n\n"
        f"Свободные: {bms_free}\n"
        f"Выдано: {bms_taken}"
    )

    return text


def send_free_kings(chat_id):
    rows = get_sheet_rows_cached(SHEET_KINGS)

    free_rows = []

    for row in rows[1:]:
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

        status = str(row[4]).strip().lower()

        if status == "free":
            free_rows.append(row)

    if not free_rows:
        tg_send_message(chat_id, "Свободных кингов сейчас нет.")
        return

    geo_stats = {}
    for row in free_rows:
        geo = str(row[7]).strip()
        if geo:
            geo_stats[geo] = geo_stats.get(geo, 0) + 1

    lines = [f"{geo} — {count}" for geo, count in sorted(geo_stats.items())]

    text = (
        f"Свободные кинги: {len(free_rows)}\n\n"
        + "\n".join(lines)
    )

    tg_send_message(chat_id, text)
    
def backup_tables():
    global last_backup_date

    with backup_lock:
        try:
            client = get_gspread_client()

            main_spreadsheet = client.open_by_key(SPREADSHEET_ID)
            backup_spreadsheet = client.open_by_key(BACKUP_SPREADSHEET_ID)

            accounts = main_spreadsheet.worksheet(SHEET_ACCOUNTS)
            kings = main_spreadsheet.worksheet(SHEET_KINGS)
            issues = main_spreadsheet.worksheet(SHEET_ISSUES)
            bms = main_spreadsheet.worksheet(SHEET_BMS)

            backup_accounts = backup_spreadsheet.worksheet("backup_accounts")
            backup_kings = backup_spreadsheet.worksheet("backup_kings")
            backup_issues = backup_spreadsheet.worksheet("backup_issues")
            backup_bms = backup_spreadsheet.worksheet("backup_bms")

            accounts_data = accounts.get_all_values()
            kings_data = kings.get_all_values()
            issues_data = issues.get_all_values()
            bms_data = bms.get_all_values()

            backup_accounts.clear()
            backup_kings.clear()
            backup_issues.clear()
            backup_bms.clear()

            if accounts_data:
                backup_accounts.append_rows(accounts_data)

            if kings_data:
                backup_kings.append_rows(kings_data)

            if issues_data:
                backup_issues.append_rows(issues_data)

            if bms_data:
                backup_bms.append_rows(bms_data)

            last_backup_date = datetime.now(MOSCOW_TZ).date()

            reset_google_cache()
            reset_table_cache()
            logging.info("Daily backup completed successfully")
            return True

        except Exception as e:
            logging.error(f"Backup error: {e}")
            reset_google_cache()
            reset_table_cache()
            return False

def backup_scheduler_loop():
    global last_backup_date

    while True:
        try:
            touch_background_heartbeat()

            now_msk = datetime.now(MOSCOW_TZ)
            today_msk = now_msk.date()

            if now_msk.hour == 0 and now_msk.minute == 0 and last_backup_date != today_msk:
                logging.info("Starting scheduled daily backup")
                backup_tables()

            time.sleep(30)

        except Exception as e:
            logging.error(f"backup_scheduler_loop error: {e}")
            time.sleep(30)

def watchdog_loop():
    while True:
        try:
            now = time.time()

            request_stale = now - last_request_time > WATCHDOG_TIMEOUT
            background_stale = now - last_background_time > WATCHDOG_TIMEOUT

            if request_stale and background_stale:
                logging.error("Watchdog detected full app stall. Exiting for restart.")
                os._exit(1)

            time.sleep(30)

        except Exception as e:
            logging.error(f"watchdog_loop error: {e}")
            time.sleep(30)
            
# =========================
# MESSAGE HANDLER
# =========================
def handle_photo_message(msg):
    try:
        touch_request_heartbeat()

        chat_id = msg["chat"]["id"]
        user_id = msg["from"]["id"]

        if not has_access(user_id):
            tg_send_message(
                chat_id,
                f"⛔ У вас нет доступа.\n\nВаш Telegram ID:\n{user_id}"
            )
            return

        state = get_state(user_id)

        if state.get("mode") != "awaiting_smit_screenshot":
            tg_send_message(chat_id, "Я сейчас не жду скриншот. Сначала зайди в Admin → Импорт из скрина.")
            return

        photo_list = msg.get("photo", [])
        if not photo_list:
            tg_send_message(chat_id, "Фото не найдено. Попробуй ещё раз.")
            return

        image_bytes = tg_download_photo_content(photo_list)
        if not image_bytes:
            tg_send_message(chat_id, "Не удалось скачать фото. Попробуй ещё раз.")
            return

        parsed_text = run_ocr_space(image_bytes)
        parsed = parse_smit_ocr_text(parsed_text)

        account_number = parsed.get("account_number")
        limit_bucket = parsed.get("limit_bucket")
        threshold_bucket = parsed.get("threshold_bucket")
        gmt_value = parsed.get("gmt_value")

        if not account_number:
            tg_send_message(chat_id, "Не удалось найти номер лички на скриншоте.")
            return

        if not limit_bucket:
            tg_send_message(chat_id, "Не удалось распознать лимит на скриншоте.")
            return

        if not threshold_bucket:
            tg_send_message(chat_id, "Не удалось распознать трешхолд на скриншоте.")
            return

        if not gmt_value:
            tg_send_message(chat_id, "Не удалось распознать GMT / Account Time Zone на скриншоте.")
            return

        set_state(user_id, {
            "mode": "awaiting_ocr_confirm",
            "ocr_account_number": account_number,
            "ocr_limit_bucket": limit_bucket,
            "ocr_threshold_bucket": threshold_bucket,
            "ocr_gmt_value": gmt_value
        })

        keyboard = [
            [{"text": BTN_OCR_CONFIRM}],
            [{"text": BTN_OCR_REJECT}]
        ]

        tg_send_message(
            chat_id,
            "Нашёл на скриншоте:\n\n"
            f"Личка: {account_number}\n"
            f"Лимит: {parsed.get('limit_raw')} -> {limit_bucket}\n"
            f"Трешхолд: {parsed.get('threshold_raw')} -> {threshold_bucket}\n"
            f"GMT: {parsed.get('gmt_raw')} -> {gmt_value}\n\n"
            "Подтвердить обновление?",
            keyboard
        )

    except Exception as e:
        logging.exception("handle_photo_message crashed")
        try:
            tg_send_message(msg["chat"]["id"], f"Ошибка OCR: {e}")
        except Exception:
            pass
            
def handle_message(msg):
    try:
        cleanup_states()
        touch_request_heartbeat()

        chat_id = msg["chat"]["id"]
        user_id = msg["from"]["id"]

        now = time.time()
        last = last_user_action.get(user_id, 0)
        if now - last < ACTION_COOLDOWN:
            return

        last_user_action[user_id] = now
        username = msg["from"].get("username", "")
        text = str(msg.get("text", "")).strip()

        if not has_access(user_id):
            tg_send_message(
                chat_id,
                f"⛔ У вас нет доступа.\n\nВаш Telegram ID:\n{user_id}"
            )
            return

        state = get_state(user_id)

        # ========= БАЗОВЫЕ КОМАНДЫ =========
        if text in ["/start", "/menu"]:
            clear_state(user_id)
            send_main_menu(chat_id, user_id=user_id)
            return

        if text == "/id":
            tg_send_message(chat_id, f"Ваш Telegram ID: {user_id}")
            return

        if text == "/help":
            clear_state(user_id)
            tg_send_message(
                chat_id,
                "/start — открыть меню\n"
                "/menu — открыть меню\n"
                "/help — помощь"
            )
            send_main_menu(chat_id, user_id=user_id)
            return

        if text == "/ping":
            tg_send_message(chat_id, "бот работает")
            return

        if text == MENU_CANCEL:
            clear_state(user_id)
            send_main_menu(chat_id, "Действие отменено.", user_id=user_id)
            return

        # ========= ГЛАВНОЕ МЕНЮ =========
        if text == MENU_STATS:
            stats_text = build_stats_text()
            tg_send_message(chat_id, stats_text)
            send_main_menu(chat_id, "Главное меню:", user_id=user_id)
            return

        if text == MENU_MANAGER_STATS:
            manager_stats_text = build_manager_stats_text(username)
            tg_send_message(chat_id, manager_stats_text)
            send_main_menu(chat_id, "Главное меню:", user_id=user_id)
            return

        if text == MENU_ADMIN:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа к меню Admin.")
                return

            clear_state(user_id)
            send_admin_menu(chat_id)
            return

        if text == BTN_BACK_FROM_ADMIN:
            clear_state(user_id)
            send_main_menu(chat_id, user_id=user_id)
            return

        if text == MENU_ACCOUNTS:
            clear_state(user_id)
            send_accounts_menu(chat_id)
            return

        if text == MENU_KINGS:
            clear_state(user_id)
            send_kings_menu(chat_id)
            return

        if text == MENU_BMS:
            clear_state(user_id)
            send_bms_menu(chat_id)
            return

        if text == BTN_BACK_TO_MENU:
            clear_state(user_id)
            send_main_menu(chat_id, user_id=user_id)
            return

        # ========= ADMIN =========
        if text == ADMIN_BACKUP:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            ok = backup_tables()

            if ok:
                tg_send_message(chat_id, "Бэкап успешно создан.")
            else:
                tg_send_message(chat_id, "Ошибка создания бэкапа.")

            send_admin_menu(chat_id, "Меню Admin:")
            return

        if text == ADMIN_ADD_ACCOUNTS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_bulk_add"})
            send_bulk_add_instructions(chat_id)
            return

        if text == ADMIN_ADD_KINGS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_kings_txt"})
            send_add_kings_instructions(chat_id)
            return

        if text == ADMIN_ADD_BMS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_bms_text"})
            send_add_bms_instructions(chat_id)
            return

        if text == ADMIN_IMPORT_SCREEN:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_smit_screenshot"})
            tg_send_message(
                chat_id,
                "Пришли скриншот из SMIT.\n\n"
                "На скриншоте должны быть видны:\n"
                "- номер лички\n"
                "- Threshold\n"
                "- Limit\n"
                "- Account Time Zone / GMT"
            )
            return

        if text == BTN_OCR_REJECT:
            if state.get("mode") == "awaiting_ocr_confirm":
                clear_state(user_id)
                send_admin_menu(chat_id, "Импорт из скрина отменён.")
                return

        if text == BTN_OCR_CONFIRM:
            if state.get("mode") != "awaiting_ocr_confirm":
                send_admin_menu(chat_id, "Сначала начни импорт заново.")
                return

            ok, result_text = update_account_from_ocr(
                state.get("ocr_account_number"),
                state.get("ocr_limit_bucket"),
                state.get("ocr_threshold_bucket"),
                state.get("ocr_gmt_value")
            )

            clear_state(user_id)
            tg_send_message(chat_id, result_text)
            send_admin_menu(chat_id, "Выбери следующее действие:")
            return

        # ========= ЛИЧКИ =========
        if text == SUBMENU_FREE:
            clear_state(user_id)
            send_free_accounts(chat_id)
            send_accounts_menu(chat_id, "Выбери следующее действие:")
            return

        if text == SUBMENU_SEARCH:
            set_state(user_id, {"mode": "awaiting_search_account"})
            tg_send_message(chat_id, "Впиши номер лички для поиска.")
            return

        if text == SUBMENU_RETURN:
            set_state(user_id, {"mode": "awaiting_return_account"})
            tg_send_message(chat_id, "Впиши номер лички, которую нужно отправить в ban.")
            return

        if text == SUBMENU_GET:
            clear_state(user_id)
            set_state(user_id, {"mode": "awaiting_issue_department"})
            send_department_menu(chat_id, "Выбери для кого личка:")
            return

        if text == SUBMENU_QUICK_GET:
            clear_state(user_id)
            set_state(user_id, {"mode": "awaiting_quick_issue_department"})
            send_department_menu(chat_id, "Выбери для кого быстро выдать личку:")
            return

        if text == BTN_ISSUE_CONFIRM:
            confirm_issue(chat_id, user_id, username)
            return

        if text == BTN_ISSUE_NEXT:
            if not state:
                send_main_menu(chat_id, "Начни заново.", user_id=user_id)
                return

            if state.get("mode") == "quick_account_found":
                found = find_oldest_free_account(
                    exclude_account=state.get("found_account")
                )

                if not found:
                    clear_state(user_id)
                    send_accounts_menu(chat_id, "Свободных личек больше нет.")
                    return

                show_found_account(chat_id, user_id, found)
                return

            found_account_number = state.get("found_account")
            if not found_account_number:
                clear_state(user_id)
                send_accounts_menu(chat_id, "Начни выдачу заново.")
                return

            found = find_oldest_free_account(exclude_account=found_account_number)

            if not found:
                clear_state(user_id)
                send_accounts_menu(chat_id, "Свободных личек больше нет.")
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

        # ========= КИНГИ =========
        if text == SUBMENU_FREE_KINGS:
            send_free_kings(chat_id)
            send_kings_menu(chat_id, "Выбери следующее действие:")
            return

        if text == SUBMENU_SEARCH_KING:
            set_state(user_id, {"mode": "awaiting_search_king_name"})
            tg_send_message(chat_id, "Впиши название кинга.")
            return

        if text == SUBMENU_RETURN_KING:
            set_state(user_id, {"mode": "awaiting_return_king_name"})
            tg_send_message(chat_id, "Впиши название кинга, который нужно перевести в ban.")
            return

        if text == SUBMENU_GET_KINGS:
            clear_state(user_id)
            set_state(user_id, {"mode": "awaiting_king_geo"})
            send_king_geo_options(chat_id)
            return

        if text == BTN_KING_CONFIRM:
            confirm_king_issue(chat_id, user_id, username)
            return

        if text == BTN_KING_NEXT:
            if not state or not state.get("king_geo"):
                send_kings_menu(chat_id, "Начни заново.")
                return

            found = find_free_king_by_geo(
                state["king_geo"],
                exclude_row=state.get("king_row")
            )

            if not found:
                clear_state(user_id)
                send_kings_menu(chat_id, "Свободных кингов с таким GEO больше нет.")
                return

            show_found_king(chat_id, user_id, found)
            return

        if text == BTN_KING_BAN_CONFIRM:
            if state.get("mode") != "awaiting_return_king_confirm":
                send_kings_menu(chat_id, "Сначала выбери действие заново.")
                return

            king_name = state.get("return_king_name", "")
            ok, message = return_king_to_ban(king_name)

            clear_state(user_id)
            send_kings_menu(chat_id, message)
            return

        # ========= БМы =========
        if text == SUBMENU_FREE_BMS:
            free_count = count_free_bms()
            tg_send_message(chat_id, f"Свободных БМов: {free_count}")
            send_bms_menu(chat_id, "Выбери следующее действие:")
            return

        if text == SUBMENU_SEARCH_BM:
            set_state(user_id, {"mode": "awaiting_search_bm"})
            tg_send_message(chat_id, "Впиши ID БМа для поиска.")
            return

        if text == SUBMENU_GET_BM:
            clear_state(user_id)
            set_state(user_id, {"mode": "awaiting_bm_department"})
            send_department_menu(chat_id, "Выбери для кого БМ:")
            return

        if text == BTN_BM_CONFIRM:
            confirm_bm_issue(chat_id, user_id, username)
            return

        if text == BTN_BM_NEXT:
            if not state:
                send_bms_menu(chat_id, "Начни заново.")
                return

            found = find_free_bm(exclude_bm_id=state.get("found_bm_id"))

            if not found:
                clear_state(user_id)
                send_bms_menu(chat_id, "Свободных БМов больше нет.")
                return

            show_found_bm(chat_id, user_id, found)
            return

        # ========= СОСТОЯНИЯ: ДОБАВЛЕНИЕ =========
        if state.get("mode") == "awaiting_bulk_add":
            result = add_accounts_from_text(text)
            clear_state(user_id)
            tg_send_message(chat_id, result)

            if is_admin(user_id):
                send_admin_menu(chat_id, "Выбери следующее действие:")
            else:
                send_accounts_menu(chat_id, "Готово. Выбери следующее действие:")
            return

        if state.get("mode") == "awaiting_bms_text":
            result = add_bms_from_txt_content(text)
            clear_state(user_id)
            tg_send_message(chat_id, result)

            if is_admin(user_id):
                send_admin_menu(chat_id, "Выбери следующее действие:")
            else:
                send_main_menu(chat_id, "Готово. Выбери следующее действие:", user_id=user_id)
            return

        # ========= СОСТОЯНИЯ: ПОИСК / ВОЗВРАТ ЛИЧЕК =========
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

        # ========= СОСТОЯНИЯ: ВЫДАЧА ЛИЧЕК =========
        if state.get("mode") == "awaiting_issue_department":
            if text not in [DEPT_CRYPTO, DEPT_GAMBLA]:
                send_department_menu(chat_id, "Нужно выбрать отдел кнопкой:")
                return

            state["mode"] = "awaiting_issue_for_whom"
            state["issue_department"] = text
            set_state(user_id, state)

            send_person_menu(chat_id, text)
            return

        if state.get("mode") == "awaiting_issue_for_whom":
            allowed_names = []

            if state.get("issue_department") == DEPT_CRYPTO:
                allowed_names = CRYPTO_NAMES
            elif state.get("issue_department") == DEPT_GAMBLA:
                allowed_names = GAMBLA_NAMES

            if text not in allowed_names:
                send_person_menu(chat_id, state.get("issue_department"))
                return

            set_state(user_id, {
                "mode": "awaiting_issue_account_number",
                "for_whom": text,
                "issue_department": state.get("issue_department")
            })

            tg_send_message(chat_id, "Теперь напиши номер лички, которую хочешь выдать.")
            return

        if state.get("mode") == "awaiting_issue_account_number":
            account_number = text.strip()

            if not account_number:
                tg_send_message(chat_id, "Впиши номер лички.")
                return

            found = find_account_in_base(account_number)

            if not found:
                tg_send_message(chat_id, "Личка не найдена. Впиши номер ещё раз.")
                return

            row = found["row"]

            if len(row) < 12:
                row = row + [''] * (12 - len(row))

            status = str(row[8]).strip().lower()

            if status == "taken":
                tg_send_message(chat_id, "Эта личка уже занята. Впиши другую.")
                return

            if status == "ban":
                tg_send_message(chat_id, "Эта личка в ban. Впиши другую.")
                return

            if status != "free":
                tg_send_message(chat_id, "Эта личка недоступна. Впиши другую.")
                return

            found_data = {
                "row_index": found["row_index"],
                "account_number": row[0],
                "purchase_date": row[1],
                "price": row[2],
                "supplier": row[3],
                "warehouses": row[7],
                "currency": ""
            }

            show_found_account(chat_id, user_id, found_data)
            return

        if state.get("mode") == "awaiting_quick_issue_department":
            if text not in [DEPT_CRYPTO, DEPT_GAMBLA]:
                send_department_menu(chat_id, "Нужно выбрать отдел кнопкой:")
                return

            state["mode"] = "awaiting_quick_issue_for_whom"
            state["issue_department"] = text
            set_state(user_id, state)

            send_person_menu(chat_id, text)
            return

        if state.get("mode") == "awaiting_quick_issue_for_whom":
            allowed_names = []

            if state.get("issue_department") == DEPT_CRYPTO:
                allowed_names = CRYPTO_NAMES
            elif state.get("issue_department") == DEPT_GAMBLA:
                allowed_names = GAMBLA_NAMES

            if text not in allowed_names:
                send_person_menu(chat_id, state.get("issue_department"))
                return

            state["mode"] = "quick_account_found"
            state["for_whom"] = text
            set_state(user_id, state)

            found = find_oldest_free_account()

            if not found:
                clear_state(user_id)
                send_accounts_menu(chat_id, "Свободных личек сейчас нет.")
                return

            show_found_account(chat_id, user_id, found)
            return

        # ========= СОСТОЯНИЯ: КИНГИ =========
        if state.get("mode") == "awaiting_king_geo":
            geos = get_free_king_geos()

            if text not in geos:
                send_king_geo_options(chat_id)
                return

            set_state(user_id, {
                "mode": "awaiting_king_department",
                "king_geo": text
            })

            send_department_menu(chat_id, "Выбери для кого кинг:")
            return

        if state.get("mode") == "awaiting_king_department":
            if text not in [DEPT_CRYPTO, DEPT_GAMBLA]:
                send_department_menu(chat_id, "Нужно выбрать отдел кнопкой:")
                return

            state["mode"] = "awaiting_king_for_whom"
            state["king_department"] = text
            set_state(user_id, state)

            send_person_menu(chat_id, text)
            return

        if state.get("mode") == "awaiting_king_for_whom":
            allowed_names = []

            if state.get("king_department") == DEPT_CRYPTO:
                allowed_names = CRYPTO_NAMES
            elif state.get("king_department") == DEPT_GAMBLA:
                allowed_names = GAMBLA_NAMES

            if text not in allowed_names:
                send_person_menu(chat_id, state.get("king_department"))
                return

            state["mode"] = "awaiting_king_name"
            state["king_for_whom"] = text
            set_state(user_id, state)

            tg_send_message(chat_id, "Какое название будет у кинга?")
            return

        if state.get("mode") == "awaiting_king_name":
            king_name = text.strip()

            if not king_name:
                tg_send_message(chat_id, "Напиши название кинга.")
                return

            if king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое.")
                return

            state["mode"] = "searching_king"
            state["king_name"] = king_name
            set_state(user_id, state)

            found = find_free_king_by_geo(state["king_geo"])

            if not found:
                clear_state(user_id)
                send_kings_menu(chat_id, "Свободных кингов с таким GEO нет.")
                return

            show_found_king(chat_id, user_id, found)
            return

        if state.get("mode") == "awaiting_search_king_name":
            king_name = text.strip()

            if not king_name:
                tg_send_message(chat_id, "Впиши название кинга.")
                return

            result = build_king_search_text(king_name)
            clear_state(user_id)

            if not result:
                send_kings_menu(chat_id, "Кинг не найден.")
                return

            tg_send_message(chat_id, result)
            send_kings_menu(chat_id, "Выбери следующее действие:")
            return

        if state.get("mode") == "awaiting_return_king_name":
            king_name = text.strip()

            if not king_name:
                tg_send_message(chat_id, "Впиши название кинга.")
                return

            king_info = find_king_in_base_by_name(king_name)
            if not king_info:
                clear_state(user_id)
                send_kings_menu(chat_id, "Кинг не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_return_king_confirm",
                "return_king_name": king_name
            })

            keyboard = [
                [{"text": BTN_KING_BAN_CONFIRM}],
                [{"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                f"Внимание: кинг '{king_name}' будет перемещён в ban.\nПодтвердить?",
                keyboard
            )
            return

        # ========= СОСТОЯНИЯ: БМы =========
        if state.get("mode") == "awaiting_bm_department":
            if text not in [DEPT_CRYPTO, DEPT_GAMBLA]:
                send_department_menu(chat_id, "Нужно выбрать отдел кнопкой:")
                return

            state["mode"] = "awaiting_bm_for_whom"
            state["bm_department"] = text
            set_state(user_id, state)

            send_person_menu(chat_id, text)
            return

        if state.get("mode") == "awaiting_bm_for_whom":
            allowed_names = []

            if state.get("bm_department") == DEPT_CRYPTO:
                allowed_names = CRYPTO_NAMES
            elif state.get("bm_department") == DEPT_GAMBLA:
                allowed_names = GAMBLA_NAMES

            if text not in allowed_names:
                send_person_menu(chat_id, state.get("bm_department"))
                return

            state["bm_for_whom"] = text
            set_state(user_id, state)

            found = find_free_bm()

            if not found:
                clear_state(user_id)
                send_bms_menu(chat_id, "Свободных БМов сейчас нет.")
                return

            show_found_bm(chat_id, user_id, found)
            return

        if state.get("mode") == "awaiting_search_bm":
            bm_id = text.strip()

            if not bm_id:
                tg_send_message(chat_id, "Впиши ID БМа.")
                return

            result = build_bm_search_text(bm_id)
            clear_state(user_id)

            if not result:
                send_bms_menu(chat_id, "БМ не найден.")
                return

            tg_send_message(chat_id, result)
            send_bms_menu(chat_id, "Выбери следующее действие:")
            return

        send_main_menu(chat_id, "Не понял команду. Выбери кнопку из меню:", user_id=user_id)

    except Exception as e:
        logging.exception("handle_message crashed")
        try:
            error_text = str(e)

            if "Google Sheets временно перегружен" in error_text:
                tg_send_message(chat_id, error_text)
            else:
                tg_send_message(chat_id, "Произошла ошибка. Попробуй ещё раз.")

            send_main_menu(chat_id, "Главное меню:", user_id=user_id)
        except Exception:
            pass

# =========================
# FLASK
# =========================
@app.route("/", methods=["GET"])
def index():
    return "ok", 200

@app.route("/health", methods=["GET"])
def health():
    return "healthy", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(silent=True) or {}

        msg = update.get("message") or update.get("edited_message")

        if msg:
            if msg.get("text"):
                handle_message(msg)

            elif msg.get("document"):
                handle_document_message(msg)

            elif msg.get("photo"):
                handle_photo_message(msg)

        return jsonify({"ok": True})

    except Exception as e:
        logging.error(f"webhook error: {e}")
        return jsonify({"ok": True})

if __name__ == "__main__":
    backup_thread = threading.Thread(target=backup_scheduler_loop, daemon=True)
    backup_thread.start()

    watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
    watchdog_thread.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
