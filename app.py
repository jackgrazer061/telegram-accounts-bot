from flask_cors import CORS
import os
import json
import logging
from datetime import datetime
import time
import re
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, make_response
import requests
import gspread
from google.oauth2.service_account import Credentials
import threading
import uuid
import tempfile

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)

# =========================
# ENV
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
BACKUP_SPREADSHEET_ID = os.environ.get("BACKUP_SPREADSHEET_ID", "")
BASEBOT_SPREADSHEET_ID = os.environ.get("BASEBOT_SPREADSHEET_ID", "")
EXCHANGE_API_BASE = os.environ.get("EXCHANGE_API_BASE", "https://api.exchangerate.host")
OCTO_API_TOKEN = os.environ.get("OCTO_API_TOKEN", "").strip()
OCTO_API_BASE = "https://app.octobrowser.net/api/v2/automation"
OCTO_FP_TEMPLATE_ID = os.environ.get("OCTO_FP_TEMPLATE_ID", "").strip()
OCTO_TAG_SIDO = "Sido"
OCTO_TAG_CORBY = "corby"
OCTO_TAG_ACCOUNT_MANAGERS = "AccountManagers"
OCTO_TAG_FARMERS = "Farmers"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан")

if not SPREADSHEET_ID:
    raise RuntimeError("SPREADSHEET_ID не задан")

if not SERVICE_ACCOUNT_JSON:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON не задан")

if not BACKUP_SPREADSHEET_ID:
    raise RuntimeError("BACKUP_SPREADSHEET_ID не задан")

if not BASEBOT_SPREADSHEET_ID:
    raise RuntimeError("BASEBOT_SPREADSHEET_ID не задан")
    

# =========================
# ACCESS CONTROL
# =========================

ADMINS = {
    7573650707: "JackGrazer_Deputy_Head_Account",
    7681133609: "Cillian_Murphy_Head_of_Account",
}

ADMIN_FARM_USERS = {
    7172090459: "JackieChan_FarmLead",
    7389698288: "andrewgarfield_farmlead",
}

FP_WAREHOUSE_NOTIFY_ADMIN_ID = 7573650707
CATE_USER_ID = 7851493919

FARM_FP_WAREHOUSE_NOTIFY_ADMIN_IDS = [
    7573650707,
    7172090459,
    7389698288,
]

BOT_ERROR_NOTIFY_ADMIN_ID = 7573650707
BOT_ERROR_NOTIFY_ADMIN_NAME = "JackGrazer_Deputy_Head_Account"

bot_diagnostics_lock = threading.Lock()
bot_diagnostics_running = False
last_error_notifications = {}
error_notify_lock = threading.Lock()
ERROR_NOTIFY_COOLDOWN = 300  # 5 минут на одинаковую ошибку

ACCOUNTS_USERS = {
    7953116439: "WillemDafoe_Accmanager",
    8334712952: "Ariana_Grande_Account_manager",
    7851493919: "CateBlanchettAccountManager",
    7426931469: "JimCarrey_AccountManager",
    8035275476: "SeanConneryManager",
}

FARMERS_USERS = {
    8482380951: "josephgordonlevitt_farmer",
    8389730381: "JaimeMurray_farmer",
    8589105033: "owenwilson_farmer",
    8503147017: "zendaya_farmer",
    8797795819: "markzuckerberg_farm",
}

def is_admin(user_id):
    return user_id in ADMINS or user_id in ADMIN_FARM_USERS

def is_admin_farm(user_id):
    return user_id in ADMIN_FARM_USERS

def is_accounts_user(user_id):
    return user_id in ACCOUNTS_USERS

def is_farmers_user(user_id):
    return user_id in FARMERS_USERS

def has_access(user_id):
    return is_admin(user_id) or is_accounts_user(user_id) or is_farmers_user(user_id)

def touch_request_heartbeat():
    global last_request_time
    last_request_time = time.time()

def touch_background_heartbeat():
    global last_background_time
    last_background_time = time.time()

def cleanup_error_notifications():
    now = time.time()
    to_delete = []

    with error_notify_lock:
        for key, ts in last_error_notifications.items():
            if now - ts > ERROR_NOTIFY_COOLDOWN * 3:
                to_delete.append(key)

        for key in to_delete:
            last_error_notifications.pop(key, None)

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

SHEET_ACCOUNTS = "База_личек"
SHEET_ISSUES = "Простые лички 26"
SHEET_KINGS = "База_кингов"
SHEET_BMS = "База_БМ"
SHEET_FPS = "База_ФП"
SHEET_FARM_KINGS = "База фарм кинги"
SHEET_FARM_BMS = "База фарм бм"
SHEET_FARM_FPS = "База фарм фп"
SHEET_CRYPTO_KINGS = "База_крипта_кинги"
SHEET_PIXELS = "База_пикселей"

SYNC_COL_KINGS = 12
SYNC_COL_BMS = 9
SYNC_COL_CRYPTO_KINGS = 12
SYNC_COL_PIXELS = 8
SYNC_COL_FARM_KINGS = 12
SYNC_COL_FARM_BMS = 9

BASEBOT_SHEET_KINGS = "BaseBot Kings"
BASEBOT_SHEET_BMS = "BaseBot BM"
BASEBOT_SHEET_CRYPTO_KINGS = "BaseBot Crypto Kings"
BASEBOT_SHEET_PIXELS = "BaseBot Pixels"
BASEBOT_SHEET_FARM_KINGS = "BaseBot Farm Kings"
BASEBOT_SHEET_FARM_BMS = "BaseBot Farm BM"

BASEBOT_SYNC_COL_KINGS = 7
BASEBOT_SYNC_COL_BMS = 5
BASEBOT_SYNC_COL_PIXELS = 4

LIMIT_OPTIONS = ['-250', '250-500', '500-1200', '1200-1500', 'unlim']
THRESHOLD_OPTIONS = ['0-49', '50-99', '100-199', '200-499', '500+']
GMT_OPTIONS = ['-10', '-9', '-8', '-7', '-6', '-5', '-4', '-3', '-2', '-1', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
ACCOUNT_CURRENCY_COL = 12  # M колонка в База_личек

MENU_ACCOUNTS = 'Accounts'
MENU_PIXELS = 'Пиксели'
MENU_FARMERS = 'Farmers'
FARM_MENU_KING = 'King'
FARM_MENU_BM = 'BM'
FARM_MENU_FP = 'FP'

FARM_SUBMENU_GET_KINGS = '➡️Взять кинги'
FARM_SUBMENU_FREE_KINGS = '🆓Cвободныe кинги'
FARM_SUBMENU_RETURN_KING = '↩️Beрнуть кинг'
FARM_SUBMENU_SEARCH_KING = '🔎Пoиск кингa'

BTN_FARM_KINGS_PARTIAL_CONFIRM = 'Выдать'
BTN_FARM_KINGS_PARTIAL_CANCEL = 'Отмена'

FARM_SUBMENU_GET_BM = '➡️Получить BM'
FARM_SUBMENU_FREE_BMS = '🆓Свободные BMы'
FARM_SUBMENU_SEARCH_BM = '🔎Поиск BMа'
FARM_SUBMENU_RETURN_BM = '↩️Вернуть BM'

FARM_SUBMENU_GET_FP = '➡️Выдать FP'
FARM_SUBMENU_SEARCH_FP = '🔎Поиск FP'

BTN_BACK_TO_FARMERS = 'Назад в Farmers'
MENU_KINGS = 'Кинги'
MENU_BMS = 'БМ'
MENU_FPS = 'ФП'
MENU_STATS = 'Статистика'
MENU_MANAGER_STATS = 'Статистика менеджера'
MENU_FARMER_STATS = 'Статистика фармера'
MENU_ADMIN = 'Admin'
MENU_CANCEL = 'Отмена'

SUBMENU_GET_PIXELS = '➡️Получить Пиксели'
SUBMENU_SEARCH_PIXEL = '🔎Найти Пиксель'
SUBMENU_RETURN_PIXEL = '↩️Вернуть Пиксель'
SUBMENU_ACCOUNTS_MAIN = 'Лички'
SUBMENU_BACK_MAIN = 'В меню'

DEPT_CRYPTO = '🪙Крипта'
DEPT_GAMBLA = '🎰Гембла'

CRYPTO_NAMES = [
    '№3 dasha', '№5 mark', '№20 misha',
    '№32 alex', '№34 anton', '№37 vladimir2',
    '№4 nikita', '№57 VD3', '№60 MSH5'
]

GAMBLA_NAMES = [
    '№8 artem', '№13 ivan', '№16 sergei', '№19 ilya', '№26 maksim1',
    '№27 denis', '№29 ivansh', '№14 evgen', '№777 asim',
    '№30 maksim2', '№39 alex_gambl', '№47 daniil', '№48 semen', '№49 ivan2',
    '№50 andrey2', '№51 vitaliy', '№21 vladimir1', '№22 andrey', '№52 gleb', '№53 dasha2', '№54 vladimir3',
    '№000 richard', '№55 artem2', '№56 IC1', '№58 KM2', '№59 AH6', '№43 maksim3', '№45 anton2', '№61 SN1'
]

SUBMENU_GET = '➡️Выдать лички'
SUBMENU_QUICK_GET = '⏭️Быстро выдать личку'
SUBMENU_FREE = '🆓Свободные лички'
SUBMENU_RETURN = '↩️Вернуть личку'
SUBMENU_SEARCH = '🔎Поиск лички'

FREE_LIMIT_250 = '250'
FREE_LIMIT_500 = '500'
FREE_LIMIT_1200 = '1200'
FREE_LIMIT_1500 = '1500'
FREE_LIMIT_UNLIM = 'unlim'

SUBMENU_FREE_KINGS = '🆓Свободные кинги'
SUBMENU_GET_KINGS = '➡️Получить кинг'
SUBMENU_RETURN_KING = '↩️Вернуть кинг'
SUBMENU_SEARCH_KING = '🔎Поиск кинга'

SUBMENU_GET_BM = '➡️Получить БМ'
SUBMENU_FREE_BMS = '🆓Свободные БМы'
SUBMENU_SEARCH_BM = '🔎Поиск БМа'
SUBMENU_RETURN_BM = '↩️Вернуть БМ'
BTN_BM_BAN_CONFIRM = 'Подтвердить ban bm'

SUBMENU_GET_FP = '➡️Выдать ФП'
SUBMENU_SEARCH_FP = '🔎Поиск ФП'

ADMIN_ACCOUNTANTS = 'Акаунтеры'
ADMIN_FARMERS = 'Фармеры'
ADMIN_BOT_CHECK = 'Проверка бота'
ADMIN_BACKUP = 'Бэкап таблиц'
ADMIN_UPDATE_5M = 'Обновление 5м'
ADMIN_ALL_STATS = 'Статистика всех'
SUBMENU_CRYPTO_KINGS = 'Крипта кинги'
ADMIN_ADD_CRYPTO_KINGS = 'Добавить crypto king'

BTN_CRYPTO_KING_CONFIRM = "✅выдать"
BTN_CRYPTO_KING_OTHER = "🔄другой"

BTN_DOWNLOAD_CRYPTO_KING_TXT = "📄 Скачать txt"
BTN_CRYPTO_KING_BACK_TO_MENU = "📘 В меню"

ADMIN_ADD_ACCOUNTS = 'Добавить лички'
ADMIN_ADD_KINGS = 'Добавить кинги'
ADMIN_ADD_BMS = 'Добавить БМы'
ADMIN_ADD_FPS = 'Добавить ФП'
ADMIN_ADD_FARM_KINGS = 'Добавить king'
ADMIN_ADD_FARM_BMS = 'Добавить bm'
ADMIN_ADD_FARM_FPS = 'Добавить fp'
ADMIN_ADD_PIXELS = 'Добавить Пиксели'
BTN_PIXEL_CONFIRM = 'Выдать Пиксель'
BTN_PIXEL_NEXT = 'Другой Пиксель'
BTN_PIXEL_BAN_CONFIRM = 'Подтвердить ban пикселя'
BTN_BACK_FROM_ADMIN_FARMERS = 'Назад в Admin'
BTN_BM_CONFIRM = 'Выдать БМ'
BTN_BM_NEXT = 'Другой БМ'
BTN_BACK_FROM_ADMIN = 'Назад из Admin'
BTN_BACK_FROM_ACCOUNTANTS = 'Назад из Акаунтеры'

BTN_BACK_TO_MENU = '📘В меню'
BTN_BACK_STEP = "⬅️ Назад"

# кнопки выдачи личек
BTN_ISSUE_CONFIRM = 'Выдать личку'
BTN_ISSUE_NEXT = 'Другая личка'
BTN_ISSUE_MORE = 'Выдать еще'
BTN_RETURN_CONFIRM = 'Подтвердить бан'

# кнопки выдачи кингов
BTN_KING_CONFIRM = 'Выдать кинг'
BTN_KING_NEXT = 'Другой кинг'
BTN_KING_BAN_CONFIRM = 'Подтвердить ban'

# кнопки выдачи фп
BTN_FP_CONFIRM = 'Подтвердить выдачу'
BTN_FP_NEXT = 'Другое ФП'

BTN_RETURN_TO_BAN = '🚫В бан'
BTN_RETURN_TO_FREE = '↩️Вернуть'

BTN_KING_RETURN_FREE_CONFIRM = 'Подтвердить возврат кинга'
BTN_FARM_KING_RETURN_FREE_CONFIRM = 'Подтвердить возврат farm king'
BTN_FP_RETURN_FREE_CONFIRM = 'Подтвердить возврат ФП'
BTN_FARM_FP_RETURN_FREE_CONFIRM = 'Подтвердить возврат farm FP'
BTN_FARM_BM_RETURN_FREE_CONFIRM = 'Подтвердить возврат farm BM'
BTN_BM_RETURN_FREE_CONFIRM = 'Подтвердить возврат БМ'
BTN_PIXEL_RETURN_FREE_CONFIRM = 'Подтвердить возврат Пикселя'

SUBMENU_RETURN_FP = '↩️Вернуть ФП'
FARM_SUBMENU_RETURN_FP = '↩️Вернуть FP'

# Память состояний пользователей (для старта хватит)
user_states = {}
user_state_history = {}
state_lock = threading.Lock()
user_action_lock = threading.Lock()
issue_lock = threading.Lock()
accounts_lock = threading.Lock()
processed_updates = {}
processed_updates_lock = threading.Lock()
PROCESSED_UPDATES_TTL = 600  # 10 минут

backup_lock = threading.Lock()
google_lock = threading.RLock()
last_backup_date = None
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

last_user_action = {}
ACTION_COOLDOWN = 0.2

STATE_TTL = 600  # обычный ttl

CRYPTO_BULK_PROXY_TTL = 60 * 60 * 6  # 6 часов

CRYPTO_BULK_MODE_COUNT = "awaiting_crypto_kings_count"
CRYPTO_BULK_MODE_NAMES = "awaiting_crypto_kings_names_bulk"
CRYPTO_BULK_MODE_PROXY = "awaiting_crypto_kings_proxy_bulk"
CRYPTO_SINGLE_MODE_PRICE = "awaiting_crypto_king_price"
CRYPTO_BULK_MODE_PRICE = "awaiting_crypto_kings_price_bulk"
CRYPTO_BULK_MODE_CONFIRM = "awaiting_crypto_kings_confirm_bulk"

FARM_KING_BULK_PROXY_TTL = CRYPTO_BULK_PROXY_TTL

FARM_KING_OCTO_MODE_COUNT = "awaiting_farm_kings_count_octo"
FARM_KING_OCTO_MODE_GEO = "awaiting_farm_king_geo_octo"
FARM_KING_OCTO_MODE_NAME = "awaiting_farm_king_name_octo"
FARM_KING_OCTO_MODE_FOUND = "farm_king_found_octo"
FARM_KING_OCTO_MODE_SINGLE_PROXY = "awaiting_farm_king_octo_proxy"
FARM_KING_OCTO_MODE_BULK_NAMES = "awaiting_farm_king_names_bulk_octo"
FARM_KING_OCTO_MODE_BULK_CONFIRM = "awaiting_farm_kings_confirm_bulk_octo"
FARM_KING_OCTO_MODE_BULK_PROXY = "awaiting_farm_kings_proxy_bulk_octo"

last_request_time = time.time()
last_background_time = time.time()
WATCHDOG_TIMEOUT = 300  # 5 минут

gspread_client = None
sheet_cache = {}

google_error_until = 0
GOOGLE_ERROR_COOLDOWN = 5
google_error_count = 0

def reset_google_cache():
    global gspread_client, sheet_cache, table_cache, basebot_sheet_cache
    gspread_client = None
    sheet_cache = {}
    basebot_sheet_cache = {}

    with table_cache_lock:
        table_cache = {
            SHEET_ACCOUNTS: {"rows": None, "updated_at": 0},
            SHEET_ISSUES: {"rows": None, "updated_at": 0},
            SHEET_KINGS: {"rows": None, "updated_at": 0},
            SHEET_CRYPTO_KINGS: {"rows": None, "updated_at": 0},
            SHEET_BMS: {"rows": None, "updated_at": 0},
            SHEET_FPS: {"rows": None, "updated_at": 0},
            SHEET_FARM_KINGS: {"rows": None, "updated_at": 0},
            SHEET_FARM_BMS: {"rows": None, "updated_at": 0},
            SHEET_FARM_FPS: {"rows": None, "updated_at": 0},
            SHEET_PIXELS: {"rows": None, "updated_at": 0},
        }

def reset_table_cache():
    global table_cache
    with table_cache_lock:
        table_cache = {
            SHEET_ACCOUNTS: {"rows": None, "updated_at": 0},
            SHEET_ISSUES: {"rows": None, "updated_at": 0},
            SHEET_KINGS: {"rows": None, "updated_at": 0},
            SHEET_CRYPTO_KINGS: {"rows": None, "updated_at": 0},
            SHEET_BMS: {"rows": None, "updated_at": 0},
            SHEET_FPS: {"rows": None, "updated_at": 0},
            SHEET_FARM_KINGS: {"rows": None, "updated_at": 0},
            SHEET_FARM_BMS: {"rows": None, "updated_at": 0},
            SHEET_FARM_FPS: {"rows": None, "updated_at": 0},
            SHEET_PIXELS: {"rows": None, "updated_at": 0},
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
TABLE_CACHE_TTL = 90

table_cache = {
    SHEET_ACCOUNTS: {"rows": None, "updated_at": 0},
    SHEET_ISSUES: {"rows": None, "updated_at": 0},
    SHEET_KINGS: {"rows": None, "updated_at": 0},
    SHEET_CRYPTO_KINGS: {"rows": None, "updated_at": 0},
    SHEET_BMS: {"rows": None, "updated_at": 0},
    SHEET_FPS: {"rows": None, "updated_at": 0},
    SHEET_FARM_KINGS: {"rows": None, "updated_at": 0},
    SHEET_FARM_BMS: {"rows": None, "updated_at": 0},
    SHEET_FARM_FPS: {"rows": None, "updated_at": 0},
    SHEET_PIXELS: {"rows": None, "updated_at": 0},
}

table_cache_lock = threading.Lock()


def refresh_sheet_cache(sheet_name):
    def _do():
        with google_lock:
            sheet = get_sheet(sheet_name)
            return sheet.get_all_values()

    rows = google_read_with_retry(_do)

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


def mark_sheet_cache_stale(sheet_name):
    with table_cache_lock:
        if sheet_name in table_cache:
            table_cache[sheet_name]["updated_at"] = 0


def sheet_update_and_refresh(sheet_name, cell_range, values):
    def _do():
        with google_lock:
            sheet = get_sheet(sheet_name)
            sheet.update(cell_range, values)

    google_write_with_retry(_do)
    mark_sheet_cache_stale(sheet_name)

def append_issue_row_fixed(row):
    rows = get_sheet_rows_cached(SHEET_ISSUES, force=True)

    next_row = len(rows) + 1
    values = list(row or [])

    if len(values) < 7:
        values = values + [""] * (7 - len(values))
    else:
        values = values[:7]

    sheet_update_and_refresh(
        SHEET_ISSUES,
        f"A{next_row}:G{next_row}",
        [values]
    )

def append_issue_rows_fixed(rows_to_add):
    rows = [list(x or [])[:7] for x in (rows_to_add or []) if x]
    if not rows:
        return

    normalized = []
    for row in rows:
        if len(row) < 7:
            row = row + [""] * (7 - len(row))
        normalized.append(row[:7])

    current_rows = get_sheet_rows_cached(SHEET_ISSUES, force=True)
    start_row = len(current_rows) + 1
    end_row = start_row + len(normalized) - 1

    sheet_update_and_refresh(
        SHEET_ISSUES,
        f"A{start_row}:G{end_row}",
        normalized
    )

def sheet_update_raw(sheet_name, cell_range, values):
    def _do():
        with google_lock:
            sheet = get_sheet(sheet_name)
            sheet.update(cell_range, values)

    google_write_with_retry(_do)
    mark_sheet_cache_stale(sheet_name)

def sheet_batch_update_raw(sheet_name, updates):
    if not updates:
        return

    def _do():
        with google_lock:
            sheet = get_sheet(sheet_name)
            sheet.batch_update(updates)

    google_write_with_retry(_do)
    mark_sheet_cache_stale(sheet_name)

def sheet_append_row_and_refresh(sheet_name, row, value_input_option="USER_ENTERED"):
    def _do():
        with google_lock:
            sheet = get_sheet(sheet_name)
            sheet.append_row(row, value_input_option=value_input_option)

    google_write_with_retry(_do)
    mark_sheet_cache_stale(sheet_name)

def sheet_delete_row_and_refresh(sheet_name, row_index):
    def _do():
        with google_lock:
            sheet = get_sheet(sheet_name)
            sheet.delete_rows(row_index)

    google_write_with_retry(_do)
    mark_sheet_cache_stale(sheet_name)

def sheet_append_rows_and_refresh(sheet_name, rows, value_input_option="USER_ENTERED"):
    def _do():
        with google_lock:
            sheet = get_sheet(sheet_name)
            sheet.append_rows(rows, value_input_option=value_input_option)

    google_write_with_retry(_do)
    mark_sheet_cache_stale(sheet_name)

def is_google_quota_error(exc):
    text = str(exc).lower()
    return (
        "quota exceeded" in text
        or "write requests per minute per user" in text
        or "[429]" in text
        or "too many requests" in text
    )

def google_read_with_retry(action, retries=5):
    delay = 2

    for attempt in range(retries):
        try:
            return action()
        except Exception as e:
            if not is_google_quota_error(e) or attempt == retries - 1:
                raise

            logging.warning(f"Google read quota hit, retry in {delay}s: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 12)

def google_write_with_retry(action, retries=5):
    delay = 2

    for attempt in range(retries):
        try:
            return action()
        except Exception as e:
            if not is_google_quota_error(e) or attempt == retries - 1:
                raise

            logging.warning(f"Google write quota hit, retry in {delay}s: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 12)

# =========================
# GOOGLE SHEETS
# =========================
def get_gspread_client():
    global gspread_client

    if gspread_client is not None:
        return gspread_client

    try:
        raw_service_json = str(SERVICE_ACCOUNT_JSON or "").strip()

        if not raw_service_json:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON пустой")

        try:
            data = json.loads(raw_service_json)
        except Exception as e:
            logging.error(
                f"GOOGLE_SERVICE_ACCOUNT_JSON parse error. "
                f"First 200 chars: {raw_service_json[:200]}"
            )
            raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_JSON невалидный JSON: {e}")

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
        cached = sheet_cache.get(sheet_name)
        if cached is not None:
            return cached

        start_time = time.time()

        def _open_sheet():
            client = get_gspread_client()
            spreadsheet = client.open_by_key(SPREADSHEET_ID)
            return spreadsheet.worksheet(sheet_name)

        sheet = google_read_with_retry(_open_sheet)

        if time.time() - start_time > 10:
            logging.warning(f"Google Sheets slow response for '{sheet_name}'")

        sheet_cache[sheet_name] = sheet
        google_error_count = 0
        google_error_until = 0
        return sheet

    except Exception as e:
        logging.error(f"get_sheet error for '{sheet_name}': {e}")
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

basebot_sheet_cache = {}

def get_basebot_sheet(sheet_name):
    global basebot_sheet_cache

    check_google_available()

    try:
        cached = basebot_sheet_cache.get(sheet_name)
        if cached is not None:
            return cached

        def _open_sheet():
            client = get_gspread_client()
            spreadsheet = client.open_by_key(BASEBOT_SPREADSHEET_ID)
            return spreadsheet.worksheet(sheet_name)

        sheet = google_read_with_retry(_open_sheet)
        basebot_sheet_cache[sheet_name] = sheet
        return sheet

    except Exception as e:
        logging.error(f"get_basebot_sheet error for '{sheet_name}': {e}")
        basebot_sheet_cache = {}
        raise

def basebot_append_rows(sheet_name, rows, value_input_option="USER_ENTERED"):
    def _do():
        with google_lock:
            sheet = get_basebot_sheet(sheet_name)
            sheet.append_rows(rows, value_input_option=value_input_option)

    google_write_with_retry(_do)

def basebot_update_range(sheet_name, cell_range, values):
    def _do():
        with google_lock:
            sheet = get_basebot_sheet(sheet_name)
            sheet.update(cell_range, values)

    google_write_with_retry(_do)

def basebot_get_all_rows(sheet_name):
    def _do():
        with google_lock:
            sheet = get_basebot_sheet(sheet_name)
            return sheet.get_all_values()

    return google_read_with_retry(_do)

def basebot_delete_row(sheet_name, row_index):
    def _do():
        with google_lock:
            sheet = get_basebot_sheet(sheet_name)
            sheet.delete_rows(row_index)

    google_write_with_retry(_do)


# =========================
# TELEGRAM
# =========================
CATE_TRIGGERS = [
    "✅",
    "готово",
    "выдан",
    "выдано",
    "заведен",
    "заведены",
    "успешно",
    "создан",
    "созданы",
    "готов",
]

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
            timeout=20
        )

        if resp.status_code != 200:
            logging.warning(f"Telegram send failed: {resp.text}")

        try:
            text_raw = str(text or "").strip()
            text_lower = text_raw.lower()
        
            if str(chat_id) == "7851493919" and text_raw != "♿️Все Кать отьебись♿️":
                if any(trigger in text_lower for trigger in CATE_TRIGGERS):
                    requests.post(
                        f"{BASE_URL}/sendMessage",
                        json={
                            "chat_id": chat_id,
                            "text": "♿️Все Кать отьебись♿️"
                        },
                        timeout=20
                    )
        except Exception:
            logging.exception("cate auto message failed")

    except Exception as e:
        logging.error(f"tg_send_message error: {e}")

def tg_send_inline_message(chat_id, text, inline_buttons):
    try:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "reply_markup": {
                "inline_keyboard": inline_buttons
            }
        }

        resp = requests.post(
            f"{BASE_URL}/sendMessage",
            json=payload,
            timeout=20
        )

        if resp.status_code != 200:
            logging.warning(f"Telegram inline send failed: {resp.text}")
            return None

        data = resp.json()
        if not data.get("ok"):
            logging.warning(f"Telegram inline send api error: {data}")
            return None

        return data

    except Exception as e:
        logging.error(f"tg_send_inline_message error: {e}")
        return None

def tg_edit_message_text(chat_id, message_id, text, inline_buttons=None):
    try:
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text
        }

        if inline_buttons is not None:
            payload["reply_markup"] = {
                "inline_keyboard": inline_buttons
            }

        resp = requests.post(
            f"{BASE_URL}/editMessageText",
            json=payload,
            timeout=20
        )

        if resp.status_code != 200:
            logging.warning(f"Telegram editMessageText failed: {resp.text}")
            return False

        data = resp.json()
        if not data.get("ok"):
            logging.warning(f"Telegram editMessageText api error: {data}")
            return False

        return True

    except Exception as e:
        logging.error(f"tg_edit_message_text error: {e}")
        return False

def tg_answer_callback_query(callback_query_id, text=""):
    try:
        payload = {
            "callback_query_id": callback_query_id,
            "text": text
        }

        requests.post(
            f"{BASE_URL}/answerCallbackQuery",
            json=payload,
            timeout=20
        )
    except Exception as e:
        logging.error(f"tg_answer_callback_query error: {e}")

def tg_send_message_safe(chat_id, text, keyboard=None):
    try:
        tg_send_message(chat_id, text, keyboard)
    except Exception:
        logging.exception("tg_send_message_safe crashed")


def notify_admin_about_error(source, error_text, extra_text=""):
    try:
        source_key = str(source or "unknown")
        err_key = str(error_text or "unknown").strip()[:300]
        dedupe_key = f"{source_key}|{err_key}"

        now = time.time()

        with error_notify_lock:
            last_ts = last_error_notifications.get(dedupe_key, 0)
            if now - last_ts < ERROR_NOTIFY_COOLDOWN:
                return
            last_error_notifications[dedupe_key] = now

        message = (
            "🚨 Ошибка в боте\n\n"
            f"Источник: {source_key}\n"
            f"Ошибка: {error_text}"
        )

        if extra_text:
            message += f"\n\nДетали:\n{extra_text}"

        tg_send_long_message(BOT_ERROR_NOTIFY_ADMIN_ID, message)

    except Exception:
        logging.exception("notify_admin_about_error crashed")

def send_main_menu(chat_id, text="Главное меню:", user_id=None):
    if user_id is not None and is_admin(user_id):
        keyboard = [
            [{"text": MENU_ACCOUNTS}, {"text": MENU_FARMERS}],
            [{"text": MENU_STATS}],
            [{"text": MENU_ADMIN}],
            [{"text": MENU_CANCEL}]
        ]
    elif user_id is not None and is_accounts_user(user_id):
        keyboard = [
            [{"text": MENU_ACCOUNTS}],
            [{"text": MENU_STATS}],
            [{"text": MENU_CANCEL}]
        ]
    elif user_id is not None and is_farmers_user(user_id):
        keyboard = [
            [{"text": MENU_FARMERS}],
            [{"text": MENU_STATS}],
            [{"text": MENU_CANCEL}]
        ]
    else:
        keyboard = [
            [{"text": MENU_CANCEL}]
        ]

    tg_send_message(chat_id, text, keyboard)

def send_text_input_prompt(chat_id, text):
    keyboard = [
        [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_return_action_menu(chat_id, what_text):
    keyboard = [
        [{"text": BTN_RETURN_TO_BAN}, {"text": BTN_RETURN_TO_FREE}],
        [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
    ]
    tg_send_message(chat_id, f"Что сделать с {what_text}?", keyboard)

def send_accounts_main_menu(chat_id, text="Меню Accounts:"):
    keyboard = [
        [{"text": SUBMENU_ACCOUNTS_MAIN}, {"text": MENU_KINGS}],
        [{"text": MENU_BMS}, {"text": MENU_FPS}],
        [{"text": MENU_PIXELS}],
        [{"text": MENU_MANAGER_STATS}],
        [{"text": SUBMENU_BACK_MAIN}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_fps_menu(chat_id, text="Меню ФП:"):
    keyboard = [
        [{"text": SUBMENU_GET_FP}],
        [{"text": SUBMENU_SEARCH_FP}],
        [{"text": SUBMENU_RETURN_FP}],
        [{"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_pixels_menu(chat_id, text="Меню Пикселей:"):
    keyboard = [
        [{"text": SUBMENU_GET_PIXELS}],
        [{"text": SUBMENU_SEARCH_PIXEL}],
        [{"text": SUBMENU_RETURN_PIXEL}],
        [{"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_farmers_menu(chat_id, text="Меню Farmers:"):
    keyboard = [
        [{"text": FARM_MENU_KING}, {"text": FARM_MENU_BM}],
        [{"text": FARM_MENU_FP}],
        [{"text": MENU_FARMER_STATS}],
        [{"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_farm_kings_menu(chat_id, text="Меню Farm King:"):
    keyboard = [
        [{"text": FARM_SUBMENU_GET_KINGS}],
        [{"text": FARM_SUBMENU_FREE_KINGS}],
        [{"text": FARM_SUBMENU_RETURN_KING}],
        [{"text": FARM_SUBMENU_SEARCH_KING}],
        [{"text": BTN_BACK_TO_FARMERS}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_farm_bms_menu(chat_id, text="Меню Farm BM:"):
    keyboard = [
        [{"text": FARM_SUBMENU_GET_BM}],
        [{"text": FARM_SUBMENU_FREE_BMS}],
        [{"text": FARM_SUBMENU_SEARCH_BM}],
        [{"text": FARM_SUBMENU_RETURN_BM}],
        [{"text": BTN_BACK_TO_FARMERS}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_farm_fps_menu(chat_id, text="Меню Farm FP:"):
    keyboard = [
        [{"text": FARM_SUBMENU_GET_FP}],
        [{"text": FARM_SUBMENU_SEARCH_FP}],
        [{"text": FARM_SUBMENU_RETURN_FP}],
        [{"text": BTN_BACK_TO_FARMERS}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_accounts_menu(chat_id, text="Меню личек:"):
    keyboard = [
        [{"text": SUBMENU_GET}, {"text": SUBMENU_QUICK_GET}],
        [{"text": SUBMENU_FREE}, {"text": SUBMENU_RETURN}],
        [{"text": SUBMENU_SEARCH}, {"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_free_accounts_limit_menu(chat_id, text="Выбери лимит:"):
    keyboard = [
        [{"text": FREE_LIMIT_250}, {"text": FREE_LIMIT_500}],
        [{"text": FREE_LIMIT_1200}, {"text": FREE_LIMIT_1500}],
        [{"text": FREE_LIMIT_UNLIM}],
        [{"text": MENU_CANCEL}]
    ]
    tg_send_message(chat_id, text, keyboard)
def send_kings_menu(chat_id, text="Меню кингов:"):
    keyboard = [
        [{"text": SUBMENU_GET_KINGS}, {"text": SUBMENU_CRYPTO_KINGS}],
        [{"text": SUBMENU_FREE_KINGS}],
        [{"text": SUBMENU_RETURN_KING}],
        [{"text": SUBMENU_SEARCH_KING}],
        [{"text": SUBMENU_BACK_MAIN}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_bms_menu(chat_id, text="Меню БМов:"):
    keyboard = [
        [{"text": SUBMENU_GET_BM}],
        [{"text": SUBMENU_FREE_BMS}],
        [{"text": SUBMENU_SEARCH_BM}],
        [{"text": SUBMENU_RETURN_BM}],
        [{"text": BTN_BACK_TO_MENU}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_admin_menu(chat_id, text="Меню Admin:", user_id=None):
    if user_id is not None and is_admin_farm(user_id):
        keyboard = [
            [{"text": ADMIN_FARMERS}, {"text": ADMIN_ALL_STATS}],
            [{"text": BTN_BACK_FROM_ADMIN}]
        ]
    else:
        keyboard = [
            [{"text": ADMIN_BACKUP}, {"text": ADMIN_UPDATE_5M}],
            [{"text": ADMIN_ACCOUNTANTS}, {"text": ADMIN_FARMERS}],
            [{"text": ADMIN_ALL_STATS}, {"text": ADMIN_BOT_CHECK}],
            [{"text": BTN_BACK_FROM_ADMIN}]
        ]

    tg_send_message(chat_id, text, keyboard)

def send_admin_farmers_menu(chat_id, text="Admin / Фармеры:"):
    keyboard = [
        [{"text": ADMIN_ADD_FARM_KINGS}, {"text": ADMIN_ADD_FARM_BMS}],
        [{"text": ADMIN_ADD_FARM_FPS}],
        [{"text": BTN_BACK_FROM_ADMIN_FARMERS}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_admin_accountants_menu(chat_id, text="Меню Акаунтеры:"):
    keyboard = [
        [{"text": ADMIN_ADD_ACCOUNTS}, {"text": ADMIN_ADD_KINGS}],
        [{"text": ADMIN_ADD_CRYPTO_KINGS}, {"text": ADMIN_ADD_BMS}],
        [{"text": ADMIN_ADD_FPS}, {"text": ADMIN_ADD_PIXELS}],
        [{"text": BTN_BACK_FROM_ACCOUNTANTS}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_add_kings_instructions(chat_id):
    text = (
        "Пришли Кинги txt файлом.\n\n"
        "Формат:\n"
        "номер) дата покупки; цена; поставщик; гео\n"
        "данные кинга.\n\n"
        "Пример:\n"
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
        "cookie - zzzzzz"
    )
    tg_send_message(chat_id, text)

def send_add_bms_instructions(chat_id):
    text = (
        "Пришли БМы сообщением.\n\n"
        "Формат:\n"
        "номер) id БМа; дата покупки; цена; у кого купили\n"
        "инвайт ссылка на БМ.\n\n"
        "Пример:\n"
        "1) 123456789; 15/02/2026; 300; WD\n"
        "https://business.facebook.com/invitation/?token=......\n\n"
        "2) 987654321; 18/02/2026; 500; TT\n"
        "https://business.facebook.com/invitation/?token=......"
    )
    tg_send_message(chat_id, text)

def send_add_fps_instructions(chat_id):
    text = (
        "Пришли ФП сообщением, каждая с новой строки.\n\n"
        "Формат:\n"
        "ссылка; дата покупки; цена; у кого купили; склад\n\n"
        "Пример:\n"
        "https://facebook.com/profile.php?id=123; 15/02/2026; 300; WD; sklad1\n"
        "https://facebook.com/profile.php?id=456; 16/02/2026; 500; TT; sklad2"
    )
    tg_send_message(chat_id, text)

def send_add_pixels_instructions(chat_id):
    text = (
        "Пришли Пиксели сообщением блоками.\n\n"
        "Формат одного блока:\n"
        "дата покупки; цена; поставщик\n"
        "данные пикселя\n\n"
        "Потом пустая строка и следующий пиксель.\n\n"
        "Пример:\n"
        "15/02/2026; 300; WD\n"
        "pixel data 1 line 1\n"
        "pixel data 1 line 2\n\n"
        "16/02/2026; 500; TT\n"
        "pixel data 2"
    )
    tg_send_message(chat_id, text)

def send_octo_king_data_instructions(chat_id, warehouse_name=""):
    warehouse_text = f" для склада {warehouse_name}" if warehouse_name else ""

    text = (
        f"Теперь скинь данные кинга{warehouse_text} в таком формате:\n\n"
        "GEO - \n\n"
        "FB Login: \n"
        "FB Password: \n\n"
        "Email: \n"
        "Email's Password: \n"
        "Service: \n"
        "2FA: \n\n"
        "Doc's:"
    )

    tg_send_message(chat_id, text)

def get_free_king_geos():
    rows = get_sheet_rows_cached(SHEET_KINGS)

    geos = []
    seen = set()

    for row in rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        status = str(row[4]).strip().lower()
        geo = str(row[7]).strip()

        if status == "free" and geo and geo not in seen:
            geos.append(geo)
            seen.add(geo)

    return geos

def get_free_crypto_king_geos():
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

    geos = []
    seen = set()

    for row in rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        status = str(row[4]).strip().lower()
        geo = str(row[7]).strip()

        if status == "free" and geo and geo not in seen:
            geos.append(geo)
            seen.add(geo)

    return geos

def normalize_price_key(value):
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return ""
    try:
        num = float(text)
        if num.is_integer():
            return str(int(num))
        return str(num).rstrip("0").rstrip(".")
    except Exception:
        return text


def get_free_crypto_king_prices_by_geo(geo):
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

    prices = []
    seen = set()
    geo = str(geo or "").strip()

    for row in rows[1:]:
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if current_name:
            continue
        if not price:
            continue

        if price not in seen:
            seen.add(price)
            prices.append(price)

    def price_sort_key(x):
        try:
            return (0, float(str(x).replace(",", ".")))
        except Exception:
            return (1, str(x))

    prices.sort(key=price_sort_key)
    return prices


def send_crypto_king_price_options(chat_id, geo):
    prices = get_free_crypto_king_prices_by_geo(geo)

    if not prices:
        send_kings_menu(chat_id, f"Нет свободных crypto king с GEO {geo}.")
        return

    keyboard = []
    row = []

    for price in prices:
        row.append({"text": price})
        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}])

    tg_send_message(chat_id, f"Выбери цену для GEO {geo}:", keyboard)


def find_free_crypto_king_by_geo_and_price(geo, price, exclude_row=None):
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

    candidates = []
    geo = str(geo or "").strip()
    price = normalize_price_key(price)

    for idx, row in enumerate(rows[1:], start=2):
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        row_price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if row_price != price:
            continue
        if current_name:
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
            "data_text": get_full_king_data_from_row(row),
            "row": row
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[0]


def find_free_crypto_kings_by_geo_and_price(count_needed, geo, price):
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

    candidates = []
    geo = str(geo or "").strip()
    price = normalize_price_key(price)

    for idx, row in enumerate(rows[1:], start=2):
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        row_price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if row_price != price:
            continue
        if current_name:
            continue

        purchase_date = parse_date(row[1]) or datetime.max

        candidates.append({
            "row_index": idx,
            "purchase_date_obj": purchase_date,
            "purchase_date": row[1],
            "price": row[2],
            "supplier": row[3],
            "geo": row[7],
            "data_text": get_full_king_data_from_row(row),
            "row": row
        })

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[:count_needed]

def send_crypto_king_geo_options(chat_id):
    geos = get_free_crypto_king_geos()

    if not geos:
        send_kings_menu(chat_id, "Нет свободных crypto king ни по одному GEO.")
        return

    keyboard = []
    for geo in geos:
        keyboard.append([{"text": geo}])

    keyboard.append([{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}])

    tg_send_message(chat_id, "Какое нужно гео?", keyboard)


def crypto_king_name_exists(king_name):
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

    target = str(king_name).strip().lower()
    if not target:
        return False

    for row in rows[1:]:
        existing_name = str(row[0]).strip().lower() if len(row) > 0 else ""
        if existing_name == target:
            return True

    return False


def find_free_crypto_king_by_geo(geo, exclude_row=None):
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

    candidates = []

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

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
            "data_text": get_full_king_data_from_row(row)
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[0]

def find_free_crypto_kings_by_geo(count_needed, geo):
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

    candidates = []

    for idx, row in enumerate(rows[1:], start=2):
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()

        if status != "free":
            continue

        if row_geo != str(geo).strip():
            continue

        # свободный crypto king должен быть без названия
        if current_name:
            continue

        purchase_date = parse_date(row[1]) or datetime.max

        candidates.append({
            "row_index": idx,
            "purchase_date_obj": purchase_date,
            "purchase_date": row[1],
            "price": row[2],
            "supplier": row[3],
            "geo": row[7],
            "data_text": get_full_king_data_from_row(row),
            "row": row
        })

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[:count_needed]

def find_next_crypto_king_after_row(current_row_index, geo_value, price_value=None):
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS, force=True)

    geo_value = str(geo_value or "").strip()
    price_value = normalize_price_key(price_value)

    def row_matches(row):
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        current_name = str(row[0]).strip()
        current_geo = str(row[7]).strip()
        current_price = normalize_price_key(row[2])

        if status != "free" or current_name:
            return False
        if geo_value and current_geo != geo_value:
            return False
        if price_value and current_price != price_value:
            return False
        return True

    for idx, row in enumerate(rows[1:], start=2):
        if idx <= int(current_row_index):
            continue

        if row_matches(row):
            row = ensure_row_len(row, 13)
            return {
                "row_index": idx,
                "purchase_date": row[1],
                "price": row[2],
                "supplier": row[3],
                "geo": row[7],
                "row": row
            }

    for idx, row in enumerate(rows[1:], start=2):
        if idx >= int(current_row_index):
            break

        if row_matches(row):
            row = ensure_row_len(row, 13)
            return {
                "row_index": idx,
                "purchase_date": row[1],
                "price": row[2],
                "supplier": row[3],
                "geo": row[7],
                "row": row
            }

    return None

def send_crypto_bulk_found_preview(chat_id, user_id):
    state = get_state(user_id)

    queue = state.get("crypto_bulk_queue", [])
    if not queue:
        return

    king_for_whom = state.get("king_for_whom", "не указано")

    if len(queue) == 1:
        item = queue[0]

        text = (
            "🔍Найден crypto king:\n\n"
            f"🗓Дата покупки: {item.get('purchase_date', '')}\n"
            f"💵Цена: {item.get('price', '')}\n"
            f"🌐Гео: {item.get('geo', '')}\n"
            f"👨‍💻Для кого: {king_for_whom}\n"
            f"✏️Название: {item.get('king_name', 'не указано')}"
        )

        tg_send_message(chat_id, text)
        return

    lines = [
        "🔍Найдены crypto king:",
        "",
        f"👨‍💻Для кого: {king_for_whom}",
        ""
    ]

    for item in queue:
        king_name = str(item.get("king_name", "")).strip() or "не указано"
        price = str(item.get("price", "")).strip() or "не указана"
        geo = str(item.get("geo", "")).strip()
        purchase_date = str(item.get("purchase_date", "")).strip()

        line = f"• кинг с названием {king_name} цена {price}"

        extra_parts = []
        if geo:
            extra_parts.append(f"гео {geo}")
        if purchase_date:
            extra_parts.append(f"дата {purchase_date}")

        if extra_parts:
            line += " " + ", ".join(extra_parts)

        lines.append(line)

    tg_send_message(chat_id, "\n".join(lines))

def send_crypto_bulk_found_preview_once(chat_id, user_id):
    state = get_state(user_id)

    queue = state.get("crypto_bulk_queue", [])
    if not queue:
        send_kings_menu(chat_id, "Не удалось собрать список crypto king.")
        return

    lines = [
        "🔍Найдены crypto king:",
        "",
        f"👨‍💻Для кого: {state.get('king_for_whom', 'не указано')}",
        ""
    ]

    for i, item in enumerate(queue, start=1):
        lines.append(f"📦Кинг {i} из {len(queue)}")
        lines.append(f"✏️Название: {item.get('king_name', 'не указано')}")
        lines.append(f"💵Цена: {item.get('price', '')}")
        lines.append(f"🌐Гео: {item.get('geo', '')}")
    
        if has_bm_in_king_data(item.get("data_text", "")):
            lines.append("✅Есть BM")
    
        lines.append("")

    sent = tg_send_inline_message(
        chat_id,
        "\n".join(lines).strip(),
        [[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_crypto_bulk_item:{user_id}"
            },
            {
                "text": "❌отмена",
                "callback_data": f"cancel_crypto_bulk:{user_id}"
            }
        ]]
    )

    state["mode"] = CRYPTO_BULK_MODE_CONFIRM

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state["crypto_bulk_confirm_message_id"] = message_id
    except Exception:
        logging.exception("send_crypto_bulk_found_preview_once failed to save message_id")

    set_state_with_custom_ttl(user_id, state, CRYPTO_BULK_PROXY_TTL)

def has_bm_in_king_data(data_text):
    try:
        parsed = parse_crypto_king_raw_data(data_text) or {}
        bm_links = parsed.get("bm_links", []) or []
        bm_email_pairs = parsed.get("bm_email_pairs", []) or []
        return bool(bm_links or bm_email_pairs)
    except Exception:
        return False

def show_found_crypto_king(chat_id, user_id, found):
    state = get_state(user_id)

    state["mode"] = "crypto_king_found"
    state["king_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "🔍Найден crypto king:\n\n"
        f"🗓Дата покупки: {found['purchase_date']}\n"
        f"💵Цена: {found['price']}\n"
        f"🌐Гео: {found['geo']}\n"
        f"👨‍💻Для кого: {state.get('king_for_whom', 'не указано')}\n"
        f"✏️Название: {state.get('king_name', 'не указано')}"
    )

    if has_bm_in_king_data(found.get("data_text", "")):
        text += "\n✅Есть BM"

    sent = tg_send_inline_message(
        chat_id,
        text,
        [[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_crypto_king:{user_id}"
            },
            {
                "text": "🔄другой",
                "callback_data": f"other_crypto_king:{user_id}"
            }
        ]]
    )

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state = get_state(user_id)
                state["crypto_preview_message_id"] = message_id
                set_state(user_id, state)
    except Exception:
        logging.exception("show_found_crypto_king failed to save preview message_id")

def edit_found_crypto_king_preview(chat_id, message_id, user_id, found):
    state = get_state(user_id)

    state["mode"] = "crypto_king_found"
    state["king_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "🔍Найден crypto king:\n\n"
        f"🗓Дата покупки: {found['purchase_date']}\n"
        f"💵Цена: {found['price']}\n"
        f"🌐Гео: {found['geo']}\n"
        f"👨‍💻Для кого: {state.get('king_for_whom', 'не указано')}\n"
        f"✏️Название: {state.get('king_name', 'не указано')}"
    )

    if has_bm_in_king_data(found.get("data_text", "")):
        text += "\n✅Есть BM"

    tg_edit_message_text(
        chat_id,
        message_id,
        text,
        inline_buttons=[[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_crypto_king:{user_id}"
            },
            {
                "text": "🔄другой",
                "callback_data": f"other_crypto_king:{user_id}"
            }
        ]]
    )

def mark_crypto_king_preview_as_issued(chat_id, message_id, king_name, king_for_whom, price, geo_value):
    text = (
        "✅ Выдано\n\n"
        f"Crypto king выдан.\n"
        f"Название: {king_name}\n"
        f"Для кого: {king_for_whom}\n"
        f"Цена: {price}\n"
        f"Гео: {geo_value}"
    )

    try:
        tg_edit_message_text(
            chat_id,
            message_id,
            text,
            inline_buttons=[]
        )
    except Exception:
        logging.exception("mark_crypto_king_preview_as_issued failed")

def start_crypto_kings_bulk_proxy_step(chat_id, user_id):
    state = get_state(user_id)

    queue = state.get("crypto_bulk_queue", [])
    current_index = int(state.get("crypto_bulk_current_index", 0))

    if current_index >= len(queue):
        finish_crypto_kings_bulk(chat_id, user_id)
        return

    if state.get("crypto_bulk_skip_all_proxies"):
        process_crypto_bulk_proxy_step(
            chat_id=chat_id,
            user_id=user_id,
            username=state.get("crypto_bulk_username", ""),
            proxy_text="__SKIP_ALL_PROXIES__"
        )
        return

    current_item = queue[current_index]
    king_name = current_item.get("king_name", "")
    geo = current_item.get("geo", "")
    price = current_item.get("price", "")

    text = (
        f"Скинь socks5 proxy для кинга {king_name}\n\n"
        f"Цена: {price}\n"
        f"Гео: {geo}\n"
        f"Шаг {current_index + 1} из {len(queue)}\n\n"
        f"Формат:\n"
        f"socks5://login:password@host:port\n"
        f"или\n"
        f"socks5://host:port"
    )

    sent = tg_send_inline_message(
        chat_id,
        text,
        [[
            {
                "text": "⏭️ Пропустить все прокси",
                "callback_data": f"crypto_bulk_skip_all_proxies:{user_id}"
            }
        ]]
    )

    state["mode"] = CRYPTO_BULK_MODE_PROXY

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state["crypto_bulk_proxy_message_id"] = message_id
    except Exception:
        logging.exception("start_crypto_kings_bulk_proxy_step failed to save message_id")

    set_state_with_custom_ttl(user_id, state, CRYPTO_BULK_PROXY_TTL)

def send_king_geo_options(chat_id):
    geos = get_free_king_geos()

    if not geos:
        send_kings_menu(chat_id, "Нет свободных кингов ни по одному GEO.")
        return

    keyboard = []
    for geo in geos:
        keyboard.append([{"text": geo}])

    keyboard.append([{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}])

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

def tg_send_document_bytes(chat_id, filename, content_bytes, caption=None):
    try:
        data = {
            "chat_id": str(chat_id)
        }

        if caption:
            data["caption"] = str(caption)

        files = {
            "document": (filename, content_bytes, "text/plain")
        }

        resp = requests.post(
            f"{BASE_URL}/sendDocument",
            data=data,
            files=files,
            timeout=60
        )

        if resp.status_code != 200:
            logging.warning(f"Telegram sendDocument failed: {resp.status_code} {resp.text}")

    except Exception as e:
        logging.error(f"tg_send_document_bytes error: {e}")

def make_safe_txt_filename(name, default_name="king"):
    raw = str(name or "").strip()
    if not raw:
        raw = default_name

    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', raw)
    safe = safe.strip("._")

    if not safe:
        safe = default_name

    if not safe.lower().endswith(".txt"):
        safe += ".txt"

    return safe[:120]

import io
import zipfile

def tg_send_kings_as_zip(chat_id, issued_items, archive_name="kings_bundle.zip"):
    memory_file = io.BytesIO()

    with zipfile.ZipFile(memory_file, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for item in issued_items:
            king_name = str(item.get("king_name", "")).strip() or "king"
            data_text = str(item.get("data_text", "") or "")

            safe_name = "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in king_name)
            filename = f"{safe_name}.txt"

            zf.writestr(filename, data_text)

    memory_file.seek(0)

    files = {
        "document": (archive_name, memory_file.getvalue(), "application/zip")
    }

    data = {
        "chat_id": str(chat_id)
    }

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    resp = requests.post(url, data=data, files=files, timeout=120)
    resp.raise_for_status()
    return resp.json()

def build_crypto_bulk_result_text(results, for_whom):
    success_items = [x for x in results if x.get("octo_ok")]
    failed_items = [x for x in results if not x.get("octo_ok")]

    if success_items and not failed_items:
        header = "✅ Кинги заведены в Octo"
    elif failed_items and not success_items:
        header = "❌ Кинги не заведены в Octo"
    else:
        header = "⚠️ Часть кингов заведена в Octo"

    lines = [
        header,
        f"👨‍💻Для кого: {for_whom or 'не указано'}",
        ""
    ]

    for item in results:
        king_name = str(item.get("king_name", "")).strip() or "не указано"
        price = str(item.get("price", "")).strip() or "не указана"
        geo = str(item.get("geo", "")).strip() or "не указано"

        lines.append(f"✏️Название: {king_name}")
        lines.append(f"💵Цена: {price}")
        lines.append(f"🌐Гео: {geo}")

        if not item.get("octo_ok"):
            error_text = str(item.get("error_text", "")).strip() or "не удалось завести в Octo"
            lines.append(f"❌Ошибка: {error_text}")

    return "\n".join(lines)

def build_crypto_bulk_result_messages(results, for_whom, max_len=3500):
    success_items = [x for x in results if x.get("octo_ok")]
    failed_items = [x for x in results if not x.get("octo_ok")]

    if success_items and not failed_items:
        header = "✅ Кинги заведены в Octo"
    elif failed_items and not success_items:
        header = "❌ Кинги не заведены в Octo"
    else:
        header = "⚠️ Часть кингов заведена в Octo"

    header_text = (
        f"{header}\n"
        f"👨‍💻Для кого: {for_whom or 'не указано'}"
    )

    blocks = []

    for item in results:
        king_name = str(item.get("king_name", "")).strip() or "не указано"
        price = str(item.get("price", "")).strip() or "не указана"
        geo = str(item.get("geo", "")).strip() or "не указано"

        block_lines = [
            f"✏️Название: {king_name}",
            f"💵Цена: {price}",
            f"🌐Гео: {geo}",
        ]

        if not item.get("octo_ok"):
            error_text = str(item.get("error_text", "")).strip() or "не удалось завести в Octo"
            block_lines.append(f"❌Ошибка: {error_text}")

        blocks.append("\n".join(block_lines))

    messages = []
    current = header_text
    first_block = True

    for block in blocks:
        if first_block:
            separator = "\n\n"
        elif current:
            separator = "\n"
        else:
            separator = ""

        candidate = f"{current}{separator}{block}" if current else block

        if len(candidate) <= max_len:
            current = candidate
            first_block = False
            continue

        if current:
            messages.append(current)

        current = block
        first_block = False

    if current:
        messages.append(current)

    return messages

def send_crypto_bulk_followup_messages(chat_id, results):
    tg_send_message(
        chat_id,
        "Вручную проверь и выставь:\n"
        "• User-Agent\n"
        "• расширения\n"
        "• куки"
    )

    ua_lines = []
    bm_lines = []

    for item in results:
        if not item.get("octo_ok"):
            continue

        king_name = item.get("king_name", "")
        parsed = item.get("parsed_crypto", {}) or {}

        user_agent = str(parsed.get("user_agent", "")).strip()
        bm_links = parsed.get("bm_links", []) or []
        bm_email_pairs = parsed.get("bm_email_pairs", []) or []

        if user_agent:
            ua_lines.append(f"у кинга {king_name} есть User-Agent✅")

        if bm_links or bm_email_pairs:
            bm_lines.append(f"у кинга {king_name} есть BM✅")

    if ua_lines:
        tg_send_message(chat_id, "\n".join(ua_lines))

    if bm_lines:
        tg_send_message(chat_id, "\n".join(bm_lines))

def finish_crypto_kings_bulk(chat_id, user_id):
    state = get_state(user_id)

    results = state.get("crypto_bulk_results", [])
    for_whom = state.get("king_for_whom", "")

    if not results:
        clear_state(user_id)
        send_kings_menu(chat_id, "Не удалось выдать ни одного crypto king.")
        return

    message_parts = build_crypto_bulk_result_messages(results, for_whom)

    success_items = [x for x in results if x.get("octo_ok")]
    inline_buttons = []

    if len(success_items) == 1:
        inline_buttons = [[
            {
                "text": "📄 Скачать txt",
                "callback_data": f"download_crypto_bulk_txt:{user_id}:0"
            }
        ]]
    elif len(success_items) >= 2:
        inline_buttons = [[
            {
                "text": "📦 Скачать zip",
                "callback_data": f"download_crypto_bulk_zip:{user_id}"
            }
        ]]

    tg_send_inline_message_parts(
        chat_id=chat_id,
        message_parts=message_parts,
        inline_buttons=inline_buttons
    )

    send_crypto_bulk_followup_messages(chat_id, results)

    download_state = {
        "mode": "crypto_bulk_done",
        "crypto_bulk_results": results,
        "updated_at": time.time()
    }
    set_state(user_id, download_state)

    send_kings_menu(chat_id, "Выбери следующее действие:")

def tg_send_king_data_as_txt(chat_id, king_name, data_text, caption=None):
    text = str(data_text or "").strip()

    if not text:
        tg_send_message(chat_id, "Данные кинга не найдены.")
        return

    filename = make_safe_txt_filename(king_name or "king", default_name="king")
    tg_send_document_bytes(
        chat_id=chat_id,
        filename=filename,
        content_bytes=text.encode("utf-8"),
        caption=caption
    )

def tg_send_king_search_result_as_txt(chat_id, title, king_name, meta_text, data_text):
    meta_text = str(meta_text or "").strip()
    data_text = str(data_text or "").strip()

    full_text = meta_text
    if data_text:
        full_text += f"\n\nДанные:\n{data_text}"

    tg_send_king_data_as_txt(
        chat_id=chat_id,
        king_name=king_name or title or "king_search",
        data_text=full_text
    )

def extract_digits(text):
    return re.sub(r"\D", "", str(text or ""))

def clean_text_for_parsing(text):
    text = str(text or "")

    # BOM / zero-width / неразрывный пробел
    text = text.replace("\ufeff", "")
    text = text.replace("\u200b", "")
    text = text.replace("\u200c", "")
    text = text.replace("\u200d", "")
    text = text.replace("\xa0", " ")

    return text

def extract_account_ids_from_lines(lines):
    result = []

    for line in lines:
        clean = str(line).strip()
        digits = extract_digits(clean)

        # берем только строки, где почти весь текст — это ID
        # это отсечет названия типа MNQ... и оставит 38218890
        if 6 <= len(digits) <= 20:
            non_digits_removed = re.sub(r'[\s\-_]', '', clean)
            if digits == non_digits_removed or clean == digits:
                result.append(digits)

    # убираем дубли, сохраняя порядок
    unique = []
    seen = set()
    for x in result:
        if x not in seen:
            seen.add(x)
            unique.append(x)

    return unique

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

def convert_to_usd(amount_raw, currency_code):
    try:
        if amount_raw is None:
            return None

        value = float(str(amount_raw).replace(",", ""))

        currency_code = str(currency_code or "").upper().strip()

        # если уже USD — ничего не делаем
        if currency_code == "USD":
            return round(value, 2)

        # запрос курса
        url = f"{EXCHANGE_API_BASE}/latest"
        resp = requests.get(
            url,
            params={
                "base": currency_code,
                "symbols": "USD"
            },
            timeout=20
        )

        resp.raise_for_status()
        data = resp.json()

        rate = data.get("rates", {}).get("USD")

        if not rate:
            return value

        usd_value = value * float(rate)

        return round(usd_value, 2)

    except Exception as e:
        logging.warning(f"Currency convert error: {e}")
        return None

def normalize_currency_value(raw_value):
    if raw_value is None:
        return None

    text = str(raw_value).upper().strip()

    # нормализуем мусор от OCR
    text = text.replace("—", "-").replace("–", "-").replace("_", "-")
    text = re.sub(r"\s+", "", text)

    allowed = {
        "USD", "EUR", "TRY", "GBP", "AED", "JPY", "CAD", "AUD",
        "BRL", "MXN", "SGD", "HKD", "INR", "THB", "IDR", "MYR",
        "PEN"
    }

    # 1) TRY-TS / EUR-TS / MXN-TS / USDTS
    for code in allowed:
        if text.startswith(code):
            return code

    # 2) ищем код в любом месте строки
    for code in allowed:
        if code in text:
            return code

    return None

def extract_numeric_values_from_lines(lines):
    values = []

    for line in lines:
        text = str(line).strip()

        # числа формата 11,088.61 / 4478.12 / 200 / 36
        matches = re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?', text)

        for m in matches:
            raw = m.replace(",", "")
            try:
                num = float(raw)
                values.append({
                    "raw": m,
                    "value": num,
                    "line": text
                })
            except Exception:
                pass

    return values

def extract_currency_from_lines(lines):
    allowed = {
        "USD", "EUR", "TRY", "GBP", "AED", "JPY", "CAD", "AUD",
        "BRL", "MXN", "SGD", "HKD", "INR", "THB", "IDR", "MYR",
        "PEN"
    }

    # 1) сначала пробуем строки рядом с заголовком Currency
    for i, line in enumerate(lines):
        if re.search(r'Currency', line, re.IGNORECASE):
            for j in range(i, min(i + 6, len(lines))):
                curr = normalize_currency_value(lines[j])
                if curr in allowed:
                    return curr

    # 2) потом ищем по всем строкам
    for line in lines:
        curr = normalize_currency_value(line)
        if curr in allowed:
            return curr

    # 3) потом ищем даже внутри "склеенного" текста
    full_text = " ".join(lines)
    curr = normalize_currency_value(full_text)
    if curr in allowed:
        return curr

    return None


def get_fx_rate_to_usd(currency_code):
    currency_code = str(currency_code or "").strip().upper()

    if not currency_code:
        raise RuntimeError("Не указана валюта для конвертации")

    if currency_code == "USD":
        return 1.0

    url = f"{EXCHANGE_API_BASE}/latest"
    resp = requests.get(
        url,
        params={
            "base": currency_code,
            "symbols": "USD"
        },
        timeout=20
    )
    resp.raise_for_status()

    data = resp.json()
    rates = data.get("rates", {})
    rate = rates.get("USD")

    if rate is None:
        raise RuntimeError(f"Не удалось получить курс {currency_code} -> USD")

    return float(rate)


def convert_amount_to_usd(amount_value, currency_code):
    if amount_value is None:
        return None

    currency_code = str(currency_code or "").strip().upper()
    if not currency_code or currency_code == "USD":
        return float(amount_value)

    rate = get_fx_rate_to_usd(currency_code)
    return round(float(amount_value) * rate, 2)

def is_king_header_line(line):
    line = clean_text_for_parsing(line).strip()
    return re.match(r'^\d+[.)]\s+', line) is not None

def parse_kings_txt(text):
    text = clean_text_for_parsing(text)
    lines = text.splitlines()

    blocks = []
    current_header = None
    current_data_lines = []

    for raw_line in lines:
        line = clean_text_for_parsing(raw_line).rstrip()

        if not line.strip():
            if current_header is not None:
                current_data_lines.append("")
            continue

        if re.match(r'^\d+[.)]\s+', line.strip()):
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
            errors.append(f"Блок {idx}: нужно 4 поля: дата; цена; поставщик; гео")
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

def split_text_for_three_cells(text, max_len=50000):
    text = str(text or "")

    if len(text) <= max_len:
        return text, "", ""

    if len(text) <= max_len * 2:
        return text[:max_len], text[max_len:], ""

    if len(text) <= max_len * 3:
        return text[:max_len], text[max_len:max_len * 2], text[max_len * 2:]

    return None, None, None


def get_full_king_data_from_row(row):
    row = list(row or [])

    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    part_j = str(row[9] or "")
    part_k = str(row[10] or "")
    part_l = str(row[11] or "")

    return part_j + part_k + part_l

def add_kings_from_txt_content(file_text, target_sheet=SHEET_KINGS):
    rows, errors = parse_kings_txt(file_text)

    if not rows:
        if errors:
            return "Ничего не добавил.\n\nОшибки:\n" + "\n".join(errors[:10])
        return "Ничего не добавил. Не удалось разобрать файл."

    to_append = []

    for idx, item in enumerate(rows, start=1):
        part_j, part_k, part_l = split_text_for_three_cells(item["data_text"], max_len=50000)

        if part_j is None:
            errors.append(
                f"Блок {idx}: Данные кинга слишком большие даже для трёх ячеек: {len(item['data_text'])} символов"
            )
            continue

        sync_id = make_sync_id("king")

        row_to_add = [
            "",                     # A название кинга — пока пусто
            item["purchase_date"],  # B дата покупки
            item["price"],          # C цена
            item["supplier"],       # D у кого купили
            "free",                 # E статус
            "",                     # F кому выдали
            "",                     # G дата взятия
            item["geo"],            # H гео
            "",                     # I кто взял
            part_j,                 # J данные часть 1
            part_k,                 # K данные часть 2
            part_l,                 # L данные часть 3
            sync_id                 # M sync_id
        ]

        to_append.append(row_to_add)

    if to_append:
        sheet_append_rows_and_refresh(target_sheet, to_append)

        if target_sheet == SHEET_KINGS:
            basebot_sheet = BASEBOT_SHEET_KINGS
            sync_prefix = "king"
        elif target_sheet == SHEET_CRYPTO_KINGS:
            basebot_sheet = BASEBOT_SHEET_CRYPTO_KINGS
            sync_prefix = "crypto king"
        elif target_sheet == SHEET_FARM_KINGS:
            basebot_sheet = BASEBOT_SHEET_FARM_KINGS
            sync_prefix = "farm king"
        else:
            basebot_sheet = None
            sync_prefix = "king"

        if basebot_sheet:
            basebot_rows = []
            for row in to_append:
                basebot_rows.append([
                    sync_prefix,  # тип
                    row[3],       # supplier
                    row[4],       # status
                    row[7],       # geo
                    row[9],       # data1
                    row[10],      # data2
                    row[11],      # data3
                    row[12],      # sync_id (M)
                ])
            basebot_append_rows(basebot_sheet, basebot_rows)

        invalidate_stats_cache()

    message = (
        f"Готово ✅\n"
        f"Добавлено king: {len(to_append)}\n"
        f"Ошибок: {len(errors)}"
    )

    if errors:
        message += "\n\nОшибки:\n" + "\n".join(errors[:10])
        if len(errors) > 10:
            message += f"\n... и ещё {len(errors) - 10}"

    return message

def add_bms_from_txt_content(file_text, target_sheet=SHEET_BMS):
    rows, errors = parse_bms_txt(file_text)

    if not rows:
        if errors:
            return "Ничего не добавил.\n\nОшибки:\n" + "\n".join(errors[:10])
        return "Ничего не добавил. Не удалось разобрать файл."

    to_append = []

    for idx, item in enumerate(rows, start=1):
        sync_id = make_sync_id("bm")

        row_to_add = [
            item["bm_id"],           # A bm id
            item["purchase_date"],   # B дата покупки
            item["price"],           # C цена
            item["supplier"],        # D у кого купили
            "free",                  # E статус
            "",                      # F кому выдали
            "",                      # G дата выдачи
            "",                      # H кто выдал / кто взял
            item["data_text"],       # I данные
            sync_id                  # J sync_id
        ]

        to_append.append(row_to_add)

    if to_append:
        sheet_append_rows_and_refresh(target_sheet, to_append)

        if target_sheet == SHEET_BMS:
            basebot_sheet = BASEBOT_SHEET_BMS
            bm_type = "bm"
        elif target_sheet == SHEET_FARM_BMS:
            basebot_sheet = BASEBOT_SHEET_FARM_BMS
            bm_type = "farm bm"
        else:
            basebot_sheet = None
            bm_type = "bm"

        if basebot_sheet:
            basebot_rows = []
            for row in to_append:
                basebot_rows.append([
                    bm_type,  # тип
                    row[3],   # supplier
                    row[4],   # status
                    "",       # geo
                    row[8],   # data
                    "",       # data2
                    "",       # data3
                    row[9],   # sync_id (J)
                ])

            basebot_append_rows(basebot_sheet, basebot_rows)

        invalidate_stats_cache()

    label = "farm BM" if target_sheet == SHEET_FARM_BMS else "BM"

    message = (
        f"Готово ✅\n"
        f"Добавлено {label}: {len(to_append)}\n"
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

        if mode not in [
            "awaiting_kings_txt",
            "awaiting_bms_text",
            "awaiting_farm_kings_txt",
            "awaiting_farm_bms_text",
            "awaiting_crypto_kings_txt"
        ]:
            tg_send_message(
                chat_id,
                "Я сейчас не жду файл. Сначала открой нужный раздел в Admin и выбери добавление king или BM."
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
            file_text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            try:
                file_text = content.decode("utf-16")
            except UnicodeDecodeError:
                try:
                    file_text = content.decode("utf-16-le")
                except UnicodeDecodeError:
                    try:
                        file_text = content.decode("utf-8")
                    except UnicodeDecodeError:
                        try:
                            file_text = content.decode("cp1251")
                        except UnicodeDecodeError:
                            tg_send_message(
                                chat_id,
                                "Не удалось прочитать txt файл. Поддерживаются UTF-8, UTF-16, ANSI."
                            )
                            return

        file_text = clean_text_for_parsing(file_text)

        if mode == "awaiting_kings_txt":
            result_message = add_kings_from_txt_content(file_text, target_sheet=SHEET_KINGS)
        elif mode == "awaiting_bms_text":
            result_message = add_bms_from_txt_content(file_text)
        elif mode == "awaiting_farm_kings_txt":
            result_message = add_kings_from_txt_content(file_text, target_sheet=SHEET_FARM_KINGS)
        elif mode == "awaiting_farm_bms_text":
            result_message = add_bms_from_txt_content(file_text, target_sheet=SHEET_FARM_BMS)
        elif mode == "awaiting_crypto_kings_txt":
            result_message = add_kings_from_txt_content(file_text, target_sheet=SHEET_CRYPTO_KINGS)
        else:
            tg_send_message(chat_id, "Неизвестный режим загрузки файла.")
            return

        clear_state(user_id)
        tg_send_message(chat_id, result_message)

        if is_admin(user_id):
            if mode in ["awaiting_farm_kings_txt", "awaiting_farm_bms_text"]:
                send_admin_farmers_menu(chat_id, "Выбери следующее действие:")
            else:
                send_admin_menu(chat_id, "Выбери следующее действие:")
        else:
            send_main_menu(chat_id, "Выбери следующее действие:", user_id=user_id)

    except Exception as e:
        logging.exception("handle_document_message crashed")
        notify_admin_about_error(
            "handle_document_message",
            str(e),
            extra_text=f"user_id={msg.get('from', {}).get('id')}, chat_id={msg.get('chat', {}).get('id')}"
        )
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
        [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
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

    keyboard.append([{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}])

    tg_send_message(chat_id, title, keyboard)

def notify_all_users_about_update():
    recipients = sorted(set(ADMINS.keys()) | set(ACCOUNTS_USERS.keys()) | set(FARMERS_USERS.keys()))

    text = "Внимание: через 5 минут бот будет перезапущен из-за обновления."
    sent = 0
    failed = 0

    for uid in recipients:
        try:
            tg_send_message(uid, text)
            sent += 1
        except Exception as e:
            logging.error(f"notify_all_users_about_update error for {uid}: {e}")
            failed += 1

    return sent, failed

def append_auto_warehouse_rows_for_new_fps(added_items):
    if not added_items:
        return 0

    grouped = {}

    for item in added_items:
        warehouse = str(item.get("warehouse", "")).strip()
        if not warehouse:
            continue

        if warehouse not in grouped:
            grouped[warehouse] = item

    rows_to_append = []

    for warehouse, item in grouped.items():
        purchase_date = item.get("purchase_date", "")
        supplier = item.get("supplier", "")
        transfer_date = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
    
        rows_to_append.append([
            warehouse,          # A
            "KING",             # B
            purchase_date,      # C дата покупки
            35,                 # D цена склада
            transfer_date,      # E дата передачи
            supplier,           # F поставщик
            "TEAM"              # G кому передали
        ])

    if rows_to_append:
        sheet_append_rows_and_refresh(
            SHEET_ISSUES,
            rows_to_append,
            value_input_option="USER_ENTERED"
        )

    return len(rows_to_append)

def add_fps_from_text(text, target_sheet=SHEET_FPS):
    existing_rows = get_sheet_rows_cached(target_sheet)
    existing_links = set()

    for row in existing_rows[1:]:
        if row and row[0].strip():
            existing_links.add(row[0].strip())

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    to_append = []
    added_items = []
    errors = []
    duplicates = 0

    for i, line in enumerate(lines, start=1):
        fields = [x.strip() for x in line.split(";")]
        if len(fields) != 5:
            errors.append(f"Строка {i}: должно быть 5 полей через ';'")
            continue

        fp_link, purchase_date_raw, price_raw, supplier, warehouse = fields

        if not fp_link:
            errors.append(f"Строка {i}: пустая ссылка ФП")
            continue

        if fp_link in existing_links:
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

        purchase_date_str = purchase_date.strftime("%d/%m/%Y")

        to_append.append([
            fp_link,
            purchase_date_str,
            price,
            supplier,
            warehouse,
            "free",
            "",
            "",
            ""
        ])

        added_items.append({
            "warehouse": warehouse,
            "purchase_date": purchase_date_str,
            "supplier": supplier
        })

        existing_links.add(fp_link)

    warehouse_rows_added = 0
    new_warehouses = octo_extract_unique_warehouses(added_items)
    
    if to_append:
        sheet_append_rows_and_refresh(target_sheet, to_append)
        warehouse_rows_added = append_auto_warehouse_rows_for_new_fps(added_items)
        invalidate_stats_cache()

    message = (
        f"Готово ✅\n"
        f"Добавлено FP: {len(to_append)}\n"
        f"Добавлено авто-строк по складам: {warehouse_rows_added}\n"
        f"Новых складов для Octo: {len(new_warehouses)}\n"
        f"Дубликатов пропущено: {duplicates}\n"
        f"Ошибок: {len(errors)}"
    )

    if errors:
        message += "\n\nОшибки:\n" + "\n".join(errors[:10])
        if len(errors) > 10:
            message += f"\n... и ещё {len(errors) - 10}"

    return {
        "message": message,
        "new_warehouses": new_warehouses
    }

def parse_pixel_line(block_text):
    text = str(block_text or "").strip()
    if not text:
        return None

    lines = [x.rstrip() for x in text.splitlines() if x.strip()]
    if len(lines) < 2:
        return None

    first_line = lines[0].strip()
    parts = [x.strip() for x in first_line.split(";")]
    if len(parts) < 3:
        return None

    purchase_date = parts[0]
    price = parts[1].replace(",", ".").strip()
    supplier = parts[2]

    # Берём весь хвост блока после первой строки как есть,
    # чтобы не потерять "Токен cAPI" и длинный токен
    data_lines = lines[1:]
    data_text = "\n".join(data_lines).strip()

    return {
        "purchase_date": purchase_date,
        "price": price,
        "supplier": supplier,
        "data_text": data_text
    }

def add_pixels_from_text(file_text):
    text = str(file_text or "").strip()
    if not text:
        return "Ничего не добавил. Пустой текст."

    raw_lines = [line.rstrip() for line in text.splitlines()]

    blocks = []
    current_block = []

    for line in raw_lines:
        if line.strip():
            if ";" in line and current_block:
                blocks.append("\n".join(current_block).strip())
                current_block = [line.strip()]
            else:
                current_block.append(line.strip())

    if current_block:
        blocks.append("\n".join(current_block).strip())

    errors = []
    to_append = []

    for idx, block in enumerate(blocks, start=1):
        try:
            parsed = parse_pixel_line(block)
            if not parsed:
                errors.append(f"Строка {idx}: не удалось разобрать пиксель")
                continue

            sync_id = make_sync_id("pixel")

            row_to_add = [
                parsed["purchase_date"],  # A дата покупки
                parsed["price"],          # B цена
                parsed["supplier"],       # C у кого купили
                "free",                   # D статус
                "",                       # E кому выдали
                "",                       # F дата выдачи
                "",                       # G кто выдал / кто взял
                parsed["data_text"],      # H данные пикселя
                sync_id                   # I sync_id
            ]

            to_append.append(row_to_add)

        except Exception as e:
            errors.append(f"Строка {idx}: {e}")

    if to_append:
        sheet_append_rows_and_refresh(SHEET_PIXELS, to_append)

        basebot_rows = []
        for row in to_append:
            basebot_rows.append([
                "pixel",  # тип
                row[2],   # supplier
                row[3],   # status
                "",       # geo
                row[7],   # data
                "",       # data2
                "",       # data3
                row[8],   # sync_id (I)
            ])

        basebot_append_rows(BASEBOT_SHEET_PIXELS, basebot_rows)
        invalidate_stats_cache()

    message = (
        f"Готово ✅\n"
        f"Добавлено пикселей: {len(to_append)}\n"
        f"Ошибок: {len(errors)}"
    )

    if errors:
        message += "\n\nОшибки:\n" + "\n".join(errors[:10])
        if len(errors) > 10:
            message += f"\n... и ещё {len(errors) - 10}"

    return message

def find_fp_in_base(fp_link):
    rows = get_sheet_rows_cached(SHEET_FPS)

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        if str(row[0]).strip() == str(fp_link).strip():
            return {
                "row_index": idx,
                "row": row
            }

    return None


def build_fp_search_text(fp_link):
    fp_info = find_fp_in_base(fp_link)

    if not fp_info:
        return None

    row = fp_info["row"]

    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    text = (
        f"Ссылка ФП: {row[0]}\n"
        f"Дата покупки: {row[1] or 'не указана'}\n"
        f"Цена: {row[2] or 'не указана'}\n"
        f"Склад: {row[4] or 'не указан'}\n"
        f"Статус: {row[5] or 'не указан'}\n"
        f"Для кого: {row[6] or 'не указано'}\n"
        f"Кто взял: {row[7] or 'не указано'}\n"
        f"Дата выдачи: {row[8] or 'не указана'}"
    )

    return text

def build_fp_search_texts(fp_links):
    results = []

    for raw_link in fp_links:
        fp_link = str(raw_link).strip()
        if not fp_link:
            continue

        result = build_fp_search_text(fp_link)

        if result:
            results.append({
                "found": True,
                "fp_link": fp_link,
                "text": result
            })
        else:
            results.append({
                "found": False,
                "fp_link": fp_link,
                "text": f"ФП не найдено:\n{fp_link}"
            })

    return results

def return_fp_to_ban(fp_link, comment_text=""):
    found = find_fp_in_base(fp_link)
    if not found:
        return False, "ФП не найдено."

    row = found["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    status = str(row[5]).strip().lower()

    if status == "ban":
        return False, "Это ФП уже в ban."

    sheet_update_and_refresh(
        SHEET_FPS,
        f"F{found['row_index']}:G{found['row_index']}",
        [["ban", "ban"]]
    )

    issue_info = find_last_fp_issue_row(fp_link)
    if issue_info:
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, "ФП переведено в ban."


def return_fp_to_free(fp_link):
    found = find_fp_in_base(fp_link)
    if not found:
        return False, "ФП не найдено."

    row = found["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    status = str(row[5]).strip().lower()

    if status == "free":
        return False, "Это ФП уже free."

    sheet_update_and_refresh(
        SHEET_FPS,
        f"F{found['row_index']}:I{found['row_index']}",
        [["free", "", "", ""]]
    )

    delete_last_fp_issue_row(fp_link)

    invalidate_stats_cache()
    return True, "ФП возвращено в free."

def extract_warehouse_sort_key(warehouse_name):
    text = str(warehouse_name or "").strip().lower()

    # ищем последнее число в названии склада
    nums = re.findall(r"\d+", text)
    if nums:
        return int(nums[-1])

    # если числа нет — отправляем в конец
    return 10**9


def get_next_fp_warehouse_name(current_warehouse):
    rows = get_sheet_rows_cached(SHEET_FPS)

    warehouses = set()
    for row in rows[1:]:
        if len(row) < 5:
            row = row + [''] * (5 - len(row))

        warehouse = str(row[4]).strip()
        if warehouse:
            warehouses.add(warehouse)

    if not warehouses:
        return None

    ordered = sorted(warehouses, key=extract_warehouse_sort_key)

    current = str(current_warehouse or "").strip()
    if current not in ordered:
        return None

    idx = ordered.index(current)
    if idx + 1 < len(ordered):
        return ordered[idx + 1]

    return None


def count_free_fp_in_warehouse(warehouse_name):
    rows = get_sheet_rows_cached(SHEET_FPS)

    count = 0
    target = str(warehouse_name or "").strip()

    for row in rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        warehouse = str(row[4]).strip()
        status = str(row[5]).strip().lower()

        if warehouse == target and status == "free":
            count += 1

    return count


def notify_admin_fp_warehouse_finished(warehouse_name):
    next_warehouse = get_next_fp_warehouse_name(warehouse_name)

    if next_warehouse:
        ok, msg = octo_update_profile_tags_by_title(
            profile_title=next_warehouse,
            tags_to_add=["Sido", "corby", OCTO_TAG_ACCOUNT_MANAGERS]
        )

        if ok:
            text = (
                f"Склад {warehouse_name} закончился.\n"
                f"Успешно тегнул на {next_warehouse}✅"
            )
        else:
            text = (
                f"Склад {warehouse_name} закончился.\n"
                f"Не удалось тегнуть на {next_warehouse}❌\n"
                f"Детали: {msg}"
            )
    else:
        text = (
            f"Склад {warehouse_name} закончился.\n"
            f"Следующего склада для выдачи не найдено."
        )

    tg_send_message(FP_WAREHOUSE_NOTIFY_ADMIN_ID, text)

def get_next_farm_fp_warehouse_name(current_warehouse):
    rows = get_sheet_rows_cached(SHEET_FARM_FPS)

    warehouses = set()
    for row in rows[1:]:
        if len(row) < 5:
            row = row + [''] * (5 - len(row))

        warehouse = str(row[4]).strip()
        if warehouse:
            warehouses.add(warehouse)

    if not warehouses:
        return None

    ordered = sorted(warehouses, key=extract_warehouse_sort_key)

    current = str(current_warehouse or "").strip()
    if current not in ordered:
        return None

    idx = ordered.index(current)
    if idx + 1 < len(ordered):
        return ordered[idx + 1]

    return None


def count_free_farm_fp_in_warehouse(warehouse_name):
    rows = get_sheet_rows_cached(SHEET_FARM_FPS)

    count = 0
    target = str(warehouse_name or "").strip()

    for row in rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        warehouse = str(row[4]).strip()
        status = str(row[5]).strip().lower()

        if warehouse == target and status == "free":
            count += 1

    return count


def get_current_open_farm_fp_warehouse():
    rows = get_sheet_rows_cached(SHEET_FARM_FPS)

    free_warehouses = set()
    for row in rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        warehouse = str(row[4]).strip()
        status = str(row[5]).strip().lower()

        if warehouse and status == "free":
            free_warehouses.add(warehouse)

    if not free_warehouses:
        return None

    ordered = sorted(free_warehouses, key=extract_warehouse_sort_key)
    return ordered[0]


def notify_admin_farm_fp_warehouse_finished(warehouse_name):
    next_warehouse = get_next_farm_fp_warehouse_name(warehouse_name)

    if next_warehouse:
        ok, msg = octo_update_profile_tags_by_title(
            profile_title=next_warehouse,
            tags_to_add=["Sido", "corby", OCTO_TAG_FARMERS]
        )

        if ok:
            text = (
                f"Склад {warehouse_name} закончился.\n"
                f"Успешно тегнул на {next_warehouse}✅"
            )
        else:
            text = (
                f"Склад {warehouse_name} закончился.\n"
                f"Не удалось тегнуть на {next_warehouse}❌\n"
                f"Детали: {msg}"
            )
    else:
        text = (
            f"Склад {warehouse_name} закончился.\n"
            f"Следующего склада для выдачи не найдено."
        )

    for admin_id in FARM_FP_WAREHOUSE_NOTIFY_ADMIN_IDS:
        try:
            tg_send_message(admin_id, text)
        except Exception:
            logging.exception(
                f"notify_admin_farm_fp_warehouse_finished failed for admin_id={admin_id}"
            )

def find_free_fp(exclude_link=None):
    rows = get_sheet_rows_cached(SHEET_FPS)

    candidates = []

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        fp_link = str(row[0]).strip()
        purchase_date_raw = str(row[1]).strip()
        warehouse = str(row[4]).strip()
        status = str(row[5]).strip().lower()

        if status != "free":
            continue

        if exclude_link and fp_link == exclude_link:
            continue

        purchase_date = parse_date(purchase_date_raw) or datetime.max
        warehouse_key = extract_warehouse_sort_key(warehouse)

        candidates.append({
            "row_index": idx,
            "fp_link": fp_link,
            "purchase_date_obj": purchase_date,
            "purchase_date": purchase_date_raw,
            "price": row[2],
            "supplier": row[3],
            "warehouse": warehouse,
            "warehouse_key": warehouse_key
        })

    if not candidates:
        return None

    # сначала по складу, потом по дате покупки
    candidates.sort(key=lambda x: (x["warehouse_key"], x["purchase_date_obj"]))
    return candidates[0]


def show_found_fp(chat_id, user_id, found):
    state = get_state(user_id)
    state["mode"] = "fp_found"
    state["fp_row"] = found["row_index"]
    state["found_fp_link"] = found["fp_link"]
    set_state(user_id, state)

    text = (
        "Найдено ФП:\n\n"
        f"Ссылка: {found['fp_link']}\n"
        f"Дата покупки: {found['purchase_date']}\n"
        f"Цена: {found['price']}\n"
        f"Для кого: {state.get('fp_for_whom', 'не указано')}"
    )

    keyboard = [
        [{"text": BTN_FP_CONFIRM}, {"text": BTN_FP_NEXT}],
        [{"text": BTN_BACK_TO_MENU}]
    ]

    tg_send_message(chat_id, text, keyboard)


def confirm_fp_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)

            if state.get("mode") != "fp_found":
                send_fps_menu(chat_id, "Сначала выбери ФП заново.")
                return

            fp_for_whom = state.get("fp_for_whom", "").strip()
            if not fp_for_whom:
                clear_state(user_id)
                send_fps_menu(chat_id, "Не найдено для кого выдавать ФП. Начни заново.")
                return

            row_index = state.get("fp_row")
            if not row_index:
                clear_state(user_id)
                send_fps_menu(chat_id, "Не найдено выбранное ФП. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_FPS, force=True)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_fps_menu(chat_id, "ФП не найдено в таблице. Начни заново.")
                return

            row = rows[row_index - 1]

            if len(row) < 9:
                row = row + [''] * (9 - len(row))

            status = str(row[5]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_fps_menu(chat_id, "Это ФП уже занято.")
                return

            if status == "ban":
                clear_state(user_id)
                send_fps_menu(chat_id, "Это ФП уже в ban.")
                return

            if status != "free":
                clear_state(user_id)
                send_fps_menu(chat_id, "Это ФП недоступно.")
                return

            fp_link = row[0]
            purchase_date = row[1]
            price = row[2]
            supplier = row[3]
            warehouse_name = row[4]

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            sheet_update_raw(
                SHEET_FPS,
                f"F{row_index}:I{row_index}",
                [[
                    "taken",
                    fp_for_whom,
                    who_took_text,
                    today
                ]]
            )

            sheet_append_row_and_refresh(
                SHEET_ISSUES,
                [
                    fp_link,
                    "FP",
                    purchase_date,
                    normalize_numeric_for_sheet(price),
                    today,
                    supplier,
                    fp_for_whom
                ],
                value_input_option="USER_ENTERED"
            )

            refresh_sheet_cache(SHEET_FPS)
            refresh_sheet_cache(SHEET_ISSUES)
            invalidate_stats_cache()

            remaining_in_warehouse = count_free_fp_in_warehouse(warehouse_name)
            if remaining_in_warehouse == 0:
                try:
                    notify_admin_fp_warehouse_finished(warehouse_name)
                except Exception:
                    logging.exception("notify_admin_fp_warehouse_finished crashed")

            clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"ФП выдано.\n"
            f"🔗Ссылка: {fp_link}\n"
            f"🗃Склад: {warehouse_name}\n"
            f"👨‍💻Для кого: {fp_for_whom}"
        )

        send_accounts_main_menu(chat_id, "Меню Accounts:")

    except Exception:
        logging.exception("confirm_fp_issue crashed")
        tg_send_message(chat_id, "Ошибка выдачи ФП. Попробуй ещё раз.")
        send_accounts_main_menu(chat_id, "Меню Accounts:")

def issue_fps_bulk(chat_id, user_id, username, count_needed):
    try:
        state = get_state(user_id)

        if state.get("mode") != "awaiting_fp_count":
            send_fps_menu(chat_id, "Сначала начни выдачу ФП заново.")
            return

        fp_for_whom = state.get("fp_for_whom", "").strip()
        if not fp_for_whom:
            clear_state(user_id)
            send_fps_menu(chat_id, "Не найдено для кого выдавать ФП. Начни заново.")
            return

        rows = get_sheet_rows_cached(SHEET_FPS)
        candidates = []

        for idx, row in enumerate(rows[1:], start=2):
            if len(row) < 9:
                row = row + [''] * (9 - len(row))

            if str(row[5]).strip().lower() != "free":
                continue

            purchase_date = parse_date(row[1]) or datetime.max
            warehouse_key = extract_warehouse_sort_key(row[4])
            candidates.append((idx, warehouse_key, purchase_date, row))

        candidates.sort(key=lambda x: (x[1], x[2]))
        selected = candidates[:count_needed]

        if len(selected) < count_needed:
            clear_state(user_id)
            send_fps_menu(chat_id, f"Недостаточно свободных ФП. Доступно: {len(selected)}")
            return

        today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
        who_took_text = f"@{username}" if username else "без username"

        issued_items = []

        with issue_lock:
            current_rows = get_sheet_rows_cached(SHEET_FPS, force=True)

            for row_index, _, _, _ in selected:
                if row_index - 1 >= len(current_rows):
                    clear_state(user_id)
                    send_fps_menu(chat_id, "Одна из ФП пропала из таблицы. Начни заново.")
                    return

                row = current_rows[row_index - 1]
                if len(row) < 9:
                    row = row + [''] * (9 - len(row))

                if str(row[5]).strip().lower() != "free":
                    clear_state(user_id)
                    send_fps_menu(chat_id, "Одна из ФП уже не свободна. Начни заново.")
                    return

            issue_rows = []
            warehouses_touched = []

            for row_index, _, _, _ in selected:
                row = current_rows[row_index - 1]
                if len(row) < 9:
                    row = row + [''] * (9 - len(row))

                warehouse_name = row[4]

                sheet_update_raw(
                    SHEET_FPS,
                    f"F{row_index}:I{row_index}",
                    [[
                        "taken",
                        fp_for_whom,
                        who_took_text,
                        today
                    ]]
                )

                issue_rows.append([
                    row[0],
                    "FP",
                    row[1],
                    normalize_numeric_for_sheet(row[2]),
                    today,
                    row[3],
                    fp_for_whom
                ])

                issued_items.append({
                    "fp_link": row[0],
                    "warehouse": warehouse_name
                })

                warehouses_touched.append(warehouse_name)

            refresh_sheet_cache(SHEET_FPS)

            if issue_rows:
                sheet_append_rows_and_refresh(
                    SHEET_ISSUES,
                    issue_rows,
                    value_input_option="USER_ENTERED"
                )

            for warehouse_name in sorted(set(warehouses_touched), key=extract_warehouse_sort_key):
                remaining_in_warehouse = count_free_fp_in_warehouse(warehouse_name)
                if remaining_in_warehouse == 0:
                    try:
                        notify_admin_fp_warehouse_finished(warehouse_name)
                    except Exception:
                        logging.exception("notify_admin_fp_warehouse_finished crashed")

            invalidate_stats_cache()

        clear_state(user_id)

        for item in issued_items:
            tg_send_message(
                chat_id,
                f"Готово ✅\n\n"
                f"ФП выдано.\n"
                f"🔗Ссылка: {item['fp_link']}\n"
                f"🗃Склад: {item['warehouse']}\n"
                f"👨‍💻Для кого: {fp_for_whom}"
            )

        send_accounts_main_menu(chat_id, "Меню Accounts:")

    except Exception:
        logging.exception("issue_fps_bulk crashed")
        tg_send_message(chat_id, "Ошибка массовой выдачи ФП. Попробуй ещё раз.")
        send_accounts_main_menu(chat_id, "Меню Accounts:")

def confirm_pixel_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)

            if state.get("mode") != "pixel_found":
                send_pixels_menu(chat_id, "Сначала выбери Пиксель заново.")
                return

            pixel_for_whom = state.get("pixel_for_whom", "").strip()
            if not pixel_for_whom:
                clear_state(user_id)
                send_pixels_menu(chat_id, "Не найдено для кого выдавать Пиксель. Начни заново.")
                return

            row_index = state.get("pixel_row")
            if not row_index:
                clear_state(user_id)
                send_pixels_menu(chat_id, "Не найден выбранный Пиксель. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_PIXELS)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_pixels_menu(chat_id, "Пиксель не найден в таблице. Начни заново.")
                return

            row = rows[row_index - 1]
            row = ensure_row_len(row, 9)
            sync_id = row[8]

            status = str(row[3]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_pixels_menu(chat_id, "Этот Пиксель уже занят.")
                return

            if status == "ban":
                clear_state(user_id)
                send_pixels_menu(chat_id, "Этот Пиксель уже в ban.")
                return

            if status != "free":
                clear_state(user_id)
                send_pixels_menu(chat_id, "Этот Пиксель недоступен.")
                return

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"
            data_text = row[7]
            pixel_name = extract_pixel_name_from_data(data_text)
            pixel_id = extract_pixel_id_from_data(data_text)
            issue_pixel_value = pixel_id or pixel_name

            sheet_update_and_refresh(
                SHEET_PIXELS,
                f"D{row_index}:G{row_index}",
                [[
                    "taken",
                    pixel_for_whom,
                    today,
                    who_took_text
                ]]
            )

            if sync_id:
                sync_status_to_basebot(BASEBOT_SHEET_PIXELS, sync_id, "taken")

            append_issue_row_fixed([
                issue_pixel_value,
                "PIXEL",
                row[0],
                normalize_numeric_for_sheet(row[1]),
                today,
                row[2],
                pixel_for_whom
            ])

            invalidate_stats_cache()
            clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"Пиксель выдан.\n"
            f"🔥id Пикселя: {pixel_id}\n"
            f"👨‍💻Для кого: {pixel_for_whom}"
        )

        if data_text:
            tg_send_message(chat_id, data_text)
        else:
            tg_send_message(chat_id, "Данные Пикселя не найдены.")

        send_pixels_menu(chat_id, "Выбери следующее действие:")

    except Exception:
        logging.exception("confirm_pixel_issue crashed")
        tg_send_message(chat_id, "Ошибка выдачи Пикселя. Попробуй ещё раз.")
        send_pixels_menu(chat_id, "Меню Пикселей:")
        
def issue_pixels_bulk(chat_id, user_id, username, count_needed):
    try:
        state = get_state(user_id)

        if state.get("mode") != "awaiting_pixel_count":
            send_pixels_menu(chat_id, "Сначала начни выдачу Пикселей заново.")
            return

        pixel_for_whom = state.get("pixel_for_whom", "").strip()
        if not pixel_for_whom:
            clear_state(user_id)
            send_pixels_menu(chat_id, "Не найдено для кого выдавать Пиксели. Начни заново.")
            return

        try:
            count_needed = int(count_needed)
        except Exception:
            tg_send_message(chat_id, "Количество Пикселей должно быть числом.")
            return

        if count_needed <= 0:
            tg_send_message(chat_id, "Количество должно быть больше нуля.")
            return

        found_pixels = find_free_pixels(count_needed)

        if len(found_pixels) < count_needed:
            clear_state(user_id)
            send_pixels_menu(
                chat_id,
                f"Недостаточно свободных Пикселей. Доступно: {len(found_pixels)}"
            )
            return

        today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
        who_took_text = f"@{username}" if username else "без username"

        issued_messages = []
        issue_rows = []

        with issue_lock:
            current_rows = get_sheet_rows_cached(SHEET_PIXELS, force=True)

            for item in found_pixels:
                row_index = item["row_index"]

                if row_index - 1 >= len(current_rows):
                    clear_state(user_id)
                    send_pixels_menu(chat_id, "Один из Пикселей пропал из таблицы. Начни заново.")
                    return

                row = current_rows[row_index - 1]
                row = ensure_row_len(row, 9)
                current_rows[row_index - 1] = row
                sync_id = row[8]

                status = str(row[3]).strip().lower()
                if status != "free":
                    clear_state(user_id)
                    send_pixels_menu(chat_id, "Один из Пикселей уже не свободен. Начни заново.")
                    return

            for item in found_pixels:
                row_index = item["row_index"]
                row = current_rows[row_index - 1]

                if len(row) < 8:
                    row = row + [''] * (8 - len(row))
                    current_rows[row_index - 1] = row

                data_text = str(row[7] or "").strip()
                pixel_name = extract_pixel_name_from_data(data_text)
                pixel_id = extract_pixel_id_from_data(data_text)
                issue_pixel_value = pixel_id or pixel_name

                sheet_update_raw(
                    SHEET_PIXELS,
                    f"D{row_index}:G{row_index}",
                    [[
                        "taken",
                        pixel_for_whom,
                        today,
                        who_took_text
                    ]]
                )

                row[3] = "taken"
                row[4] = pixel_for_whom
                row[5] = today
                row[6] = who_took_text

                issue_rows.append([
                    issue_pixel_value,
                    "PIXEL",
                    row[0],
                    normalize_numeric_for_sheet(row[1]),
                    today,
                    row[2],
                    pixel_for_whom
                ])

                issued_messages.append({
                    "pixel_name": pixel_name,
                    "pixel_id": pixel_id,
                    "data_text": data_text,
                    "sync_id": sync_id
                })

            if issue_rows:
                append_issue_rows_fixed(issue_rows)

            with table_cache_lock:
                table_cache[SHEET_PIXELS]["rows"] = current_rows
                table_cache[SHEET_PIXELS]["updated_at"] = time.time()

            invalidate_stats_cache()

        for item in issued_messages:
            if item["sync_id"]:
                sync_status_to_basebot(BASEBOT_SHEET_PIXELS, item["sync_id"], "taken")

        clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"🔢Выдано Пикселей: {len(issued_messages)}\n"
            f"👨‍💻Для кого: {pixel_for_whom}"
        )

        for i, item in enumerate(issued_messages, start=1):
            text_to_send = (
                f"Пиксель {i}: {item['pixel_name']}\n"
                f"🔥id Пикселя: {item['pixel_id']}\n\n"
                f"{item['data_text']}"
            )
            tg_send_long_message(chat_id, text_to_send)

        send_pixels_menu(chat_id, "Выбери следующее действие:")

    except Exception as e:
        logging.exception("issue_pixels_bulk crashed")
        tg_send_message(chat_id, f"Ошибка выдачи Пикселей.\n\n{e}")
        send_pixels_menu(chat_id, "Меню Пикселей:")

def extract_pixel_id_from_data(data_text):
    text = str(data_text or "").strip()
    if not text:
        return ""

    match = re.search(r'ID\s*пикселя\s*:\s*([0-9]+)', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    return ""


def find_pixel_in_base_by_data(pixel_query):
    rows = get_sheet_rows_cached(SHEET_PIXELS)
    target = str(pixel_query).strip().lower()
    target_digits = re.sub(r"\D", "", target)

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 8:
            row = row + [''] * (8 - len(row))

        data_text = str(row[7]).strip()
        data_text_lower = data_text.lower()

        pixel_id = extract_pixel_id_from_data(data_text)

        # 1) точный поиск по ID пикселя
        if target_digits and pixel_id and target_digits == pixel_id:
            return {
                "row_index": idx,
                "row": row
            }

        # 2) запасной поиск по тексту
        if target and target in data_text_lower:
            return {
                "row_index": idx,
                "row": row
            }

    return None


def build_pixel_search_text(pixel_query):
    found = find_pixel_in_base_by_data(pixel_query)
    if not found:
        return None

    row = found["row"]
    if len(row) < 8:
        row = row + [''] * (8 - len(row))

    return (
        f"Дата покупки: {row[0] or 'не указана'}\n"
        f"Цена: {row[1] or 'не указана'}\n"
        f"У кого купили: {row[2] or 'не указан'}\n"
        f"Статус: {row[3] or 'не указан'}\n"
        f"Для кого: {row[4] or 'не указано'}\n"
        f"Дата выдачи: {row[5] or 'не указана'}\n"
        f"Кто взял: {row[6] or 'не указано'}\n\n"
        f"Данные:\n{row[7] or 'нет данных'}"
    )

def extract_pixel_name_from_data(data_text):
    text = str(data_text or "").strip()

    if not text:
        return "PIXEL"

    # ищем строку вида: Имя пикселя: px13
    match = re.search(r'Имя\s*пикселя\s*:\s*(.+)', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    # запасной вариант — просто первая непустая строка
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line[:100]

    return "PIXEL"

def find_free_pixels(count_needed, exclude_rows=None):
    rows = get_sheet_rows_cached(SHEET_PIXELS)

    exclude_rows = set(exclude_rows or [])
    candidates = []

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 8:
            row = row + [''] * (8 - len(row))

        status = str(row[3]).strip().lower()
        if status != "free":
            continue

        if idx in exclude_rows:
            continue

        purchase_date = parse_date(row[0]) or datetime.max

        candidates.append({
            "row_index": idx,
            "purchase_date_obj": purchase_date,
            "purchase_date": row[0],
            "price": row[1],
            "supplier": row[2],
            "data_text": row[7]
        })

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[:count_needed]

def find_free_pixel(exclude_row=None):
    rows = get_sheet_rows_cached(SHEET_PIXELS)

    candidates = []
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 8:
            row = row + [''] * (8 - len(row))

        status = str(row[3]).strip().lower()
        if status != "free":
            continue

        if exclude_row and idx == exclude_row:
            continue

        purchase_date = parse_date(row[0]) or datetime.max

        candidates.append({
            "row_index": idx,
            "purchase_date_obj": purchase_date,
            "purchase_date": row[0],
            "price": row[1],
            "supplier": row[2],
            "data_text": row[7]
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[0]


def show_found_pixel(chat_id, user_id, found):
    state = get_state(user_id)
    state["mode"] = "pixel_found"
    state["pixel_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "Найден Пиксель:\n\n"
        f"Дата покупки: {found['purchase_date']}\n"
        f"Цена: {found['price']}\n"
        f"Для кого: {state.get('pixel_for_whom', 'не указано')}"
    )

    keyboard = [
        [{"text": BTN_PIXEL_CONFIRM}, {"text": BTN_PIXEL_NEXT}],
        [{"text": BTN_BACK_TO_MENU}]
    ]

    tg_send_message(chat_id, text, keyboard)


def find_last_pixel_issue_row(pixel_name=None, pixel_id=None):
    rows = get_sheet_rows_cached(SHEET_ISSUES)

    last_match = None
    target_name = str(pixel_name or "").strip().lower()
    target_id = str(pixel_id or "").strip()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 7:
            row = row + [''] * (7 - len(row))

        issue_name = str(row[0]).strip().lower()
        issue_type = str(row[1]).strip().lower()

        if issue_type != "pixel":
            continue

        if target_name and issue_name == target_name:
            last_match = {
                "row_index": idx,
                "row": row
            }

        elif target_id and target_id in issue_name:
            last_match = {
                "row_index": idx,
                "row": row
            }

    return last_match


def return_pixel_to_ban(pixel_query, comment_text=""):
    found = find_pixel_in_base_by_data(pixel_query)
    if not found:
        return False, "Пиксель не найден."

    row = found["row"]
    if len(row) < 8:
        row = row + [''] * (8 - len(row))

    status = str(row[3]).strip().lower()
    if status == "ban":
        return False, "Этот Пиксель уже в ban."

    data_text = row[7]
    pixel_name = extract_pixel_name_from_data(data_text)
    pixel_id = extract_pixel_id_from_data(data_text)

    sheet_update_and_refresh(
        SHEET_PIXELS,
        f"D{found['row_index']}:E{found['row_index']}",
        [["ban", "ban"]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[8]
    
    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_PIXELS, sync_id, "ban")

    issue_info = find_last_pixel_issue_row(pixel_name=pixel_name, pixel_id=pixel_id)
    if issue_info:
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, "Пиксель переведён в ban."

def return_pixel_to_free(pixel_query):
    found = find_pixel_in_base_by_data(pixel_query)
    if not found:
        return False, "Пиксель не найден."

    row = found["row"]
    if len(row) < 8:
        row = row + [''] * (8 - len(row))

    status = str(row[3]).strip().lower()

    if status == "free":
        return False, "Этот Пиксель уже free."

    data_text = row[7]
    pixel_name = extract_pixel_name_from_data(data_text)
    pixel_id = extract_pixel_id_from_data(data_text)

    sheet_update_and_refresh(
        SHEET_PIXELS,
        f"D{found['row_index']}:G{found['row_index']}",
        [["free", "", "", ""]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[8]

    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_PIXELS, sync_id, "free")

    issue_info = find_last_pixel_issue_row(pixel_name=pixel_name, pixel_id=pixel_id)
    if issue_info:
        sheet_delete_row_and_refresh(SHEET_ISSUES, issue_info["row_index"])

    invalidate_stats_cache()
    return True, "Пиксель возвращён в free."

def get_free_farm_king_geos():
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    geos = []
    seen = set()

    for row in rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        status = str(row[4]).strip().lower()
        geo = str(row[7]).strip()

        if status == "free" and geo and geo not in seen:
            geos.append(geo)
            seen.add(geo)

    return geos


def send_farm_king_geo_options(chat_id):
    geos = get_free_farm_king_geos()

    if not geos:
        send_farm_kings_menu(chat_id, "Нет свободных фарм кингов ни по одному GEO.")
        return

    keyboard = []
    for geo in geos:
        keyboard.append([{"text": geo}])

    keyboard.append([{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}])

    tg_send_message(chat_id, "Какое GEO нужно?", keyboard)

def send_free_farm_kings(chat_id):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    free_rows = []
    for row in rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        if str(row[4]).strip().lower() == "free":
            free_rows.append(row)

    if not free_rows:
        tg_send_message(chat_id, "Свободных фарм кингов сейчас нет.")
        return

    geo_stats = {}
    for row in free_rows:
        geo = str(row[7]).strip()
        if geo:
            geo_stats[geo] = geo_stats.get(geo, 0) + 1

    lines = [f"{geo} — {count}" for geo, count in sorted(geo_stats.items())]

    text = (
        f"Свободные фарм кинги: {len(free_rows)}\n\n"
        + "\n".join(lines)
    )

    tg_send_message(chat_id, text)

def find_free_farm_kings(count_needed, geo=None):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    candidates = []
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        if str(row[4]).strip().lower() != "free":
            continue

        row_geo = str(row[7]).strip()
        if geo is not None and row_geo != geo:
            continue

        purchase_date = parse_date(row[1]) or datetime.max
        candidates.append({
            "row_index": idx,
            "row": row,
            "purchase_date_obj": purchase_date
        })

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[:count_needed]

def get_free_farm_king_prices_by_geo(geo):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    prices = []
    seen = set()
    geo = str(geo or "").strip()

    for row in rows[1:]:
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if current_name:
            continue
        if not price:
            continue

        if price not in seen:
            seen.add(price)
            prices.append(price)

    def price_sort_key(x):
        try:
            return (0, float(str(x).replace(",", ".")))
        except Exception:
            return (1, str(x))

    prices.sort(key=price_sort_key)
    return prices


def send_farm_king_price_options(chat_id, geo):
    prices = get_free_farm_king_prices_by_geo(geo)

    if not prices:
        send_farm_kings_menu(chat_id, f"Нет свободных farm king с GEO {geo}.")
        return

    keyboard = []
    row = []

    for price in prices:
        row.append({"text": price})
        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}])

    tg_send_message(chat_id, f"Выбери цену для GEO {geo}:", keyboard)


def find_free_farm_king_by_geo_and_price(geo, price, exclude_row=None):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    candidates = []
    geo = str(geo or "").strip()
    price = normalize_price_key(price)

    for idx, row in enumerate(rows[1:], start=2):
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        row_price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if row_price != price:
            continue
        if current_name:
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
            "data_text": get_full_king_data_from_row(row),
            "row": row
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[0]


def find_free_farm_kings_by_geo_and_price(count_needed, geo, price):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    candidates = []
    geo = str(geo or "").strip()
    price = normalize_price_key(price)

    for idx, row in enumerate(rows[1:], start=2):
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        row_price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if row_price != price:
            continue
        if current_name:
            continue

        purchase_date = parse_date(row[1]) or datetime.max

        candidates.append({
            "row_index": idx,
            "purchase_date_obj": purchase_date,
            "purchase_date": row[1],
            "price": row[2],
            "supplier": row[3],
            "geo": row[7],
            "data_text": get_full_king_data_from_row(row),
            "row": row
        })

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[:count_needed]

def show_found_farm_king_octo(chat_id, user_id, found):
    state = get_state(user_id)

    state["mode"] = FARM_KING_OCTO_MODE_FOUND
    state["farm_king_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "🔍Найден farm king:\n\n"
        f"🗓Дата покупки: {found['purchase_date']}\n"
        f"💵Цена: {found['price']}\n"
        f"🌐Гео: {found['geo']}\n"
        f"👨‍💻Для кого: farm\n"
        f"✏️Название: {state.get('farm_king_name', 'не указано')}"
    )

    if has_bm_in_king_data(found.get("data_text", "")):
        text += "\n✅Есть BM"

    sent = tg_send_inline_message(
        chat_id,
        text,
        [[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_farm_king_octo:{user_id}"
            },
            {
                "text": "🔄другой",
                "callback_data": f"other_farm_king_octo:{user_id}"
            }
        ]]
    )

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state = get_state(user_id)
                state["farm_king_preview_message_id"] = message_id
                set_state(user_id, state)
    except Exception:
        logging.exception("show_found_farm_king_octo failed to save preview message_id")


def edit_found_farm_king_octo_preview(chat_id, message_id, user_id, found):
    state = get_state(user_id)

    state["mode"] = FARM_KING_OCTO_MODE_FOUND
    state["farm_king_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "🔍Найден farm king:\n\n"
        f"🗓Дата покупки: {found['purchase_date']}\n"
        f"💵Цена: {found['price']}\n"
        f"🌐Гео: {found['geo']}\n"
        f"👨‍💻Для кого: farm\n"
        f"✏️Название: {state.get('farm_king_name', 'не указано')}"
    )

    if has_bm_in_king_data(found.get("data_text", "")):
        text += "\n✅Есть BM"

    tg_edit_message_text(
        chat_id,
        message_id,
        text,
        inline_buttons=[[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_farm_king_octo:{user_id}"
            },
            {
                "text": "🔄другой",
                "callback_data": f"other_farm_king_octo:{user_id}"
            }
        ]]
    )


def mark_farm_king_octo_preview_as_issued(chat_id, message_id, king_name, price, geo_value):
    text = (
        "✅ Выдано\n\n"
        f"Farm king выдан.\n"
        f"Название: {king_name}\n"
        f"Для кого: farm\n"
        f"Цена: {price}\n"
        f"Гео: {geo_value}"
    )

    try:
        tg_edit_message_text(
            chat_id,
            message_id,
            text,
            inline_buttons=[]
        )
    except Exception:
        logging.exception("mark_farm_king_octo_preview_as_issued failed")


def send_farm_kings_bulk_found_preview_once(chat_id, user_id):
    state = get_state(user_id)

    queue = state.get("farm_kings_bulk_queue", [])
    if not queue:
        send_farm_kings_menu(chat_id, "Не удалось собрать список farm king.")
        return

    lines = [
        "🔍Найдены farm king:",
        "",
        "👨‍💻Для кого: farm",
        ""
    ]

    for i, item in enumerate(queue, start=1):
        lines.append(f"📦Кинг {i} из {len(queue)}")
        lines.append(f"✏️Название: {item.get('king_name', 'не указано')}")
        lines.append(f"💵Цена: {item.get('price', '')}")
        lines.append(f"🌐Гео: {item.get('geo', '')}")

        if has_bm_in_king_data(item.get("data_text", "")):
            lines.append("✅Есть BM")

        lines.append("")

    sent = tg_send_inline_message(
        chat_id,
        "\n".join(lines).strip(),
        [[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_farm_kings_bulk_octo:{user_id}"
            },
            {
                "text": "❌отмена",
                "callback_data": f"cancel_farm_kings_bulk_octo:{user_id}"
            }
        ]]
    )

    state["mode"] = FARM_KING_OCTO_MODE_BULK_CONFIRM

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state["farm_kings_bulk_confirm_message_id"] = message_id
    except Exception:
        logging.exception("send_farm_kings_bulk_found_preview_once failed to save message_id")

    set_state_with_custom_ttl(user_id, state, FARM_KING_BULK_PROXY_TTL)


def start_farm_kings_bulk_proxy_step(chat_id, user_id):
    state = get_state(user_id)

    queue = state.get("farm_kings_bulk_queue", [])
    current_index = int(state.get("farm_kings_bulk_current_index", 0))

    if current_index >= len(queue):
        finish_farm_kings_bulk(chat_id, user_id)
        return

    if state.get("farm_kings_bulk_skip_all_proxies"):
        process_farm_kings_bulk_proxy_step(
            chat_id=chat_id,
            user_id=user_id,
            username=state.get("farm_kings_bulk_username", ""),
            proxy_text="__SKIP_ALL_PROXIES__"
        )
        return

    current_item = queue[current_index]
    king_name = current_item.get("king_name", "")
    geo = current_item.get("geo", "")
    price = current_item.get("price", "")

    text = (
        f"Скинь socks5 proxy для farm king {king_name}\n\n"
        f"Цена: {price}\n"
        f"Гео: {geo}\n"
        f"Шаг {current_index + 1} из {len(queue)}\n\n"
        f"Формат:\n"
        f"socks5://login:password@host:port\n"
        f"или\n"
        f"socks5://host:port"
    )

    sent = tg_send_inline_message(
        chat_id,
        text,
        [[
            {
                "text": "⏭️ Пропустить все прокси",
                "callback_data": f"farm_kings_bulk_skip_all_proxies:{user_id}"
            }
        ]]
    )

    state["mode"] = FARM_KING_OCTO_MODE_BULK_PROXY

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state["farm_kings_bulk_proxy_message_id"] = message_id
    except Exception:
        logging.exception("start_farm_kings_bulk_proxy_step failed to save message_id")

    set_state_with_custom_ttl(user_id, state, FARM_KING_BULK_PROXY_TTL)


def build_farm_kings_bulk_result_messages(results, max_len=3500):
    success_items = [x for x in results if x.get("octo_ok")]
    failed_items = [x for x in results if not x.get("octo_ok")]

    if success_items and not failed_items:
        header = "✅ Farm king заведены в Octo"
    elif failed_items and not success_items:
        header = "❌ Farm king не заведены в Octo"
    else:
        header = "⚠️ Часть farm king заведена в Octo"

    header_text = f"{header}\n👨‍💻Для кого: farm"

    blocks = []

    for item in results:
        king_name = str(item.get("king_name", "")).strip() or "не указано"
        price = str(item.get("price", "")).strip() or "не указана"
        geo = str(item.get("geo", "")).strip() or "не указано"

        block_lines = [
            f"✏️Название: {king_name}",
            f"💵Цена: {price}",
            f"🌐Гео: {geo}",
        ]

        if not item.get("octo_ok"):
            error_text = str(item.get("error_text", "")).strip() or "не удалось завести в Octo"
            block_lines.append(f"❌Ошибка: {error_text}")

        blocks.append("\n".join(block_lines))

    messages = []
    current = header_text
    first_block = True

    for block in blocks:
        if first_block:
            separator = "\n\n"
        elif current:
            separator = "\n"
        else:
            separator = ""

        candidate = f"{current}{separator}{block}" if current else block

        if len(candidate) <= max_len:
            current = candidate
            first_block = False
            continue

        if current:
            messages.append(current)

        current = block
        first_block = False

    if current:
        messages.append(current)

    return messages


def send_farm_kings_bulk_followup_messages(chat_id, results):
    tg_send_message(
        chat_id,
        "Вручную проверь и выставь:\n"
        "• User-Agent\n"
        "• расширения\n"
        "• куки"
    )

    ua_lines = []
    bm_lines = []

    for item in results:
        if not item.get("octo_ok"):
            continue

        king_name = item.get("king_name", "")
        parsed = item.get("parsed_farm_king", {}) or {}

        user_agent = str(parsed.get("user_agent", "")).strip()
        bm_links = parsed.get("bm_links", []) or []
        bm_email_pairs = parsed.get("bm_email_pairs", []) or []

        if user_agent:
            ua_lines.append(f"у farm king {king_name} есть User-Agent✅")

        if bm_links or bm_email_pairs:
            bm_lines.append(f"у farm king {king_name} есть BM✅")

    if ua_lines:
        tg_send_message(chat_id, "\n".join(ua_lines))

    if bm_lines:
        tg_send_message(chat_id, "\n".join(bm_lines))


def finish_farm_kings_bulk(chat_id, user_id):
    state = get_state(user_id)

    results = state.get("farm_kings_bulk_results", [])

    if not results:
        clear_state(user_id)
        send_farm_kings_menu(chat_id, "Не удалось выдать ни одного farm king.")
        return

    message_parts = build_farm_kings_bulk_result_messages(results)

    success_items = [x for x in results if x.get("octo_ok")]
    inline_buttons = []

    if len(success_items) == 1:
        inline_buttons = [[
            {
                "text": "📄 Скачать txt",
                "callback_data": f"download_farm_king_bulk_txt:{user_id}:0"
            }
        ]]
    elif len(success_items) >= 2:
        inline_buttons = [[
            {
                "text": "📦 Скачать zip",
                "callback_data": f"download_farm_king_bulk_zip:{user_id}"
            }
        ]]

    tg_send_inline_message_parts(
        chat_id=chat_id,
        message_parts=message_parts,
        inline_buttons=inline_buttons
    )

    send_farm_kings_bulk_followup_messages(chat_id, results)

    set_state(user_id, {
        "mode": "farm_kings_bulk_done",
        "farm_kings_bulk_results": results,
        "updated_at": time.time()
    })

    send_farm_kings_menu(chat_id, "Выбери следующее действие:")

def farm_king_name_exists(king_name):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)
    target = str(king_name).strip().lower()

    if not target:
        return False

    for row in rows[1:]:
        existing_name = str(row[0]).strip().lower() if len(row) > 0 else ""
        if existing_name == target:
            return True

    return False


def find_farm_king_in_base_by_name(king_name):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)
    target = str(king_name).strip().lower()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        if str(row[0]).strip().lower() == target:
            return {
                "row_index": idx,
                "row": row
            }

    return None


def build_farm_king_search_text(king_name):
    found = find_farm_king_in_base_by_name(king_name)
    if not found:
        return None

    row = found["row"]
    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    full_data_text = get_full_king_data_from_row(row) or ""

    meta_text = (
        f"Farm king:\n"
        f"Название: {row[0]}\n"
        f"Статус: {row[4] or 'не указан'}\n"
        f"Цена: {row[2] or 'не указана'}\n"
        f"Дата взятия: {row[6] or 'не указана'}\n"
        f"Кто взял: {row[8] or 'не указано'}\n"
        f"Для кого: {row[5] or 'не указано'}"
    )

    return {
        "title": "Farm king",
        "king_name": row[0] or king_name,
        "meta_text": meta_text,
        "data_text": full_data_text
    }


def return_farm_king_to_ban(king_name, comment_text=""):
    found = find_farm_king_in_base_by_name(king_name)
    if not found:
        return False, "Кинг не найден в База фарм кинги."

    row = found["row"]
    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    if str(row[4]).strip().lower() == "ban":
        return False, "Этот кинг уже в ban."

    sheet_update_and_refresh(
        SHEET_FARM_KINGS,
        f"E{found['row_index']}:F{found['row_index']}",
        [["ban", "ban"]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[12]
    
    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_FARM_KINGS, sync_id, "ban")

    issue_info = find_last_king_issue_row(king_name)
    if issue_info:
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, f"Кинг '{king_name}' переведён в ban."

def return_farm_king_to_free(king_name):
    found = find_farm_king_in_base_by_name(king_name)
    if not found:
        return False, "Кинг не найден в База фарм кинги."

    row = found["row"]
    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    status = str(row[4]).strip().lower()

    if status == "free":
        return False, "Этот farm king уже free."

    old_king_name = str(row[0]).strip()

    sheet_update_and_refresh(
        SHEET_FARM_KINGS,
        f"A{found['row_index']}:I{found['row_index']}",
        [[
            "",          # A название очищаем
            row[1],
            row[2],
            row[3],
            "free",
            "",
            "",
            row[7],
            ""
        ]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[12]
    
    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_FARM_KINGS, sync_id, "free")

    if old_king_name:
        delete_last_king_issue_row(old_king_name)

    invalidate_stats_cache()
    return True, f"Farm king '{king_name}' возвращён в free."


def issue_farm_kings(chat_id, user_id, username, king_names):
    state = get_state(user_id)
    selected_rows = state.get("farm_king_rows", [])
    selected_geo = str(state.get("farm_king_geo", "")).strip()

    if not selected_rows or len(selected_rows) != len(king_names):
        clear_state(user_id)
        send_farm_kings_menu(chat_id, "Ошибка выдачи фарм кингов. Начни заново.")
        return

    count_needed = state.get("farm_kings_count", 0)
    if not count_needed or count_needed != len(king_names):
        clear_state(user_id)
        send_farm_kings_menu(chat_id, "Ошибка количества farm кингов. Начни заново.")
        return

    # защита от дублей названий
    duplicate_names = []
    for name in king_names:
        if farm_king_name_exists(name):
            duplicate_names.append(name)

    if duplicate_names:
        clear_state(user_id)
        tg_send_message(
            chat_id,
            "Эти названия уже существуют:\n" + "\n".join(duplicate_names[:20])
        )
        send_farm_kings_menu(chat_id, "Выдача отменена. Начни заново.")
        return

    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
    who_took_text = f"@{username}" if username else "без username"

    issue_rows = []
    issued_items = []

    with issue_lock:
        current_rows = get_sheet_rows_cached(SHEET_FARM_KINGS, force=True)

        # 1. Полная проверка перед записью
        for item, king_name in zip(selected_rows, king_names):
            row_index = item["row_index"]

            if row_index - 1 >= len(current_rows):
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Ошибка: один из кингов пропал из таблицы.")
                return

            row = ensure_row_len(current_rows[row_index - 1], 13)

            if str(row[4]).strip().lower() != "free":
                clear_state(user_id)
                send_farm_kings_menu(chat_id, f"Кинг '{row[0] or king_name}' уже не свободен.")
                return

            current_geo = str(row[7]).strip()
            if selected_geo and current_geo != selected_geo:
                clear_state(user_id)
                send_farm_kings_menu(
                    chat_id,
                    f"Ошибка GEO: ожидался {selected_geo}, но в строке найден {current_geo}.\nНачни заново."
                )
                return

        # 2. Обновление строк
        for item, king_name in zip(selected_rows, king_names):
            row_index = item["row_index"]
            row = ensure_row_len(current_rows[row_index - 1], 13)
            sync_id = row[12]

            current_geo = str(row[7]).strip()
            if selected_geo and current_geo != selected_geo:
                clear_state(user_id)
                send_farm_kings_menu(
                    chat_id,
                    f"Ошибка GEO перед записью: ожидался {selected_geo}, но найден {current_geo}.\nВыдача отменена."
                )
                return

            sheet_update_raw(
                SHEET_FARM_KINGS,
                f"A{row_index}:L{row_index}",
                [[
                    king_name,       # A название
                    row[1],          # B дата покупки
                    row[2],          # C цена
                    row[3],          # D supplier
                    "taken",         # E статус
                    "farm",          # F кому выдали
                    today,           # G дата выдачи
                    row[7],          # H geo
                    who_took_text,   # I кто выдал / кто взял
                    row[9],          # J data1
                    row[10],         # K data2
                    row[11]          # L data3
                ]]
            )

            issue_rows.append([
                king_name,
                "KING",
                row[1],
                normalize_numeric_for_sheet(row[2]),
                today,
                row[3],
                "farm"
            ])

            issued_items.append({
                "king_name": king_name,
                "purchase_date": row[1],
                "price": row[2],
                "supplier": row[3],
                "geo": row[7],
                "data_text": get_full_king_data_from_row(row),
                "sync_id": sync_id
            })

        refresh_sheet_cache(SHEET_FARM_KINGS)

        # 3. Запись в issues
        if issue_rows:
            sheet_append_rows_and_refresh(
                SHEET_ISSUES,
                issue_rows,
                value_input_option="USER_ENTERED"
            )

        # 4. Синк в BaseBot
        for item in issued_items:
            if item["sync_id"]:
                sync_status_to_basebot(BASEBOT_SHEET_FARM_KINGS, item["sync_id"], "taken")

        invalidate_stats_cache()

    clear_state(user_id)

    try:
        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"Выдано кингов: {len(issued_items)}"
        )
    except Exception:
        logging.exception("issue_farm_kings summary send failed")

    # 5. Отправка txt / zip
    if len(issued_items) > 5:
        try:
            archive_name = f"farm_kings_{datetime.now(MOSCOW_TZ).strftime('%Y%m%d_%H%M%S')}.zip"
            tg_send_kings_as_zip(
                chat_id=chat_id,
                issued_items=issued_items,
                archive_name=archive_name
            )
        except Exception:
            logging.exception("issue_farm_kings zip send failed")
            tg_send_message(chat_id, "Фарм кинги выданы, но zip-архив не удалось отправить.")
    else:
        txt_failed = []

        for item in issued_items:
            try:
                tg_send_message(
                    chat_id,
                    f"Готово ✅\n\n"
                    f"Кинг выдан.\n"
                    f"Название: {item['king_name']}\n"
                    f"Цена: {item['price']}\n"
                    f"Гео: {item['geo']}"
                )

                tg_send_king_data_as_txt(
                    chat_id=chat_id,
                    king_name=item["king_name"],
                    data_text=item["data_text"]
                )
            except Exception:
                logging.exception(f"issue_farm_kings send failed for {item['king_name']}")
                txt_failed.append(item["king_name"])

        if txt_failed:
            tg_send_message(
                chat_id,
                "Эти фарм кинги выданы, но txt не удалось отправить:\n" + "\n".join(txt_failed[:20])
            )

    send_farm_kings_menu(chat_id, "Выбери следующее действие:")


def count_free_farm_bms():
    rows = get_sheet_rows_cached(SHEET_FARM_BMS)

    count = 0
    for row in rows[1:]:
        if len(row) < 5:
            row = row + [''] * (5 - len(row))

        if str(row[4]).strip().lower() == "free":
            count += 1

    return count


def find_free_farm_bm(exclude_bm_id=None):
    rows = get_sheet_rows_cached(SHEET_FARM_BMS)

    candidates = []

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        bm_id = str(row[0]).strip()
        if exclude_bm_id and bm_id == exclude_bm_id:
            continue

        if str(row[4]).strip().lower() != "free":
            continue

        purchase_date = parse_date(row[1]) or datetime.max
        candidates.append({
            "row_index": idx,
            "row": row,
            "purchase_date_obj": purchase_date
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    item = candidates[0]
    row = item["row"]

    return {
        "row_index": item["row_index"],
        "bm_id": row[0],
        "purchase_date": row[1],
        "price": row[2],
        "supplier": row[3],
        "data_text": row[8]
    }


def find_farm_bm_in_base(bm_id):
    rows = get_sheet_rows_cached(SHEET_FARM_BMS)

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        if str(row[0]).strip() == str(bm_id).strip():
            return {
                "row_index": idx,
                "row": row
            }

    return None


def build_farm_bm_search_text(bm_id):
    found = find_farm_bm_in_base(bm_id)
    if not found:
        return None

    row = found["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    return (
        f"ID BM: {row[0]}\n"
        f"Дата покупки: {row[1] or 'не указана'}\n"
        f"Цена: {row[2] or 'не указана'}\n"
        f"Статус: {row[4] or 'не указан'}\n"
        f"Для кого: {row[5] or 'не указано'}\n"
        f"Кто взял: {row[6] or 'не указано'}\n"
        f"Дата выдачи: {row[7] or 'не указана'}\n\n"
        f"Данные:\n{row[8] or 'нет данных'}"
    )

def return_farm_bm_to_ban(bm_id, comment_text=""):
    found = find_farm_bm_in_base(bm_id)
    if not found:
        return False, "BM не найден в База фарм бм."

    row = found["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    status = str(row[4]).strip().lower()

    if status == "ban":
        return False, "Этот BM уже в ban."

    sheet_update_and_refresh(
        SHEET_FARM_BMS,
        f"E{found['row_index']}:F{found['row_index']}",
        [["ban", "ban"]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[9]
    
    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_FARM_BMS, sync_id, "ban")

    issue_info = find_last_bm_issue_row(bm_id)
    if issue_info:
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, f"BM '{bm_id}' переведён в ban."

def return_farm_bm_to_free(bm_id):
    found = find_farm_bm_in_base(bm_id)
    if not found:
        return False, "BM не найден в База фарм бм."

    row = found["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    status = str(row[4]).strip().lower()

    if status == "free":
        return False, "Этот farm BM уже free."

    sheet_update_and_refresh(
        SHEET_FARM_BMS,
        f"E{found['row_index']}:H{found['row_index']}",
        [["free", "", "", ""]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[9]
    
    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_FARM_BMS, sync_id, "free")

    delete_last_bm_issue_row(bm_id)

    invalidate_stats_cache()
    return True, f"Farm BM '{bm_id}' возвращён в free."

def issue_farm_bm(chat_id, user_id, username):
    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
    who_took_text = f"@{username}" if username else "без username"

    with issue_lock:
        found = find_free_farm_bm()

        if not found:
            send_farm_bms_menu(chat_id, "Свободных фарм BMов сейчас нет.")
            return

        row_index = found["row_index"]
        rows = get_sheet_rows_cached(SHEET_FARM_BMS)

        if row_index - 1 >= len(rows):
            send_farm_bms_menu(chat_id, "BM не найден в таблице.")
            return

        row = rows[row_index - 1]
        row = ensure_row_len(row, 10)
        sync_id = row[9]

        if str(row[4]).strip().lower() != "free":
            send_farm_bms_menu(chat_id, "Этот BM уже занят.")
            return

        sheet_update_and_refresh(
            SHEET_FARM_BMS,
            f"E{row_index}:H{row_index}",
            [[
                "taken",
                "farm",
                who_took_text,
                today
            ]]
        )

        if sync_id:
            sync_status_to_basebot(BASEBOT_SHEET_FARM_BMS, sync_id, "taken")

        append_issue_row_fixed([
            row[0],
            "БМ",
            row[1],
            normalize_numeric_for_sheet(row[2]),
            today,
            row[3],
            "farm"
        ])

        invalidate_stats_cache()

    tg_send_message(
        chat_id,
        f"Готово ✅\n\n"
        f"BM выдан.\n"
        f"🔥ID BM: {row[0]}"
    )

    if len(row) > 8 and row[8]:
        tg_send_message(chat_id, row[8])
    else:
        tg_send_message(chat_id, "Данные BM не найдены.")

    send_farm_bms_menu(chat_id, "Выбери следующее действие:")


def find_free_farm_fps(count_needed):
    rows = get_sheet_rows_cached(SHEET_FARM_FPS)

    current_warehouse = get_current_open_farm_fp_warehouse()
    if not current_warehouse:
        return []

    candidates = []
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        status = str(row[5]).strip().lower()
        warehouse = str(row[4]).strip()

        if status != "free":
            continue

        if warehouse != current_warehouse:
            continue

        purchase_date = parse_date(row[1]) or datetime.max
        candidates.append({
            "row_index": idx,
            "row": row,
            "purchase_date_obj": purchase_date
        })

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[:count_needed]


def find_farm_fp_in_base(fp_link):
    rows = get_sheet_rows_cached(SHEET_FARM_FPS)

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        if str(row[0]).strip() == str(fp_link).strip():
            return {
                "row_index": idx,
                "row": row
            }

    return None


def build_farm_fp_search_text(fp_link):
    found = find_farm_fp_in_base(fp_link)
    if not found:
        return None

    row = found["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    return (
        f"Ссылка FP: {row[0]}\n"
        f"Дата покупки: {row[1] or 'не указана'}\n"
        f"Цена: {row[2] or 'не указана'}\n"
        f"У кого купили: {row[3] or 'не указан'}\n"
        f"Склад: {row[4] or 'не указан'}\n"
        f"Статус: {row[5] or 'не указан'}\n"
        f"Для кого: {row[6] or 'не указано'}\n"
        f"Кто взял: {row[7] or 'не указано'}\n"
        f"Дата выдачи: {row[8] or 'не указана'}"
    )

def return_farm_fp_to_ban(fp_link, comment_text=""):
    found = find_farm_fp_in_base(fp_link)
    if not found:
        return False, "Farm FP не найдено."

    row = found["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    status = str(row[5]).strip().lower()

    if status == "ban":
        return False, "Это farm FP уже в ban."

    sheet_update_and_refresh(
        SHEET_FARM_FPS,
        f"F{found['row_index']}:G{found['row_index']}",
        [["ban", "ban"]]
    )

    issue_info = find_last_fp_issue_row(fp_link)
    if issue_info:
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, "Farm FP переведено в ban."


def return_farm_fp_to_free(fp_link):
    found = find_farm_fp_in_base(fp_link)
    if not found:
        return False, "Farm FP не найдено."

    row = found["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    status = str(row[5]).strip().lower()

    if status == "free":
        return False, "Это farm FP уже free."

    sheet_update_and_refresh(
        SHEET_FARM_FPS,
        f"F{found['row_index']}:I{found['row_index']}",
        [["free", "", "", ""]]
    )

    delete_last_fp_issue_row(fp_link)

    invalidate_stats_cache()
    return True, "Farm FP возвращено в free."

def issue_farm_fps(chat_id, user_id, username, count_needed):
    try:
        logging.info(
            f"issue_farm_fps START user_id={user_id} username={username} "
            f"count_needed={count_needed}"
        )

        logging.info("issue_farm_fps before find_free_farm_fps")
        found = find_free_farm_fps(count_needed)
        logging.info(f"issue_farm_fps after find_free_farm_fps found_count={len(found)}")

        logging.info("issue_farm_fps before get_current_open_farm_fp_warehouse")
        current_warehouse = get_current_open_farm_fp_warehouse()
        logging.info(f"issue_farm_fps current_warehouse={current_warehouse}")

        if not current_warehouse:
            send_farm_fps_menu(chat_id, "Свободных FP сейчас нет.")
            return

        logging.info(
            f"issue_farm_fps before count_free_farm_fp_in_warehouse "
            f"warehouse={current_warehouse}"
        )
        available_in_current = count_free_farm_fp_in_warehouse(current_warehouse)
        logging.info(f"issue_farm_fps available_in_current={available_in_current}")

        if available_in_current < count_needed:
            send_farm_fps_menu(
                chat_id,
                f"Недостаточно свободных FP на текущем складе {current_warehouse}. "
                f"Доступно: {available_in_current}"
            )
            return

        today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
        who_took_text = f"@{username}" if username else "без username"

        issue_rows = []
        messages = []

        with issue_lock:
            logging.info("issue_farm_fps before get_sheet_rows_cached(force=True)")
            current_rows = get_sheet_rows_cached(SHEET_FARM_FPS, force=True)
            logging.info(f"issue_farm_fps current_rows_count={len(current_rows)}")

            for item in found:
                row_index = item["row_index"]

                if row_index - 1 >= len(current_rows):
                    send_farm_fps_menu(chat_id, "Ошибка: одна из FP пропала из таблицы.")
                    return

                row = current_rows[row_index - 1]
                if len(row) < 9:
                    row = row + [''] * (9 - len(row))

                if str(row[5]).strip().lower() != "free":
                    send_farm_fps_menu(chat_id, "Одна из FP уже не свободна.")
                    return

                if str(row[4]).strip() != current_warehouse:
                    send_farm_fps_menu(chat_id, "Одна из FP уже не из текущего склада.")
                    return

            for item in found:
                row_index = item["row_index"]
                row = current_rows[row_index - 1]
                if len(row) < 9:
                    row = row + [''] * (9 - len(row))

                sheet_update_raw(
                    SHEET_FARM_FPS,
                    f"F{row_index}:I{row_index}",
                    [[
                        "taken",
                        "farm",
                        who_took_text,
                        today
                    ]]
                )

                issue_rows.append([
                    row[0],
                    "FP",
                    row[1],
                    normalize_numeric_for_sheet(row[2]),
                    today,
                    row[3],
                    "farm"
                ])

                messages.append(f"Ссылка: {row[0]}")

            refresh_sheet_cache(SHEET_FARM_FPS)

            if issue_rows:
                append_issue_rows_fixed(issue_rows)

            invalidate_stats_cache()

        remaining_in_warehouse = count_free_farm_fp_in_warehouse(current_warehouse)

        if remaining_in_warehouse == 0:
            try:
                notify_admin_farm_fp_warehouse_finished(current_warehouse)
            except Exception:
                logging.exception("notify_admin_farm_fp_warehouse_finished crashed")

        clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"🔢Выдано FP: {len(messages)}\n"
            f"🗃Склад: {current_warehouse}"
        )

        for msg_text in messages:
            tg_send_message(chat_id, msg_text)

        send_farm_fps_menu(chat_id, "Выбери следующее действие:")

    except Exception as e:
        logging.exception("issue_farm_fps crashed")
        notify_admin_about_error(
            "issue_farm_fps",
            str(e),
            extra_text=f"user_id={user_id}, count_needed={count_needed}"
        )
        raise
# =========================
# HELPERS
# =========================

def normalize_multiline_text_block(text):
    return str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def parse_king_data_block(raw_text):
    """
    Разбирает текст формата:
    GEO -
    FB Login:
    FB Password:
    Email:
    Email's Password:
    Service:
    2FA:
    Doc's:
    """
    text = normalize_multiline_text_block(raw_text)

    result = {
        "geo": "",
        "fb_login": "",
        "fb_password": "",
        "email": "",
        "email_password": "",
        "service": "",
        "twofa": "",
        "docs": "",
    }

    if not text:
        return result

    lines = text.split("\n")
    current_key = None

    key_map = {
        "geo": "geo",
        "fb login": "fb_login",
        "fb password": "fb_password",
        "email": "email",
        "email's password": "email_password",
        "emails password": "email_password",
        "email password": "email_password",
        "service": "service",
        "2fa": "twofa",
        "doc's": "docs",
        "docs": "docs",
    }

    for raw_line in lines:
        line = str(raw_line).strip()
        if not line:
            if current_key == "docs":
                result["docs"] += "\n"
            continue

        matched = False

        # Поддержка "GEO - value"
        if re.match(r"^geo\s*-\s*", line, re.IGNORECASE):
            value = re.sub(r"^geo\s*-\s*", "", line, flags=re.IGNORECASE).strip()
            result["geo"] = value
            current_key = "geo"
            matched = True

        if not matched:
            m = re.match(r"^([^:]+):\s*(.*)$", line)
            if m:
                raw_key = m.group(1).strip().lower()
                value = m.group(2).strip()

                mapped = key_map.get(raw_key)
                if mapped:
                    if mapped == "docs":
                        result["docs"] = value
                    else:
                        result[mapped] = value
                    current_key = mapped
                    matched = True

        # Если строка без ключа и мы уже внутри Doc's — считаем продолжением
        if not matched and current_key == "docs":
            if result["docs"]:
                result["docs"] += "\n" + line
            else:
                result["docs"] = line

    return result

def _extract_first_match(pattern, text, flags=re.IGNORECASE):
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else ""


def _extract_all_emails(text):
    return re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")


def _extract_all_urls(text):
    return re.findall(r"https?://[^\s\"']+", text or "")


def _extract_cookie_json_block(text):
    text = str(text or "")
    marker = "Cookie JSON -"
    start_marker = text.find(marker)
    if start_marker == -1:
        return ""

    start_bracket = text.find("[", start_marker)
    if start_bracket == -1:
        return ""

    depth = 0
    end_bracket = -1
    for i in range(start_bracket, len(text)):
        ch = text[i]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end_bracket = i
                break

    if end_bracket == -1:
        return ""

    return text[start_bracket:end_bracket + 1].strip()


def _extract_twofa_value(text):
    text = str(text or "")

    m = re.search(r"2FA\s*-\s*(https?://[^\s]+)", text, re.IGNORECASE)
    if m:
        url = m.group(1).strip()

        key_match = re.search(r"[?&]key=([A-Z2-7]+)", url, re.IGNORECASE)
        if key_match:
            return key_match.group(1).strip()

        tail_match = re.search(r"/([A-Z2-7]{16,})$", url, re.IGNORECASE)
        if tail_match:
            return tail_match.group(1).strip()

        return url

    # fallback: просто длинная 2fa-похожая строка
    m = re.search(r"\b([A-Z2-7]{16,})\b", text)
    if m:
        return m.group(1).strip()

    return ""


def _extract_user_agent(text):
    m = re.search(r"(Mozilla/5\.0[^\n\r]+?Safari/[0-9.]+)", text)
    return m.group(1).strip() if m else ""


def _extract_geo_value(text):
    text = str(text or "").strip().strip('"').strip()
    if not text:
        return ""

    # сначала явные короткие GEO/страны в начале
    first_token = text.split()[0].strip()
    if re.fullmatch(r"[A-Za-zА-Яа-яЁё]{2,20}", first_token):
        if first_token.lower() not in {
            "login", "password", "birthday", "id", "user-agent",
            "token", "email", "cookie", "2fa"
        }:
            return first_token

    # если в начале до "login -" есть кусок
    m = re.match(r"^(.*?)\s+login\s*-\s*", text, re.IGNORECASE)
    if m:
        geo_raw = m.group(1).strip()
        if geo_raw:
            parts = geo_raw.split()
            if parts:
                return parts[0].strip()

    return ""


def _clean_crypto_text(text):
    text = str(text or "")
    text = text.replace("\ufeff", "")
    text = text.replace("\u200b", "")
    text = text.replace("\u200c", "")
    text = text.replace("\u200d", "")
    text = text.replace("\xa0", " ")
    text = text.replace("：", ":")
    text = text.replace("—", "-")
    text = text.replace("–", "-")
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def _normalize_crypto_lines(text):
    text = _clean_crypto_text(text)
    lines = [line.strip() for line in text.splitlines()]
    return [line for line in lines if line]


def _extract_all_emails(text):
    text = str(text or "")
    return re.findall(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text)


def _extract_all_urls(text):
    text = str(text or "")
    return re.findall(r"https?://[^\s]+", text)


def _dedupe_keep_order(items):
    result = []
    seen = set()

    for item in items:
        val = str(item or "").strip()
        if not val:
            continue
        key = val.lower()
        if key not in seen:
            seen.add(key)
            result.append(val)

    return result


def _extract_labeled_value(text, labels, allow_spaces=True):
    text = str(text or "")
    value_pattern = r"([^\n\r]+)" if allow_spaces else r"([^\s]+)"

    for label in labels:
        pattern = rf"(?:^|\n)\s*{label}\s*[:\-]\s*{value_pattern}"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()

    return ""


def _extract_geo_value(text):
    text = str(text or "").strip().strip('"').strip()
    if not text:
        return ""

    # 1) явное GEO:
    m = re.search(r"(?:^|\n)\s*GEO\s*[:\-]\s*([^\n\r]+)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # 2) Country:
    m = re.search(r"(?:^|\n)\s*Country\s*[:\-]\s*([^\n\r]+)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # 3) первое слово, если это реально похоже на geo
    first_token = text.split()[0].strip() if text.split() else ""
    if first_token and re.fullmatch(r"[A-Za-zА-Яа-яЁё]{2,30}", first_token):
        if first_token.lower() not in {
            "login", "fb", "facebook", "password", "email", "service",
            "cookie", "cookies", "2fa", "user-agent", "doc", "docs"
        }:
            return first_token

    return ""


def _extract_login_value(text):
    text = str(text or "")

    # 1) FB Login / Facebook Login / Login
    labels = [
        r"FB\s*Login",
        r"Facebook\s*Login",
        r"Login",
    ]

    for label in labels:
        m = re.search(
            rf"(?:^|\n)\s*{label}\s*[:\-]\s*([^\n\r]+)",
            text,
            re.IGNORECASE
        )
        if m:
            value = m.group(1).strip()

            # если пришло login:password, берём только login
            if ":" in value and not _validate_email(value):
                value = value.split(":", 1)[0].strip()

            value = value.split(";")[0].strip()

            if value:
                return value

    # 2) новый fallback: строка вида login:password
    lines = _normalize_crypto_lines(text)

    for line in lines:
        line = str(line).strip()
        if not line:
            continue

        # пропускаем email:password
        if re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\s*:", line):
            continue

        m = re.match(r"^\s*([A-Za-z0-9._\-]{5,}|[0-9]{8,20})\s*:\s*([^\s:]+)\s*$", line)
        if m:
            login_part = m.group(1).strip()
            if not _validate_email(login_part):
                return login_part

    # 3) profile.php?id=
    m = re.search(r"profile\.php\?id=\s*([0-9]{8,20})", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # 4) длинный numeric id как fallback
    m = re.search(r"\b([0-9]{8,20})\b", text)
    if m:
        return m.group(1).strip()

    return ""


def _extract_fb_password_value(text):
    text = str(text or "")

    # 1) старые форматы с подписью
    labels = [
        r"FB\s*Password",
        r"Facebook\s*Password",
        r"Password",
    ]

    for label in labels:
        m = re.search(
            rf"(?:^|\n)\s*{label}\s*[:\-]\s*([^\n\r]+)",
            text,
            re.IGNORECASE
        )
        if m:
            value = m.group(1).strip().split()[0].strip()
            if value:
                return value

    # 2) новый fallback: строка вида login:password
    lines = _normalize_crypto_lines(text)

    for line in lines:
        line = str(line).strip()
        if not line:
            continue

        # пропускаем email:password
        if re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\s*:", line):
            continue

        m = re.match(r"^\s*([A-Za-z0-9._\-]{5,}|[0-9]{8,20})\s*:\s*([^\s:]+)\s*$", line)
        if m:
            login_part = m.group(1).strip()
            password_part = m.group(2).strip()

            if not _validate_email(login_part):
                return password_part

    return ""


def _extract_email_from_email_block(text):
    text = str(text or "")

    labels = [
        r"Email",
        r"Mail",
    ]

    for label in labels:
        m = re.search(
            rf"(?:^|\n)\s*{label}\s*[:\-]\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{{2,}})",
            text,
            re.IGNORECASE
        )
        if m:
            return m.group(1).strip()

    return ""


def _extract_email_password_from_email_block(text):
    text = str(text or "")

    labels = [
        r"Email'?s\s*Password",
        r"Email\s*Password",
        r"Mail\s*Password",
    ]

    for label in labels:
        m = re.search(
            rf"(?:^|\n)\s*{label}\s*[:\-]\s*([^\n\r]+)",
            text,
            re.IGNORECASE
        )
        if m:
            value = m.group(1).strip().split()[0].strip()
            if value:
                return value

    return ""


def _extract_service_value(text):
    text = str(text or "")

    labels = [
        r"Service",
        r"Reserve\s*Mail",
        r"Backup\s*Mail",
    ]

    for label in labels:
        m = re.search(
            rf"(?:^|\n)\s*{label}\s*[:\-]\s*([^\n\r]+)",
            text,
            re.IGNORECASE
        )
        if m:
            value = m.group(1).strip()
            if value:
                return value

    return ""


def _extract_twofa_value(text):
    text = str(text or "")

    # 1) 2FA: ABCD EFGH ...
    m = re.search(r"(?:^|\n)\s*2FA\s*[:\-]\s*([A-Z2-7\s]{16,})", text, re.IGNORECASE)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()

    # 2) 2FA: ссылка
    m = re.search(r"(?:^|\n)\s*2FA\s*[:\-]\s*(https?://[^\s]+)", text, re.IGNORECASE)
    if m:
        url = m.group(1).strip()

        key_match = re.search(r"[?&]key=([A-Z2-7]+)", url, re.IGNORECASE)
        if key_match:
            return key_match.group(1).strip()

        tail_match = re.search(r"/([A-Z2-7]{16,})$", url, re.IGNORECASE)
        if tail_match:
            return tail_match.group(1).strip()

        return url

    # 3) fallback: просто длинная base32-строка
    m = re.search(r"\b([A-Z2-7]{16,})\b", text)
    if m:
        return m.group(1).strip()

    return ""


def _extract_user_agent(text):
    text = str(text or "")

    # сначала явный label
    m = re.search(r"(?:^|\n)\s*User-Agent\s*[:\-]\s*(Mozilla/5\.0[^\n\r]+)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # потом fallback по самому UA
    m = re.search(r"(Mozilla/5\.0[^\n\r]+?Safari/[0-9.]+)", text)
    return m.group(1).strip() if m else ""


def _extract_cookie_json_block(text):
    text = str(text or "")

    # 1) блок JSON массива cookies
    m = re.search(r"(\[\s*\{.*?\}\s*\])", text, re.DOTALL)
    if m:
        return m.group(1).strip()

    # 2) блок JSON объекта cookies/storage
    m = re.search(r"(\{\s*\".*?cookies.*?\})", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return ""


def _extract_docs_links(text):
    text = str(text or "")
    docs = []

    m = re.search(
        r"IZOBRAZHENIYA S PROHOZHDENIEM ZRD/CHEK\s*(https?://[^\s]+)",
        text,
        re.IGNORECASE
    )
    if m:
        docs.append(m.group(1).strip())

    for mm in re.finditer(r"\bAVA\s+(https?://[^\s]+)", text, re.IGNORECASE):
        docs.append(mm.group(1).strip())

    for mm in re.finditer(r"\bDOC\S*\s*[:=\-]?\s*(https?://[^\s]+)", text, re.IGNORECASE):
        docs.append(mm.group(1).strip())

    for mm in re.finditer(r"\bzrd\s*=\s*(https?://[^\s]+)", text, re.IGNORECASE):
        docs.append(mm.group(1).strip())

    for mm in re.finditer(r"https?://drive\.google\.com/[^\s]+", text, re.IGNORECASE):
        docs.append(mm.group(0).strip())

    return _dedupe_keep_order(docs)


def _extract_bm_links(text):
    urls = _extract_all_urls(text)
    result = []

    for url in urls:
        if "business.facebook.com/invitation/" in url or "business.facebook.com/settings/" in url:
            result.append(url)

    return _dedupe_keep_order(result)


def _extract_bm_email_pairs(text):
    text = str(text or "")
    pairs = []

    matches = re.findall(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\s*:\s*[^\s]+)",
        text
    )

    for pair in matches:
        clean_pair = re.sub(r"\s+", "", pair).strip()
        pairs.append(clean_pair)

    return _dedupe_keep_order(pairs)


def _extract_cookie_links(text):
    urls = _extract_all_urls(text)
    result = []

    for url in urls:
        if "drive.google.com" in url:
            result.append(url)

    return _dedupe_keep_order(result)


def _validate_email(value):
    value = str(value or "").strip()
    if re.fullmatch(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", value):
        return value
    return ""


def _validate_2fa(value):
    value = str(value or "").strip()
    if not value:
        return ""

    # ссылка тоже допустима
    if value.lower().startswith("http://") or value.lower().startswith("https://"):
        return value

    compact = re.sub(r"\s+", "", value).upper()
    if re.fullmatch(r"[A-Z2-7]{16,}", compact):
        return re.sub(r"\s+", " ", value).strip()

    return ""


def _sanitize_login(value):
    value = str(value or "").strip()
    if not value:
        return ""

    # если это email — тоже ок, не режем
    if _validate_email(value):
        return value

    # если это profile.php?id=...
    m = re.search(r"profile\.php\?id=\s*([0-9]{8,20})", value, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # обычный id / login
    return value.split()[0].strip()


def parse_crypto_king_raw_data(raw_text):
    text_original = str(raw_text or "")
    text = _clean_crypto_text(raw_text)

    result = {
        "geo": "",
        "fb_login": "",
        "fb_password": "",
        "email": "",
        "email_password": "",
        "service": "",
        "twofa": "",
        "cookies_json": "",
        "cookies_links": [],
        "user_agent": "",
        "docs_links": [],
        "bm_links": [],
        "bm_email_pairs": [],
        "extra_pairs": [],
        "raw_text": text_original,
    }

    # ---------- FORMAT 1: pipe ----------
    if "|" in text and text.count("|") >= 5 and "login -" not in text.lower():
        parts = [x.strip() for x in text.split("|")]
        if len(parts) >= 6:
            result["fb_login"] = _sanitize_login(parts[0])
            result["fb_password"] = parts[1].strip()
            result["twofa"] = _validate_2fa(parts[2])
            result["email"] = _validate_email(parts[3])
            result["email_password"] = parts[4].strip()
            result["service"] = parts[5].strip()

            result["cookies_json"] = _extract_cookie_json_block(text_original)
            result["docs_links"] = _extract_docs_links(text_original)
            result["bm_links"] = _extract_bm_links(text_original)
            result["cookies_links"] = _extract_cookie_links(text_original)
            result["bm_email_pairs"] = _extract_bm_email_pairs(text_original)
            result["user_agent"] = _extract_user_agent(text_original)

            return result

    # ---------- COMMON EXTRACTION ----------
    result["geo"] = _extract_geo_value(text)
    result["fb_login"] = _sanitize_login(_extract_login_value(text))
    result["fb_password"] = _extract_fb_password_value(text)
    result["email"] = _validate_email(_extract_email_from_email_block(text))
    result["email_password"] = _extract_email_password_from_email_block(text)
    result["service"] = _extract_service_value(text)
    result["twofa"] = _validate_2fa(_extract_twofa_value(text))
    result["user_agent"] = _extract_user_agent(text_original)
    result["cookies_json"] = _extract_cookie_json_block(text_original)
    result["docs_links"] = _extract_docs_links(text_original)
    result["bm_links"] = _extract_bm_links(text_original)
    result["cookies_links"] = _extract_cookie_links(text_original)
    result["bm_email_pairs"] = _extract_bm_email_pairs(text_original)

    # ---------- EMAILS ----------
    all_emails = _dedupe_keep_order(_extract_all_emails(text_original))

    pair_matches = re.findall(
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})\s*[:;]\s*([^\s;]+)",
        text_original
    )
    pair_list = []
    for em, pw in pair_matches:
        pair_list.append(f"{em}:{pw}")
    result["extra_pairs"] = _dedupe_keep_order(pair_list)

    # если явно email не найден, пробуем из списка email'ов
    if not result["email"] and all_emails:
        # если fb_login уже email, то email берем следующий
        if result["fb_login"] and _validate_email(result["fb_login"]):
            for em in all_emails:
                if em.lower() != result["fb_login"].lower():
                    result["email"] = em
                    break
        else:
            result["email"] = all_emails[0]

    # если логин пустой — пробуем первым email
    if not result["fb_login"] and all_emails:
        result["fb_login"] = all_emails[0]

    # если email_password пустой — ищем email:password пары
    if not result["email_password"] and result["email"]:
        for pair in result["extra_pairs"]:
            if ":" not in pair:
                continue
            em, pw = pair.split(":", 1)
            if em.lower() == result["email"].lower():
                result["email_password"] = pw.strip()
                break

    # если service пустой — пробуем второй email
    if not result["service"] and len(all_emails) >= 2:
        for em in all_emails:
            if result["email"] and em.lower() == result["email"].lower():
                continue
            if result["fb_login"] and _validate_email(result["fb_login"]) and em.lower() == result["fb_login"].lower():
                continue
            result["service"] = em
            break

    # ---------- FALLBACK PASSWORD LOGIC ----------
    if not result["fb_password"]:
        lines = _normalize_crypto_lines(text_original)
        for idx, line in enumerate(lines):
            if re.search(r"FB\s*Login|Facebook\s*Login|^Login\s*[:\-]", line, re.IGNORECASE):
                if idx + 1 < len(lines):
                    nxt = lines[idx + 1]
                    if not re.search(r"Email|2FA|Service|Cookie|User-Agent|Doc", nxt, re.IGNORECASE):
                        candidate = nxt.split()[0].strip()
                        if candidate and len(candidate) >= 4:
                            result["fb_password"] = candidate
                            break

    # ---------- FINAL SANITY ----------
    result["fb_login"] = _sanitize_login(result["fb_login"])
    result["email"] = _validate_email(result["email"])
    result["twofa"] = _validate_2fa(result["twofa"])

    # если email случайно попал в fb_password — чистим
    if _validate_email(result["fb_password"]):
        result["fb_password"] = ""

    # если email_password случайно выглядит как email — чистим
    if _validate_email(result["email_password"]):
        result["email_password"] = ""

    return result


def build_crypto_king_octo_description(parsed):
    lines = [
        f"GEO - {parsed.get('geo', '')}",
        "",
        f"FB Login: {parsed.get('fb_login', '')}",
        f"FB Password: {parsed.get('fb_password', '')}",
        "",
        f"Email: {parsed.get('email', '')}",
        f"Email's Password: {parsed.get('email_password', '')}",
        f"Service: {parsed.get('service', '')}",
        f"2FA: {parsed.get('twofa', '')}",
        "",
        "Doc's:",
    ]

    docs_links = parsed.get("docs_links", [])
    if docs_links:
        lines.append(docs_links[0])

    text = "\n".join(lines).strip()

    if len(text) > 1024:
        raise RuntimeError(f"Octo description too long: {len(text)} chars")

    return text

def build_octo_description_from_king_data(parsed):
    parsed = parsed or {}

    geo = str(parsed.get("geo", "") or "").strip()
    fb_login = str(parsed.get("fb_login", "") or "").strip()
    fb_password = str(parsed.get("fb_password", "") or "").strip()
    email = str(parsed.get("email", "") or "").strip()
    email_password = str(parsed.get("email_password", "") or "").strip()
    service = str(parsed.get("service", "") or "").strip()
    twofa = str(parsed.get("twofa", "") or "").strip()
    docs = str(parsed.get("docs", "") or "").strip()

    return (
        f"GEO - {geo}\n\n"
        f"FB Login: {fb_login}\n"
        f"FB Password: {fb_password}\n\n"
        f"Email: {email}\n"
        f"Email's Password: {email_password}\n"
        f"Service: {service}\n"
        f"2FA: {twofa}\n\n"
        f"Doc's: {docs}"
    )

def octo_get_profile_by_uuid(profile_uuid):
    profile_uuid = str(profile_uuid or "").strip()
    if not profile_uuid:
        raise RuntimeError("profile_uuid пустой")

    headers = {
        "X-Octo-Api-Token": OCTO_API_TOKEN,
        "Content-Type": "application/json",
    }

    url = f"{OCTO_API_BASE}/profiles/{profile_uuid}"
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    return resp.json()

def octo_update_profile_description(profile_uuid, description_text):
    if not profile_uuid:
        raise RuntimeError("Не передан profile_uuid для Octo")

    payload = {
        "description": str(description_text or "")
    }

    resp = requests.patch(
        f"{OCTO_API_BASE}/profiles/{profile_uuid}",
        headers=octo_headers(),
        json=payload,
        timeout=60
    )

    if resp.status_code not in [200, 201]:
        raise RuntimeError(f"Octo update description failed: {resp.status_code} {resp.text}")

    data = resp.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"Octo update description error: {data}")

    return True
    
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

def normalize_person_name(value):
    text = str(value or "").strip()
    text = re.sub(r'^(?:№|N|No|N°)\s*\d+\s*', '', text, flags=re.IGNORECASE).strip()
    return text

def parse_price(value):
    s = str(value).strip().replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def parse_limit_number(value):
    if value is None:
        return None

    text = str(value).strip().lower()
    if not text:
        return None

    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()

    if text in ["no limit", "unlim", "unlimited", "no limits"]:
        return "unlim"

    compact = text.replace(" ", "")

    if "," in compact:
        integer_part = compact.split(",")[0]
    elif "." in compact:
        integer_part = compact.split(".")[0]
    else:
        integer_part = compact

    try:
        return int(integer_part)
    except Exception:
        return None

def limit_matches_filter(limit_value, selected_filter):
    parsed = parse_limit_number(limit_value)

    if parsed is None:
        return False

    if selected_filter == FREE_LIMIT_250:
        return parsed != "unlim" and 0 <= parsed <= 300

    if selected_filter == FREE_LIMIT_500:
        return parsed != "unlim" and 301 <= parsed <= 800

    if selected_filter == FREE_LIMIT_1200:
        return parsed != "unlim" and 801 <= parsed <= 1300

    if selected_filter == FREE_LIMIT_1500:
        return parsed != "unlim" and 1301 <= parsed <= 1600

    if selected_filter == FREE_LIMIT_UNLIM:
        if parsed == "unlim":
            return True
        return parsed != "unlim" and parsed >= 1601

    return False

def normalize_numeric_for_sheet(value):
    num = parse_price(value)
    if num is None:
        return value

    if float(num).is_integer():
        return int(num)

    return float(num)

def ensure_row_len(row, size):
    row = list(row or [])
    if len(row) < size:
        row += [''] * (size - len(row))
    return row

def make_sync_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex}"

def parse_sheet_date(value):
    value = str(value).strip()
    for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    return None


def get_manager_stats_period():
    now = datetime.now(MOSCOW_TZ)

    # с 1 по 9 число показываем ПРОШЛЫЙ месяц
    if now.day < 10:
        if now.month == 1:
            start_date = datetime(now.year - 1, 12, 1)
            end_date = datetime(now.year, 1, 1)
        else:
            start_date = datetime(now.year, now.month - 1, 1)
            end_date = datetime(now.year, now.month, 1)

    # с 10 числа показываем ТЕКУЩИЙ месяц
    else:
        start_date = datetime(now.year, now.month, 1)

        if now.month == 12:
            end_date = datetime(now.year + 1, 1, 1)
        else:
            end_date = datetime(now.year, now.month + 1, 1)

    return start_date, end_date

def build_manager_stats_summary_text(username):
    if not username:
        return "Не указан username."

    username = username.strip().lstrip("@").lower()
    target_username = f"@{username}"

    start_date, end_date = get_manager_stats_period()

    accounts_count = 0
    kings_count = 0
    bms_count = 0
    fps_count = 0
    pixels_count = 0

    accounts_rows = get_sheet_rows_cached(SHEET_ACCOUNTS)
    for row in accounts_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))
        if str(row[11]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[10]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            accounts_count += 1

    kings_rows = get_sheet_rows_cached(SHEET_KINGS)
    for row in kings_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))
        if str(row[8]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[6]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            kings_count += 1

    crypto_kings_rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)
    for row in crypto_kings_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))
        if str(row[8]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[6]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            kings_count += 1

    bms_rows = get_sheet_rows_cached(SHEET_BMS)
    for row in bms_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))
        if str(row[6]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[7]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            bms_count += 1

    fps_rows = get_sheet_rows_cached(SHEET_FPS)
    for row in fps_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))
        if str(row[7]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[8]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            fps_count += 1

    pixels_rows = get_sheet_rows_cached(SHEET_PIXELS)
    for row in pixels_rows[1:]:
        if len(row) < 8:
            row = row + [''] * (8 - len(row))
        if str(row[6]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[5]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            pixels_count += 1

    return (
        f"Статистика accounts {target_username}\n"
        f"Период: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}\n\n"
        f"Кинги: {kings_count}\n"
        f"Лички: {accounts_count}\n"
        f"БМы: {bms_count}\n"
        f"ФП: {fps_count}\n"
        f"Пиксели: {pixels_count}"
    )

def build_manager_stats_text(username):
    if not username:
        return "У пользователя не указан username."

    username = str(username).strip().lstrip("@").lower()
    target_username = f"@{username}"

    start_date, end_date = get_manager_stats_period()

    accounts_rows = get_sheet_rows_cached(SHEET_ACCOUNTS)
    kings_rows = get_sheet_rows_cached(SHEET_KINGS)
    bms_rows = get_sheet_rows_cached(SHEET_BMS)
    fps_rows = get_sheet_rows_cached(SHEET_FPS)
    pixels_rows = get_sheet_rows_cached(SHEET_PIXELS)

    accounts_lines = []
    for row in accounts_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        who_took = str(row[11]).strip().lower()
        transfer_date = parse_sheet_date(row[10])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            accounts_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[9]}"
            )

    kings_lines = []
    for row in kings_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        who_took = str(row[8]).strip().lower()
        transfer_date = parse_sheet_date(row[6])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            kings_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[5]}"
            )

    crypto_kings_rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)
    for row in crypto_kings_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        who_took = str(row[8]).strip().lower()
        transfer_date = parse_sheet_date(row[6])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            kings_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[5]}"
            )

    bms_lines = []
    for row in bms_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        who_took = str(row[6]).strip().lower()
        transfer_date = parse_sheet_date(row[7])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            bms_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[5]}"
            )

    fps_lines = []
    for row in fps_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        who_took = str(row[7]).strip().lower()
        transfer_date = parse_sheet_date(row[8])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            fps_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[6]}"
            )

    pixels_lines = []
    for row in pixels_rows[1:]:
        if len(row) < 8:
            row = row + [''] * (8 - len(row))

        who_took = str(row[6]).strip().lower()
        transfer_date = parse_sheet_date(row[5])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            pixel_name = extract_pixel_name_from_data(row[7]) if len(row) > 7 else ""
            pixels_lines.append(
                f"{pixel_name or 'Pixel'} | {transfer_date.strftime('%d/%m/%Y')} | {row[4]}"
            )

    text_parts = [
        f"Статистика accounts {target_username}",
        f"Период: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}",
        ""
    ]

    text_parts.append(f"Кинги: {len(kings_lines)}")
    text_parts.extend(kings_lines if kings_lines else ["нет выдач"])
    text_parts.append("")

    text_parts.append(f"Лички: {len(accounts_lines)}")
    text_parts.extend(accounts_lines if accounts_lines else ["нет выдач"])
    text_parts.append("")

    text_parts.append(f"БМы: {len(bms_lines)}")
    text_parts.extend(bms_lines if bms_lines else ["нет выдач"])
    text_parts.append("")

    text_parts.append(f"ФП: {len(fps_lines)}")
    text_parts.extend(fps_lines if fps_lines else ["нет выдач"])

    text_parts.append("")
    text_parts.append(f"Пиксели: {len(pixels_lines)}")
    text_parts.extend(pixels_lines if pixels_lines else ["нет выдач"])

    return "\n".join(text_parts)

def build_farmer_stats_summary_text(username):
    if not username:
        return "Не указан username."

    username = username.strip().lstrip("@").lower()
    target_username = f"@{username}"

    start_date, end_date = get_manager_stats_period()

    farm_kings_count = 0
    farm_bms_count = 0
    farm_fps_count = 0

    farm_kings_rows = get_sheet_rows_cached(SHEET_FARM_KINGS)
    for row in farm_kings_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))
        if str(row[8]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[6]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            farm_kings_count += 1

    farm_bms_rows = get_sheet_rows_cached(SHEET_FARM_BMS)
    for row in farm_bms_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))
        if str(row[6]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[7]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            farm_bms_count += 1

    farm_fps_rows = get_sheet_rows_cached(SHEET_FARM_FPS)
    for row in farm_fps_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))
        if str(row[7]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[8]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            farm_fps_count += 1

    return (
        f"Статистика farmer {target_username}\n"
        f"Период: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}\n\n"
        f"Farm kings: {farm_kings_count}\n"
        f"Farm BM: {farm_bms_count}\n"
        f"Farm FP: {farm_fps_count}"
    )

def build_farmer_stats_text(username):
    if not username:
        return "У пользователя не указан username."

    username = str(username).strip().lstrip("@").lower()
    target_username = f"@{username}"

    start_date, end_date = get_manager_stats_period()

    farm_kings_rows = get_sheet_rows_cached(SHEET_FARM_KINGS)
    farm_bms_rows = get_sheet_rows_cached(SHEET_FARM_BMS)
    farm_fps_rows = get_sheet_rows_cached(SHEET_FARM_FPS)

    farm_kings_lines = []
    for row in farm_kings_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        who_took = str(row[8]).strip().lower()
        transfer_date = parse_sheet_date(row[6])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            farm_kings_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[5]}"
            )

    farm_bms_lines = []
    for row in farm_bms_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        who_took = str(row[6]).strip().lower()
        transfer_date = parse_sheet_date(row[7])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            farm_bms_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[5]}"
            )

    farm_fps_lines = []
    for row in farm_fps_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        who_took = str(row[7]).strip().lower()
        transfer_date = parse_sheet_date(row[8])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            farm_fps_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[6]}"
            )

    text_parts = [
        f"Статистика farmers {target_username}",
        f"Период: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}",
        ""
    ]

    text_parts.append(f"Farm kings: {len(farm_kings_lines)}")
    text_parts.extend(farm_kings_lines if farm_kings_lines else ["нет выдач"])
    text_parts.append("")

    text_parts.append(f"Farm BM: {len(farm_bms_lines)}")
    text_parts.extend(farm_bms_lines if farm_bms_lines else ["нет выдач"])
    text_parts.append("")

    text_parts.append(f"Farm FP: {len(farm_fps_lines)}")
    text_parts.extend(farm_fps_lines if farm_fps_lines else ["нет выдач"])

    return "\n".join(text_parts)
    
def get_state(user_id):
    with state_lock:
        state = user_states.get(str(user_id), {})

    if not state:
        return {}

    now = time.time()

    custom_expires_at = state.get("_custom_expires_at")
    if custom_expires_at:
        if now > float(custom_expires_at):
            clear_state(user_id)
            return {}
        return dict(state)

    state_time = state.get("_time", 0)
    if state_time and now - state_time > STATE_TTL:
        clear_state(user_id)
        return {}

    return dict(state)

def set_state_with_custom_ttl(user_id, state, ttl_seconds):
    state = dict(state or {})
    state["_time"] = time.time()
    state["_custom_expires_at"] = time.time() + int(ttl_seconds)

    with state_lock:
        user_states[str(user_id)] = state
        user_states[user_id] = state

def cleanup_states():
    now = time.time()

    with state_lock:
        to_delete = []

        for user_id, state in user_states.items():
            if not state:
                to_delete.append(user_id)
                continue

            custom_expires_at = state.get("_custom_expires_at")
            if custom_expires_at:
                try:
                    if now > float(custom_expires_at):
                        to_delete.append(user_id)
                    continue
                except Exception:
                    to_delete.append(user_id)
                    continue

            updated_at = state.get("updated_at", 0)
            if updated_at and now - updated_at > STATE_TTL:
                to_delete.append(user_id)

        for user_id in to_delete:
            user_states.pop(user_id, None)
            user_state_history.pop(user_id, None)

def cleanup_processed_updates():
    now = time.time()
    to_delete = []

    with processed_updates_lock:
        for update_id, ts in processed_updates.items():
            if now - ts > PROCESSED_UPDATES_TTL:
                to_delete.append(update_id)

        for update_id in to_delete:
            processed_updates.pop(update_id, None)

def is_duplicate_update(update_id):
    if update_id is None:
        return False

    now = time.time()

    with processed_updates_lock:
        ts = processed_updates.get(update_id)
        if ts and now - ts <= PROCESSED_UPDATES_TTL:
            return True

        processed_updates[update_id] = now
        return False

def set_state(user_id, data):
    data = dict(data)
    data["_time"] = time.time()

    with state_lock:
        current = user_states.get(str(user_id))
        if current:
            history = user_state_history.get(str(user_id), [])
            history.append(dict(current))
            user_state_history[str(user_id)] = history[-30:]

        user_states[str(user_id)] = data

def push_state_to_history(user_id, state):
    with state_lock:
        history = user_state_history.get(str(user_id), [])
        history.append(dict(state))
        user_state_history[str(user_id)] = history[-30:]  # храним последние 30 шагов

def go_back_state(user_id):
    with state_lock:
        history = user_state_history.get(str(user_id), [])
        if not history:
            return None

        prev_state = history.pop()
        user_state_history[str(user_id)] = history
        user_states[str(user_id)] = dict(prev_state)
        user_states[str(user_id)]["_time"] = time.time()
        return dict(prev_state)

def update_state(user_id, **kwargs):
    state = get_state(user_id)
    state.update(kwargs)
    set_state(user_id, state)

def clear_state(user_id):
    with state_lock:
        user_states.pop(str(user_id), None)
        user_state_history.pop(str(user_id), None)

def set_last_accounts_section(user_id, section_name):
    state = get_state(user_id)
    state["last_accounts_section"] = section_name
    set_state(user_id, state)

def set_last_farmers_section(user_id, section_name):
    state = get_state(user_id)
    state["last_farmers_section"] = section_name
    set_state(user_id, state)

def get_last_farmers_section(user_id):
    state = get_state(user_id)
    return state.get("last_farmers_section", "")

def get_last_accounts_section(user_id):
    state = get_state(user_id)
    return state.get("last_accounts_section", "")

def find_account_in_base(account_number):
    rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 14:
            row = row + [''] * (14 - len(row))

        if str(row[0]).strip() == str(account_number).strip():
            return {
                "row_index": idx,
                "row": row
            }
    return None

def update_account_from_fastadscheck(account_number, limit_value, threshold_value, gmt_value, currency_value, account_url=""):
    with accounts_lock:
        found = find_account_in_base(account_number)

        if not found:
            return False, f"Личка {account_number} не найдена в таблице."

        row_index = found["row_index"]

        row = found["row"]
        if len(row) < 14:
            row = row + [''] * (14 - len(row))

        parsed_limit = parse_limit_number(limit_value)
        if parsed_limit == "unlim":
            limit_to_store = "unlim"
        else:
            limit_to_store = normalize_numeric_for_sheet(limit_value)

        values_en = [[
            limit_to_store,                                # E
            normalize_numeric_for_sheet(threshold_value),  # F
            str(gmt_value),                                # G
            row[7] if len(row) > 7 else "",                # H
            row[8] if len(row) > 8 else "",                # I
            row[9] if len(row) > 9 else "",                # J
            row[10] if len(row) > 10 else "",              # K
            row[11] if len(row) > 11 else "",              # L
            currency_value or "",                          # M
            account_url or ""                              # N
        ]]

        sheet_update_raw(
            SHEET_ACCOUNTS,
            f"E{row_index}:N{row_index}",
            values_en
        )

        refresh_sheet_cache(SHEET_ACCOUNTS)

        return True, f"Личка {account_number} обновлена"

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

def mark_issue_row_as_ban(issue_row_index, comment_text=""):
    if not issue_row_index:
        return

    sheet_update_and_refresh(
        SHEET_ISSUES,
        f"G{issue_row_index}:I{issue_row_index}",
        [["ban", "", str(comment_text or "").strip()]]
    )

def set_issue_comment(issue_row_index, comment_text):
    if not issue_row_index:
        return

    sheet_update_and_refresh(
        SHEET_ISSUES,
        f"I{issue_row_index}",
        [[str(comment_text or "").strip()]]
    )

def is_banned_account(base_row, issue_row=None):
    base_target = ""
    if base_row and len(base_row) >= 10:
        base_target = str(base_row[9]).strip().lower()

    issue_target = ""
    if issue_row and len(issue_row) >= 7:
        issue_target = str(issue_row[6]).strip().lower()

    return base_target == "ban" or issue_target == "ban"

def return_account_to_ban(account_number, comment_text=""):
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
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, "Личка переведена в ban."

def return_account_to_free(account_number):
    base_info = find_account_in_base(account_number)

    if not base_info:
        return False, "Личка не найдена в базе."

    row = base_info["row"]

    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    status = str(row[8]).strip().lower()

    if status == "free":
        return False, "Эта личка уже free."

    sheet_update_and_refresh(
        SHEET_ACCOUNTS,
        f"I{base_info['row_index']}:L{base_info['row_index']}",
        [["free", "", "", ""]]
    )

    delete_last_issue_row(account_number)

    invalidate_stats_cache()
    return True, "Личка возвращена в free."

def delete_last_issue_row(account_number):
    issue_info = find_last_issue_row(account_number)
    if not issue_info:
        return False

    sheet_delete_row_and_refresh(SHEET_ISSUES, issue_info["row_index"])
    return True

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
def send_free_accounts(chat_id, selected_filter):
    rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

    if len(rows) < 2:
        tg_send_message(chat_id, "В базе пока нет личек.")
        return

    free_rows = []
    for row in rows[1:]:
        if len(row) < 13:
            row = row + [''] * (13 - len(row))

        status = str(row[8]).strip().lower()
        if status != "free":
            continue

        limit_val = str(row[4]).strip()
        if not limit_matches_filter(limit_val, selected_filter):
            continue

        free_rows.append(row)

    if not free_rows:
        tg_send_message(chat_id, "Свободных личек с таким лимитом сейчас нет.")
        return

    lines = []
    for i, row in enumerate(free_rows, start=1):
        acc = str(row[0]).strip()
        limit_val = str(row[4]).strip()
        threshold = str(row[5]).strip()
        gmt = str(row[6]).strip()
        warehouses = str(row[7]).strip()
        currency = str(row[12]).strip() if len(row) > 12 else ""

        lines.append(
            f"{i}. {acc}\n"
            f"Л {limit_val} | Т {threshold} | GMT {gmt} | {currency} | {warehouses}\n"
        )

    header = f"Свободные лички ({selected_filter}): {len(free_rows)}\n\n"
    current_text = header

    for line in lines:
        if len(current_text) + len(line) + 1 > 3500:
            tg_send_message(chat_id, current_text.strip())
            current_text = line + "\n"
        else:
            current_text += line + "\n"

    if current_text.strip():
        tg_send_message(chat_id, current_text.strip())

# =========================
# ADD ACCOUNTS
# =========================
def send_bulk_add_instructions(chat_id):
    text = (
        "Отправь лички сообщением, каждая с новой строки.\n\n"
        "Формат:\n"
        "номер; дата покупки; цена; поставщик; склады\n\n"
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

    wanted_limit = parse_limit_number(limit_val)
    wanted_threshold = str(threshold_val).strip()
    wanted_gmt = str(gmt_val).strip()
    wanted_currency = str(currency).strip()

    candidates = []
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) <= ACCOUNT_CURRENCY_COL:
            continue

        status = str(row[8]).strip().lower()
        row_currency = str(row[ACCOUNT_CURRENCY_COL]).strip()

        if status != "free":
            continue
        if parse_limit_number(row[4]) != wanted_limit:
            continue
        if str(row[5]).strip() != wanted_threshold:
            continue
        if str(row[6]).strip() != wanted_gmt:
            continue
        if row_currency != wanted_currency:
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
        f"Кому передали: {state.get('for_whom', 'не указано')}"
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
            normalize_numeric_for_sheet(price),
            transfer_date,
            supplier,
            for_whom
        ],
        value_input_option="USER_ENTERED"
    )

def issue_accounts_bulk(account_numbers, for_whom, username):
    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
    who_took_text = f"@{username}" if username else "без username"

    unique_numbers = []
    seen = set()
    for x in account_numbers:
        acc = str(x).strip()
        if acc and acc not in seen:
            seen.add(acc)
            unique_numbers.append(acc)

    issued = []
    not_found = []
    not_available = []

    with issue_lock, accounts_lock:
        rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

        indexed = {}
        for idx, row in enumerate(rows[1:], start=2):
            if len(row) < 14:
                row = row + [''] * (14 - len(row))
                rows[idx - 1] = row

            acc = str(row[0]).strip()
            if acc:
                indexed[acc] = (idx, row)

        issue_rows = []

        for account_number in unique_numbers:
            item = indexed.get(account_number)
            if not item:
                not_found.append(account_number)
                continue

            row_index, row = item
            status = str(row[8]).strip().lower()

            if status != "free":
                not_available.append(account_number)
                continue

            sheet_update_raw(
                SHEET_ACCOUNTS,
                f"I{row_index}:L{row_index}",
                [["taken", for_whom, today, who_took_text]]
            )

            row[8] = "taken"
            row[9] = for_whom
            row[10] = today
            row[11] = who_took_text

            issue_rows.append([
                account_number,
                "РК",
                row[1],
                normalize_numeric_for_sheet(row[2]),
                today,
                row[3],
                for_whom
            ])

            issued.append({
                "account_number": account_number,
                "warehouses": row[7],
                "purchase_date": row[1],
                "price": row[2],
                "supplier": row[3],
                "currency": row[12] if len(row) > 12 else "",
                "account_url": row[13] if len(row) > 13 else ""
            })

        with table_cache_lock:
            table_cache[SHEET_ACCOUNTS]["rows"] = rows
            table_cache[SHEET_ACCOUNTS]["updated_at"] = time.time()

        if issue_rows:
            sheet_append_rows_and_refresh(
                SHEET_ISSUES,
                issue_rows,
                value_input_option="USER_ENTERED"
            )

        invalidate_stats_cache()

    return {
        "issued": issued,
        "not_found": not_found,
        "not_available": not_available,
        "who_took_text": who_took_text,
        "for_whom": for_whom
    }


def issue_next_quick_account_for_person(for_whom, username):
    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
    who_took_text = f"@{username}" if username else "без username"

    with issue_lock, accounts_lock:
        rows = get_sheet_rows_cached(SHEET_ACCOUNTS)

        candidates = []
        for idx, row in enumerate(rows[1:], start=2):
            if len(row) < 14:
                row = row + [''] * (14 - len(row))
                rows[idx - 1] = row

            if str(row[8]).strip().lower() != "free":
                continue

            purchase_date_obj = parse_date(row[1]) or datetime.max
            candidates.append((idx, purchase_date_obj, row))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[1])
        row_index, _, row = candidates[0]

        sheet_update_raw(
            SHEET_ACCOUNTS,
            f"I{row_index}:L{row_index}",
            [["taken", for_whom, today, who_took_text]]
        )

        row[8] = "taken"
        row[9] = for_whom
        row[10] = today
        row[11] = who_took_text

        with table_cache_lock:
            table_cache[SHEET_ACCOUNTS]["rows"] = rows
            table_cache[SHEET_ACCOUNTS]["updated_at"] = time.time()

        append_issue_row(
            row[0],
            row[1],
            row[2],
            today,
            row[3],
            for_whom
        )

        invalidate_stats_cache()

    return {
        "account_number": row[0],
        "purchase_date": row[1],
        "price": row[2],
        "supplier": row[3],
        "warehouses": row[7],
        "currency": row[12] if len(row) > 12 else "",
        "account_url": row[13] if len(row) > 13 else "",
        "who_took_text": who_took_text,
        "for_whom": for_whom
    }

def confirm_issue(chat_id, user_id, username):
    try:
        state = get_state(user_id)

        current_mode = state.get("mode")
        if current_mode not in ["account_found", "quick_account_found"]:
            send_main_menu(chat_id, "Сначала найди личку.", user_id=user_id)
            return

        account_number = state.get("found_account")
        if not account_number:
            clear_state(user_id)
            send_main_menu(chat_id, "Не нашёл выбранную личку. Начни заново.", user_id=user_id)
            return

        for_whom = state.get("for_whom", "").strip()
        if not for_whom:
            clear_state(user_id)
            send_main_menu(chat_id, "Не найдено кому выдавать личку. Начни заново.", user_id=user_id)
            return

        result = issue_accounts_bulk(
            account_numbers=[account_number],
            for_whom=for_whom,
            username=username
        )

        if not result["issued"]:
            clear_state(user_id)
            send_main_menu(chat_id, "Эта личка уже недоступна. Начни заново.", user_id=user_id)
            return

        item = result["issued"][0]
        who_took_text = result["who_took_text"]

        # быстрая выдача: после подтверждения даем кнопку "Выдать еще"
        if current_mode == "quick_account_found":
            set_state(user_id, {
                "mode": "quick_issue_continue",
                "for_whom": for_whom
            })

            tg_send_message(
                chat_id,
                f"Готово ✅\n\n"
                f"Выдана личка: {item['account_number']}\n"
                f"Кому передали: {for_whom}\n"
                f"Кто взял в боте: {who_took_text}"
            )

            if item["account_url"]:
                tg_send_message(chat_id, f"Ссылка на личку:\n{item['account_url']}")
            else:
                tg_send_message(chat_id, "Ссылка на личку не найдена в колонке N.")

            keyboard = [
                [{"text": BTN_ISSUE_MORE}],
                [{"text": BTN_BACK_TO_MENU}]
            ]
            tg_send_message(chat_id, "Выдать еще личку?", keyboard)
            return

        # обычная выдача
        clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"Выдана личка: {item['account_number']}\n"
            f"Кому передали: {for_whom}\n"
            f"Кто взял в боте: {who_took_text}"
        )

        if item["account_url"]:
            tg_send_message(chat_id, f"Ссылка на личку:\n{item['account_url']}")
        else:
            tg_send_message(chat_id, "Ссылка на личку не найдена в колонке N.")

        send_accounts_main_menu(chat_id, "Меню Accounts:")

    except Exception as e:
        logging.exception("confirm_issue crashed")
        tg_send_message(chat_id, "Ошибка выдачи лички. Попробуй ещё раз.")
        send_accounts_main_menu(chat_id, "Меню Accounts:")

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

        if len(row) < 12:
            row = row + [''] * (12 - len(row))

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
            "data_text": get_full_king_data_from_row(row)
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[0]

def get_free_king_prices_by_geo(geo):
    rows = get_sheet_rows_cached(SHEET_KINGS)

    prices = []
    seen = set()
    geo = str(geo or "").strip()

    for row in rows[1:]:
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if current_name:
            continue
        if not price:
            continue

        if price not in seen:
            seen.add(price)
            prices.append(price)

    def price_sort_key(x):
        try:
            return (0, float(str(x).replace(",", ".")))
        except Exception:
            return (1, str(x))

    prices.sort(key=price_sort_key)
    return prices


def send_king_price_options(chat_id, geo):
    prices = get_free_king_prices_by_geo(geo)

    if not prices:
        send_kings_menu(chat_id, f"Нет свободных king с GEO {geo}.")
        return

    keyboard = []
    row = []

    for price in prices:
        row.append({"text": price})
        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}])

    tg_send_message(chat_id, f"Выбери цену для GEO {geo}:", keyboard)


def find_free_king_by_geo_and_price(geo, price, exclude_row=None):
    rows = get_sheet_rows_cached(SHEET_KINGS)

    candidates = []
    geo = str(geo or "").strip()
    price = normalize_price_key(price)

    for idx, row in enumerate(rows[1:], start=2):
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        row_price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if row_price != price:
            continue
        if current_name:
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
            "data_text": get_full_king_data_from_row(row),
            "row": row
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[0]


def find_free_kings_by_geo_and_price(count_needed, geo, price):
    rows = get_sheet_rows_cached(SHEET_KINGS)

    candidates = []
    geo = str(geo or "").strip()
    price = normalize_price_key(price)

    for idx, row in enumerate(rows[1:], start=2):
        row = ensure_row_len(row, 13)

        status = str(row[4]).strip().lower()
        row_geo = str(row[7]).strip()
        current_name = str(row[0]).strip()
        row_price = normalize_price_key(row[2])

        if status != "free":
            continue
        if row_geo != geo:
            continue
        if row_price != price:
            continue
        if current_name:
            continue

        purchase_date = parse_date(row[1]) or datetime.max

        candidates.append({
            "row_index": idx,
            "purchase_date_obj": purchase_date,
            "purchase_date": row[1],
            "price": row[2],
            "supplier": row[3],
            "geo": row[7],
            "data_text": get_full_king_data_from_row(row),
            "row": row
        })

    candidates.sort(key=lambda x: x["purchase_date_obj"])
    return candidates[:count_needed]


def show_found_king_octo(chat_id, user_id, found):
    state = get_state(user_id)

    state["mode"] = KING_OCTO_MODE_FOUND
    state["king_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "🔍Найден king:\n\n"
        f"🗓Дата покупки: {found['purchase_date']}\n"
        f"💵Цена: {found['price']}\n"
        f"🌐Гео: {found['geo']}\n"
        f"👨‍💻Для кого: {state.get('king_for_whom', 'не указано')}\n"
        f"✏️Название: {state.get('king_name', 'не указано')}"
    )

    if has_bm_in_king_data(found.get("data_text", "")):
        text += "\n✅Есть BM"

    sent = tg_send_inline_message(
        chat_id,
        text,
        [[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_king_octo:{user_id}"
            },
            {
                "text": "🔄другой",
                "callback_data": f"other_king_octo:{user_id}"
            }
        ]]
    )

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state = get_state(user_id)
                state["king_preview_message_id"] = message_id
                set_state(user_id, state)
    except Exception:
        logging.exception("show_found_king_octo failed to save preview message_id")


def edit_found_king_octo_preview(chat_id, message_id, user_id, found):
    state = get_state(user_id)

    state["mode"] = KING_OCTO_MODE_FOUND
    state["king_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "🔍Найден king:\n\n"
        f"🗓Дата покупки: {found['purchase_date']}\n"
        f"💵Цена: {found['price']}\n"
        f"🌐Гео: {found['geo']}\n"
        f"👨‍💻Для кого: {state.get('king_for_whom', 'не указано')}\n"
        f"✏️Название: {state.get('king_name', 'не указано')}"
    )

    if has_bm_in_king_data(found.get("data_text", "")):
        text += "\n✅Есть BM"

    tg_edit_message_text(
        chat_id,
        message_id,
        text,
        inline_buttons=[[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_king_octo:{user_id}"
            },
            {
                "text": "🔄другой",
                "callback_data": f"other_king_octo:{user_id}"
            }
        ]]
    )


def mark_king_octo_preview_as_issued(chat_id, message_id, king_name, king_for_whom, price, geo_value):
    text = (
        "✅ Выдано\n\n"
        f"King выдан.\n"
        f"Название: {king_name}\n"
        f"Для кого: {king_for_whom}\n"
        f"Цена: {price}\n"
        f"Гео: {geo_value}"
    )

    try:
        tg_edit_message_text(
            chat_id,
            message_id,
            text,
            inline_buttons=[]
        )
    except Exception:
        logging.exception("mark_king_octo_preview_as_issued failed")


def send_kings_bulk_found_preview_once(chat_id, user_id):
    state = get_state(user_id)

    queue = state.get("kings_bulk_queue", [])
    if not queue:
        send_kings_menu(chat_id, "Не удалось собрать список king.")
        return

    lines = [
        "🔍Найдены king:",
        "",
        f"👨‍💻Для кого: {state.get('king_for_whom', 'не указано')}",
        ""
    ]

    for i, item in enumerate(queue, start=1):
        lines.append(f"📦Кинг {i} из {len(queue)}")
        lines.append(f"✏️Название: {item.get('king_name', 'не указано')}")
        lines.append(f"💵Цена: {item.get('price', '')}")
        lines.append(f"🌐Гео: {item.get('geo', '')}")

        if has_bm_in_king_data(item.get("data_text", "")):
            lines.append("✅Есть BM")

        lines.append("")

    sent = tg_send_inline_message(
        chat_id,
        "\n".join(lines).strip(),
        [[
            {
                "text": "✅выдать",
                "callback_data": f"confirm_kings_bulk_octo:{user_id}"
            },
            {
                "text": "❌отмена",
                "callback_data": f"cancel_kings_bulk_octo:{user_id}"
            }
        ]]
    )

    state["mode"] = KING_OCTO_MODE_BULK_CONFIRM

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state["kings_bulk_confirm_message_id"] = message_id
    except Exception:
        logging.exception("send_kings_bulk_found_preview_once failed to save message_id")

    set_state_with_custom_ttl(user_id, state, KING_BULK_PROXY_TTL)


def start_kings_bulk_proxy_step(chat_id, user_id):
    state = get_state(user_id)

    queue = state.get("kings_bulk_queue", [])
    current_index = int(state.get("kings_bulk_current_index", 0))

    if current_index >= len(queue):
        finish_kings_bulk(chat_id, user_id)
        return

    if state.get("kings_bulk_skip_all_proxies"):
        process_kings_bulk_proxy_step(
            chat_id=chat_id,
            user_id=user_id,
            username=state.get("kings_bulk_username", ""),
            proxy_text="__SKIP_ALL_PROXIES__"
        )
        return

    current_item = queue[current_index]
    king_name = current_item.get("king_name", "")
    geo = current_item.get("geo", "")
    price = current_item.get("price", "")

    text = (
        f"Скинь socks5 proxy для king {king_name}\n\n"
        f"Цена: {price}\n"
        f"Гео: {geo}\n"
        f"Шаг {current_index + 1} из {len(queue)}\n\n"
        f"Формат:\n"
        f"socks5://login:password@host:port\n"
        f"или\n"
        f"socks5://host:port"
    )

    sent = tg_send_inline_message(
        chat_id,
        text,
        [[
            {
                "text": "⏭️ Пропустить все прокси",
                "callback_data": f"kings_bulk_skip_all_proxies:{user_id}"
            }
        ]]
    )

    state["mode"] = KING_OCTO_MODE_BULK_PROXY

    try:
        if isinstance(sent, dict):
            message_id = sent.get("result", {}).get("message_id")
            if message_id:
                state["kings_bulk_proxy_message_id"] = message_id
    except Exception:
        logging.exception("start_kings_bulk_proxy_step failed to save message_id")

    set_state_with_custom_ttl(user_id, state, KING_BULK_PROXY_TTL)

def confirm_farm_king_octo_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)

            if state.get("mode") != FARM_KING_OCTO_MODE_FOUND:
                send_farm_kings_menu(chat_id, "Сначала выбери farm king заново.")
                return

            row_index = state.get("farm_king_row")
            if not row_index:
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Не найден выбранный farm king. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_FARM_KINGS, force=True)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Farm king не найден в таблице. Начни заново.")
                return

            row = ensure_row_len(rows[row_index - 1], 26)
            sync_id = row[12]

            status = str(row[4]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Этот farm king уже занят.")
                return

            if status == "ban":
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Этот farm king уже в ban.")
                return

            if status != "free":
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Этот farm king недоступен.")
                return

            king_name = str(state.get("farm_king_name", "")).strip()
            if not king_name:
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Не найдено название farm king. Начни заново.")
                return

            current_name_in_row = str(row[0]).strip()
            if not current_name_in_row and farm_king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое название.")
                state["mode"] = FARM_KING_OCTO_MODE_NAME
                set_state(user_id, state)
                return

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            geo_value = str(row[7]).strip()
            data_text = get_full_king_data_from_row(row)

            parsed_farm_king = parse_crypto_king_raw_data(data_text)

            if not parsed_farm_king.get("geo") and geo_value:
                parsed_farm_king["geo"] = geo_value

            preview_message_id = state.get("farm_king_preview_message_id")

            set_state(user_id, {
                "mode": FARM_KING_OCTO_MODE_SINGLE_PROXY,
                "farm_king_row": row_index,
                "farm_king_name": king_name,
                "parsed_farm_king": parsed_farm_king,
                "farm_king_geo_value": geo_value,
                "farm_king_data_text": data_text,
                "farm_king_sync_id": sync_id,
                "farm_king_today": today,
                "farm_king_who_took_text": who_took_text,
                "farm_king_preview_message_id": preview_message_id,
            })

        sent = tg_send_inline_message(
            chat_id,
            f"Теперь пришли proxy для Octo профиля:\n"
            f"{king_name}\n\n"
            f"Формат:\n"
            f"socks5://login:password@host:port",
            [[
                {
                    "text": "⏭️ Пропустить прокси",
                    "callback_data": f"farm_king_skip_proxy:{user_id}"
                }
            ]]
        )
        
        try:
            if isinstance(sent, dict):
                proxy_message_id = sent.get("result", {}).get("message_id")
                if proxy_message_id:
                    state = get_state(user_id)
                    state["farm_king_proxy_message_id"] = proxy_message_id
                    set_state(user_id, state)
        except Exception:
            logging.exception("confirm_farm_king_octo_issue failed to save proxy message_id")
        return

    except Exception:
        logging.exception("confirm_farm_king_octo_issue crashed")
        tg_send_message(chat_id, "Ошибка подготовки выдачи farm king. Попробуй ещё раз.")
        send_farm_kings_menu(chat_id, "Меню Farm King:")

def confirm_king_octo_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)

            if state.get("mode") != KING_OCTO_MODE_FOUND:
                send_kings_menu(chat_id, "Сначала выбери king заново.")
                return

            king_for_whom = str(state.get("king_for_whom", "")).strip()
            if not king_for_whom:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найдено для кого выдавать king. Начни заново.")
                return

            row_index = state.get("king_row")
            if not row_index:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найден выбранный king. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_KINGS, force=True)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_kings_menu(chat_id, "King не найден в таблице. Начни заново.")
                return

            row = ensure_row_len(rows[row_index - 1], 26)
            sync_id = row[12]

            status = str(row[4]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот king уже занят.")
                return

            if status == "ban":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот king уже в ban.")
                return

            if status != "free":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот king недоступен.")
                return

            king_name = str(state.get("king_name", "")).strip()
            if not king_name:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найдено название king. Начни заново.")
                return

            current_name_in_row = str(row[0]).strip()
            if not current_name_in_row and king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое название.")
                state["mode"] = KING_OCTO_MODE_NAME
                set_state(user_id, state)
                return

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            geo_value = str(row[7]).strip()
            data_text = get_full_king_data_from_row(row)

            parsed_king = parse_crypto_king_raw_data(data_text)

            if not parsed_king.get("geo") and geo_value:
                parsed_king["geo"] = geo_value

            preview_message_id = state.get("king_preview_message_id")

            set_state(user_id, {
                "mode": KING_OCTO_MODE_SINGLE_PROXY,
                "king_row": row_index,
                "king_name": king_name,
                "king_for_whom": king_for_whom,
                "parsed_king": parsed_king,
                "king_geo_value": geo_value,
                "king_data_text": data_text,
                "king_sync_id": sync_id,
                "king_today": today,
                "king_who_took_text": who_took_text,
                "king_preview_message_id": preview_message_id,
                "last_accounts_section": "kings",
            })

        sent = tg_send_inline_message(
            chat_id,
            f"Теперь пришли proxy для Octo профиля:\n"
            f"{king_name}\n\n"
            f"Формат:\n"
            f"socks5://login:password@host:port",
            [[
                {
                    "text": "⏭️ Пропустить прокси",
                    "callback_data": f"king_skip_proxy:{user_id}"
                }
            ]]
        )
        
        try:
            if isinstance(sent, dict):
                proxy_message_id = sent.get("result", {}).get("message_id")
                if proxy_message_id:
                    state = get_state(user_id)
                    state["king_proxy_message_id"] = proxy_message_id
                    set_state(user_id, state)
        except Exception:
            logging.exception("confirm_king_octo_issue failed to save proxy message_id")
        return

    except Exception:
        logging.exception("confirm_king_octo_issue crashed")
        tg_send_message(chat_id, "Ошибка подготовки выдачи king. Попробуй ещё раз.")
        send_kings_menu(chat_id, "Меню кингов:")


def process_kings_bulk_proxy_step(chat_id, user_id, username, proxy_text):
    state = get_state(user_id)

    if state.get("mode") != KING_OCTO_MODE_BULK_PROXY:
        send_kings_menu(chat_id, "Сначала начни выдачу king заново.")
        return

    queue = state.get("kings_bulk_queue", [])
    current_index = int(state.get("kings_bulk_current_index", 0))

    if current_index >= len(queue):
        finish_kings_bulk(chat_id, user_id)
        return

    current_item = dict(queue[current_index])
    king_name = current_item.get("king_name", "")
    row_index = current_item.get("row_index")

    proxy_raw = str(proxy_text or "").strip()

    skip_all = (
        proxy_raw == "__SKIP_ALL_PROXIES__"
        or state.get("kings_bulk_skip_all_proxies") is True
    )

    proxy_data = None

    if not skip_all:
        proxy_data = parse_proxy_input(proxy_raw)
        if not proxy_data:
            tg_send_message(
                chat_id,
                f"Не удалось разобрать proxy для king {king_name}.\n"
                f"Пришли proxy заново."
            )
            set_state_with_custom_ttl(user_id, state, KING_BULK_PROXY_TTL)
            return

    rows = get_sheet_rows_cached(SHEET_KINGS, force=True)

    if row_index - 1 >= len(rows):
        current_item["octo_ok"] = False
        current_item["error_text"] = "строка пропала из таблицы"
    else:
        row = ensure_row_len(rows[row_index - 1], 13)

        if str(row[4]).strip().lower() != "free":
            current_item["octo_ok"] = False
            current_item["error_text"] = "king уже не free"
        else:
            data_text = get_full_king_data_from_row(row)
            parsed = parse_crypto_king_raw_data(data_text)

            current_item["data_text"] = data_text
            current_item["parsed_king"] = parsed

            ok = False
            error_text = ""

            try:
                octo_ok, octo_result = ensure_octo_profile_with_retry(
                    ensure_func=ensure_octo_profile_for_crypto_king,
                    profile_name=king_name,
                    parsed=parsed,
                    proxy_data=proxy_data
                )

                if octo_ok:
                    ok = True

                    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
                    who_took_text = f"@{username}" if username else "без username"
                    sync_id = row[12]

                    sheet_update_raw(
                        SHEET_KINGS,
                        f"A{row_index}:I{row_index}",
                        [[
                            king_name,
                            row[1],
                            row[2],
                            row[3],
                            "taken",
                            state.get("king_for_whom", ""),
                            today,
                            row[7],
                            who_took_text
                        ]]
                    )

                    if sync_id:
                        sync_status_to_basebot(
                            BASEBOT_SHEET_KINGS,
                            sync_id,
                            "taken"
                        )

                    append_king_to_issues_sheet(
                        king_name=king_name,
                        purchase_date=row[1],
                        price=row[2],
                        transfer_date=today,
                        supplier=row[3],
                        for_whom=state.get("king_for_whom", "")
                    )

                    invalidate_stats_cache()
                else:
                    error_text = str(octo_result)

            except Exception as e:
                logging.exception("process_kings_bulk_proxy_step crashed")
                error_text = str(e)

            current_item["octo_ok"] = ok
            current_item["error_text"] = error_text
            current_item["proxy_skipped"] = skip_all

    results = state.get("kings_bulk_results", [])
    results.append(current_item)
    state["kings_bulk_results"] = results
    state["kings_bulk_current_index"] = current_index + 1
    state["kings_bulk_username"] = username

    set_state_with_custom_ttl(user_id, state, KING_BULK_PROXY_TTL)

    if state["kings_bulk_current_index"] >= len(queue):
        finish_kings_bulk(chat_id, user_id)
        return

    start_kings_bulk_proxy_step(chat_id, user_id)

def process_farm_kings_bulk_proxy_step(chat_id, user_id, username, proxy_text):
    state = get_state(user_id)

    if state.get("mode") != FARM_KING_OCTO_MODE_BULK_PROXY:
        send_farm_kings_menu(chat_id, "Сначала начни выдачу farm king заново.")
        return

    queue = state.get("farm_kings_bulk_queue", [])
    current_index = int(state.get("farm_kings_bulk_current_index", 0))

    if current_index >= len(queue):
        finish_farm_kings_bulk(chat_id, user_id)
        return

    current_item = dict(queue[current_index])
    king_name = current_item.get("king_name", "")
    row_index = current_item.get("row_index")

    proxy_raw = str(proxy_text or "").strip()

    skip_all = (
        proxy_raw == "__SKIP_ALL_PROXIES__"
        or state.get("farm_kings_bulk_skip_all_proxies") is True
    )

    proxy_data = None

    if not skip_all:
        proxy_data = parse_proxy_input(proxy_raw)
        if not proxy_data:
            tg_send_message(
                chat_id,
                f"Не удалось разобрать proxy для farm king {king_name}.\n"
                f"Пришли proxy заново."
            )
            set_state_with_custom_ttl(user_id, state, FARM_KING_BULK_PROXY_TTL)
            return

    rows = get_sheet_rows_cached(SHEET_FARM_KINGS, force=True)

    if row_index - 1 >= len(rows):
        current_item["octo_ok"] = False
        current_item["error_text"] = "строка пропала из таблицы"
    else:
        row = ensure_row_len(rows[row_index - 1], 13)

        if str(row[4]).strip().lower() != "free":
            current_item["octo_ok"] = False
            current_item["error_text"] = "farm king уже не free"
        else:
            data_text = get_full_king_data_from_row(row)
            parsed = parse_crypto_king_raw_data(data_text)

            current_item["data_text"] = data_text
            current_item["parsed_farm_king"] = parsed

            ok = False
            error_text = ""

            try:
                octo_ok, octo_result = ensure_octo_profile_with_retry(
                    ensure_func=ensure_octo_profile_for_farm_king,
                    profile_name=king_name,
                    parsed=parsed,
                    proxy_data=proxy_data
                )

                if octo_ok:
                    ok = True

                    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
                    who_took_text = f"@{username}" if username else "без username"
                    sync_id = row[12]

                    sheet_update_raw(
                        SHEET_FARM_KINGS,
                        f"A{row_index}:I{row_index}",
                        [[
                            king_name,
                            row[1],
                            row[2],
                            row[3],
                            "taken",
                            "farm",
                            today,
                            row[7],
                            who_took_text
                        ]]
                    )

                    if sync_id:
                        sync_status_to_basebot(
                            BASEBOT_SHEET_FARM_KINGS,
                            sync_id,
                            "taken"
                        )

                    append_king_to_issues_sheet(
                        king_name=king_name,
                        purchase_date=row[1],
                        price=row[2],
                        transfer_date=today,
                        supplier=row[3],
                        for_whom="farm"
                    )

                    invalidate_stats_cache()
                else:
                    error_text = str(octo_result)

            except Exception as e:
                logging.exception("process_farm_kings_bulk_proxy_step crashed")
                error_text = str(e)

            current_item["octo_ok"] = ok
            current_item["error_text"] = error_text
            current_item["proxy_skipped"] = skip_all

    results = state.get("farm_kings_bulk_results", [])
    results.append(current_item)
    state["farm_kings_bulk_results"] = results
    state["farm_kings_bulk_current_index"] = current_index + 1
    state["farm_kings_bulk_username"] = username

    set_state_with_custom_ttl(user_id, state, FARM_KING_BULK_PROXY_TTL)

    if state["farm_kings_bulk_current_index"] >= len(queue):
        finish_farm_kings_bulk(chat_id, user_id)
        return

    start_farm_kings_bulk_proxy_step(chat_id, user_id)

def build_kings_bulk_result_messages(results, for_whom, max_len=3500):
    success_items = [x for x in results if x.get("octo_ok")]
    failed_items = [x for x in results if not x.get("octo_ok")]

    if success_items and not failed_items:
        header = "✅ Кинги заведены в Octo"
    elif failed_items and not success_items:
        header = "❌ Кинги не заведены в Octo"
    else:
        header = "⚠️ Часть кингов заведена в Octo"

    header_text = (
        f"{header}\n"
        f"👨‍💻Для кого: {for_whom or 'не указано'}"
    )

    blocks = []

    for item in results:
        king_name = str(item.get("king_name", "")).strip() or "не указано"
        price = str(item.get("price", "")).strip() or "не указана"
        geo = str(item.get("geo", "")).strip() or "не указано"

        block_lines = [
            f"✏️Название: {king_name}",
            f"💵Цена: {price}",
            f"🌐Гео: {geo}",
        ]

        if not item.get("octo_ok"):
            error_text = str(item.get("error_text", "")).strip() or "не удалось завести в Octo"
            block_lines.append(f"❌Ошибка: {error_text}")

        blocks.append("\n".join(block_lines))

    messages = []
    current = header_text
    first_block = True

    for block in blocks:
        if first_block:
            separator = "\n\n"
        elif current:
            separator = "\n"
        else:
            separator = ""

        candidate = f"{current}{separator}{block}" if current else block

        if len(candidate) <= max_len:
            current = candidate
            first_block = False
            continue

        if current:
            messages.append(current)

        current = block
        first_block = False

    if current:
        messages.append(current)

    return messages


def send_kings_bulk_followup_messages(chat_id, results):
    tg_send_message(
        chat_id,
        "Вручную проверь и выставь:\n"
        "• User-Agent\n"
        "• расширения\n"
        "• куки"
    )

    ua_lines = []
    bm_lines = []

    for item in results:
        if not item.get("octo_ok"):
            continue

        king_name = item.get("king_name", "")
        parsed = item.get("parsed_king", {}) or {}

        user_agent = str(parsed.get("user_agent", "")).strip()
        bm_links = parsed.get("bm_links", []) or []
        bm_email_pairs = parsed.get("bm_email_pairs", []) or []

        if user_agent:
            ua_lines.append(f"у king {king_name} есть User-Agent✅")

        if bm_links or bm_email_pairs:
            bm_lines.append(f"у king {king_name} есть BM✅")

    if ua_lines:
        tg_send_message(chat_id, "\n".join(ua_lines))

    if bm_lines:
        tg_send_message(chat_id, "\n".join(bm_lines))


def finish_kings_bulk(chat_id, user_id):
    state = get_state(user_id)

    results = state.get("kings_bulk_results", [])
    for_whom = state.get("king_for_whom", "")

    if not results:
        clear_state(user_id)
        send_kings_menu(chat_id, "Не удалось выдать ни одного king.")
        return

    message_parts = build_kings_bulk_result_messages(results, for_whom)

    success_items = [x for x in results if x.get("octo_ok")]
    inline_buttons = []

    if len(success_items) == 1:
        inline_buttons = [[
            {
                "text": "📄 Скачать txt",
                "callback_data": f"download_king_bulk_txt:{user_id}:0"
            }
        ]]
    elif len(success_items) >= 2:
        inline_buttons = [[
            {
                "text": "📦 Скачать zip",
                "callback_data": f"download_king_bulk_zip:{user_id}"
            }
        ]]

    tg_send_inline_message_parts(
        chat_id=chat_id,
        message_parts=message_parts,
        inline_buttons=inline_buttons
    )

    send_kings_bulk_followup_messages(chat_id, results)

    download_state = {
        "mode": "kings_bulk_done",
        "kings_bulk_results": results,
        "updated_at": time.time(),
        "last_accounts_section": "kings",
    }
    set_state(user_id, download_state)

    send_kings_menu(chat_id, "Выбери следующее действие:")

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

def return_bm_to_ban(bm_id, comment_text=""):
    bm_info = find_bm_in_base(bm_id)
    if not bm_info:
        return False, "БМ не найден в База_БМ."

    row = bm_info["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    status = str(row[4]).strip().lower()

    if status == "ban":
        return False, "Этот БМ уже в ban."

    sheet_update_and_refresh(
        SHEET_BMS,
        f"E{bm_info['row_index']}:F{bm_info['row_index']}",
        [["ban", "ban"]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[9]
    
    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_BMS, sync_id, "ban")

    issue_info = find_last_bm_issue_row(bm_id)
    if issue_info:
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, f"БМ '{bm_id}' переведён в ban."

def return_bm_to_free(bm_id):
    bm_info = find_bm_in_base(bm_id)
    if not bm_info:
        return False, "БМ не найден в База_БМ."

    row = bm_info["row"]
    if len(row) < 9:
        row = row + [''] * (9 - len(row))

    status = str(row[4]).strip().lower()

    if status == "free":
        return False, "Этот БМ уже free."

    sheet_update_and_refresh(
        SHEET_BMS,
        f"E{bm_info['row_index']}:H{bm_info['row_index']}",
        [["free", "", "", ""]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[9]

    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_BMS, sync_id, "free")

    delete_last_bm_issue_row(bm_id)

    invalidate_stats_cache()
    return True, f"БМ '{bm_id}' возвращён в free."

def show_found_bm(chat_id, user_id, found):
    state = get_state(user_id)
    state["mode"] = "bm_found"
    state["bm_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "🔍Найден БМ:\n\n"
        f"🔥ID БМа: {found['bm_id']}\n"
        f"💵цена: {found['price']}\n"
        f"👨‍💻Для кого: {state.get('bm_for_whom', 'не указано')}"
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

            bm_for_whom = state.get("bm_for_whom", "").strip()
            if not bm_for_whom:
                clear_state(user_id)
                send_bms_menu(chat_id, "Не найдено для кого выдавать БМ. Начни заново.")
                return

            row_index = state.get("bm_row")
            if not row_index:
                clear_state(user_id)
                send_bms_menu(chat_id, "Не найден выбранный БМ. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_BMS, force=True)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_bms_menu(chat_id, "БМ не найден в таблице. Начни заново.")
                return

            row = rows[row_index - 1]
            row = ensure_row_len(row, 10)
            sync_id = row[9]

            status = str(row[4]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_bms_menu(chat_id, "Этот БМ уже занят.")
                return

            if status == "ban":
                clear_state(user_id)
                send_bms_menu(chat_id, "Этот БМ уже в ban.")
                return

            if status != "free":
                clear_state(user_id)
                send_bms_menu(chat_id, "Этот БМ недоступен.")
                return

            bm_id = row[0]
            purchase_date = row[1]
            price = row[2]
            supplier = row[3]
            data_text = row[8]

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            sheet_update_and_refresh(
                SHEET_BMS,
                f"E{row_index}:H{row_index}",
                [[
                    "taken",
                    bm_for_whom,
                    who_took_text,
                    today
                ]]
            )

            if sync_id:
                sync_status_to_basebot(BASEBOT_SHEET_BMS, sync_id, "taken")

            append_issue_row_fixed([
                bm_id,
                "БМ",
                purchase_date,
                normalize_numeric_for_sheet(price),
                today,
                supplier,
                bm_for_whom
            ])

            invalidate_stats_cache()
            clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"БМ выдан.\n"
            f"🔥ID БМа: {bm_id}\n"
            f"👨‍💻Для кого: {bm_for_whom}"
        )

        if data_text:
            tg_send_message(chat_id, data_text)
        else:
            tg_send_message(chat_id, "Данные БМа не найдены.")

        send_bms_menu(chat_id, "Выбери следующее действие:")

    except Exception:
        logging.exception("confirm_bm_issue crashed")
        tg_send_message(chat_id, "Ошибка выдачи БМ. Попробуй ещё раз.")
        send_bms_menu(chat_id, "Меню БМов:")

def issue_bms_bulk(chat_id, user_id, username, count_needed):
    try:
        state = get_state(user_id)

        if state.get("mode") != "awaiting_bm_count":
            send_bms_menu(chat_id, "Сначала начни выдачу БМ заново.")
            return

        bm_for_whom = state.get("bm_for_whom", "").strip()
        if not bm_for_whom:
            clear_state(user_id)
            send_bms_menu(chat_id, "Не найдено для кого выдавать БМы. Начни заново.")
            return

        rows = get_sheet_rows_cached(SHEET_BMS)
        candidates = []

        for idx, row in enumerate(rows[1:], start=2):
            if len(row) < 9:
                row = row + [''] * (9 - len(row))

            if str(row[4]).strip().lower() != "free":
                continue

            purchase_date = parse_date(row[1]) or datetime.max
            candidates.append((idx, purchase_date, row))

        candidates.sort(key=lambda x: x[1])
        selected = candidates[:count_needed]

        if len(selected) < count_needed:
            clear_state(user_id)
            send_bms_menu(chat_id, f"Недостаточно свободных БМов. Доступно: {len(selected)}")
            return

        today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
        who_took_text = f"@{username}" if username else "без username"

        issue_rows = []
        issued_items = []

        with issue_lock:
            current_rows = get_sheet_rows_cached(SHEET_BMS, force=True)

            for row_index, _, _ in selected:
                if row_index - 1 >= len(current_rows):
                    clear_state(user_id)
                    send_bms_menu(chat_id, "Один из БМов пропал из таблицы. Начни заново.")
                    return

                row = current_rows[row_index - 1]
                row = ensure_row_len(row, 26)
                sync_id = row[9]

                if str(row[4]).strip().lower() != "free":
                    clear_state(user_id)
                    send_bms_menu(chat_id, "Один из БМов уже не свободен. Начни заново.")
                    return

            for row_index, _, _ in selected:
                row = current_rows[row_index - 1]
                if len(row) < 9:
                    row = row + [''] * (9 - len(row))

                sheet_update_raw(
                    SHEET_BMS,
                    f"E{row_index}:H{row_index}",
                    [[
                        "taken",
                        bm_for_whom,
                        who_took_text,
                        today
                    ]]
                )

                issue_rows.append([
                    row[0],
                    "БМ",
                    row[1],
                    normalize_numeric_for_sheet(row[2]),
                    today,
                    row[3],
                    bm_for_whom
                ])

                issued_items.append({
                    "bm_id": row[0],
                    "data_text": row[8] if len(row) > 8 else "",
                    "sync_id": sync_id
                })

            refresh_sheet_cache(SHEET_BMS)

            if issue_rows:
                sheet_append_rows_and_refresh(
                    SHEET_ISSUES,
                    issue_rows,
                    value_input_option="USER_ENTERED"
                )

            for item in issued_items:
                if item["sync_id"]:
                    sync_status_to_basebot(BASEBOT_SHEET_BMS, item["sync_id"], "taken")

            invalidate_stats_cache()

        clear_state(user_id)

        for item in issued_items:
            tg_send_message(
                chat_id,
                f"Готово ✅\n\n"
                f"БМ выдан.\n"
                f"ID БМа: {item['bm_id']}\n"
                f"Для кого: {bm_for_whom}\n"
                f"Кто взял в боте: {who_took_text}"
            )

            if item["data_text"]:
                tg_send_message(chat_id, item["data_text"])
            else:
                tg_send_message(chat_id, "Данные БМа не найдены.")

        send_accounts_main_menu(chat_id, "Меню Accounts:")

    except Exception:
        logging.exception("issue_bms_bulk crashed")
        tg_send_message(chat_id, "Ошибка массовой выдачи БМов. Попробуй ещё раз.")
        send_accounts_main_menu(chat_id, "Меню Accounts:")
    
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
        f"Для кого: {state.get('king_for_whom', 'не указано')}\n"
        f"Название: {state.get('king_name', 'не указано')}"
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

            king_for_whom = state.get("king_for_whom", "").strip()
            if not king_for_whom:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найдено для кого выдавать кинга. Начни заново.")
                return

            row_index = state.get("king_row")
            if not row_index:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найден выбранный кинг. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_KINGS, force=True)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_kings_menu(chat_id, "Кинг не найден в таблице. Начни заново.")
                return

            row = rows[row_index - 1]
            row = ensure_row_len(row, 26)
            sync_id = row[12]

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

            king_name = str(state.get("king_name", "")).strip()
            if not king_name:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найдено название кинга. Начни заново.")
                return

            current_name_in_row = str(row[0]).strip()
            if not current_name_in_row and king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое название.")
                state["mode"] = "awaiting_king_name"
                set_state(user_id, state)
                return

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            geo_value = str(row[7]).strip()
            data_text = get_full_king_data_from_row(row)

            sheet_update_and_refresh(
                SHEET_KINGS,
                f"A{row_index}:L{row_index}",
                [[
                    king_name,
                    row[1],
                    row[2],
                    row[3],
                    "taken",
                    king_for_whom,
                    today,
                    geo_value,
                    who_took_text,
                    row[9],
                    row[10],
                    row[11]
                ]]
            )

            if sync_id:
                sync_status_to_basebot(BASEBOT_SHEET_KINGS, sync_id, "taken")

            append_king_to_issues_sheet(
                king_name=king_name,
                purchase_date=row[1],
                price=row[2],
                transfer_date=today,
                supplier=row[3],
                for_whom=king_for_whom
            )

            invalidate_stats_cache()
            clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"Кинг выдан.\n"
            f"Название: {king_name}\n"
            f"Для кого: {king_for_whom}\n"
            f"Цена: {row[2]}\n"
            f"Гео: {geo_value}"
        )

        tg_send_king_data_as_txt(
            chat_id=chat_id,
            king_name=king_name,
            data_text=data_text
        )

        send_accounts_main_menu(chat_id, "Меню Accounts:")

    except Exception as e:
        logging.exception("confirm_king_issue crashed")
        tg_send_message(chat_id, "Ошибка выдачи кинга. Попробуй ещё раз.")
        send_accounts_main_menu(chat_id, "Меню Accounts:")

def issue_kings_bulk(chat_id, user_id, username, king_names):
    try:
        with issue_lock:
            state = get_state(user_id)

            king_for_whom = str(state.get("king_for_whom", "")).strip()
            selected_geo = str(state.get("king_geo", "")).strip()

            if not king_for_whom:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найдено для кого выдавать кинги. Начни заново.")
                return

            count_needed = len(king_names)
            if count_needed <= 0:
                clear_state(user_id)
                send_kings_menu(chat_id, "Количество кингов должно быть больше нуля.")
                return

            duplicate_names = []
            for name in king_names:
                if king_name_exists(name):
                    duplicate_names.append(name)

            if duplicate_names:
                clear_state(user_id)
                tg_send_message(
                    chat_id,
                    "Эти названия уже существуют:\n" + "\n".join(duplicate_names[:20])
                )
                send_kings_menu(chat_id, "Выдача отменена. Начни заново.")
                return

            current_rows = get_sheet_rows_cached(SHEET_KINGS, force=True)

            selected_rows = []
            for idx, row in enumerate(current_rows[1:], start=2):
                row = ensure_row_len(row, 13)

                status = str(row[4]).strip().lower()
                current_name = str(row[0]).strip()
                current_geo = str(row[7]).strip()

                if status == "free" and not current_name:
                    if selected_geo and current_geo != selected_geo:
                        continue

                    selected_rows.append({
                        "row_index": idx,
                        "row": row
                    })

                    if len(selected_rows) >= count_needed:
                        break

            if len(selected_rows) < count_needed:
                clear_state(user_id)
                if selected_geo:
                    send_kings_menu(
                        chat_id,
                        f"Недостаточно свободных king по GEO {selected_geo}. Нужно: {count_needed}, доступно: {len(selected_rows)}"
                    )
                else:
                    send_kings_menu(
                        chat_id,
                        f"Недостаточно свободных king. Нужно: {count_needed}, доступно: {len(selected_rows)}"
                    )
                return

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            issued_items = []

            # Перепроверка перед записью
            for item, king_name in zip(selected_rows, king_names):
                row_index = item["row_index"]

                if row_index - 1 >= len(current_rows):
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Ошибка: один из кингов пропал из таблицы.")
                    return

                row = ensure_row_len(current_rows[row_index - 1], 13)

                if str(row[4]).strip().lower() != "free":
                    clear_state(user_id)
                    send_kings_menu(chat_id, f"Кинг '{row[0] or king_name}' уже не свободен.")
                    return

                current_geo = str(row[7]).strip()
                if selected_geo and current_geo != selected_geo:
                    clear_state(user_id)
                    send_kings_menu(
                        chat_id,
                        f"Ошибка GEO: ожидался {selected_geo}, но найден {current_geo}. Начни заново."
                    )
                    return

            # Обновляем строки
            for item, king_name in zip(selected_rows, king_names):
                row_index = item["row_index"]
                row = ensure_row_len(current_rows[row_index - 1], 13)
                sync_id = row[12]

                current_geo = str(row[7]).strip()
                if selected_geo and current_geo != selected_geo:
                    clear_state(user_id)
                    send_kings_menu(
                        chat_id,
                        f"Ошибка GEO перед записью: ожидался {selected_geo}, но найден {current_geo}. Выдача отменена."
                    )
                    return

                sheet_update_raw(
                    SHEET_KINGS,
                    f"A{row_index}:L{row_index}",
                    [[
                        king_name,
                        row[1],
                        row[2],
                        row[3],
                        "taken",
                        king_for_whom,
                        today,
                        row[7],
                        who_took_text,
                        row[9],
                        row[10],
                        row[11]
                    ]]
                )

                issued_items.append({
                    "king_name": king_name,
                    "purchase_date": row[1],
                    "price": row[2],
                    "supplier": row[3],
                    "geo": row[7],
                    "data_text": get_full_king_data_from_row(row),
                    "sync_id": sync_id
                })

            logging.info(f"issue_kings_bulk updated {len(issued_items)} kings for user_id={user_id}")

            for item in issued_items:
                if item["sync_id"]:
                    try:
                        sync_status_to_basebot(BASEBOT_SHEET_KINGS, item["sync_id"], "taken")
                    except Exception:
                        logging.exception(
                            f"issue_kings_bulk sync failed for {item['king_name']} sync_id={item['sync_id']}"
                        )

            for item in issued_items:
                append_king_to_issues_sheet(
                    king_name=item["king_name"],
                    purchase_date=item["purchase_date"],
                    price=item["price"],
                    transfer_date=today,
                    supplier=item["supplier"],
                    for_whom=king_for_whom
                )

            invalidate_stats_cache()
            clear_state(user_id)

        try:
            tg_send_message(
                chat_id,
                f"Готово ✅\n\n"
                f"Выдано кингов: {len(issued_items)}\n"
                f"Для кого: {king_for_whom}"
            )
        except Exception:
            logging.exception("issue_kings_bulk summary send failed")

        if len(issued_items) > 5:
            try:
                archive_name = f"kings_{king_for_whom}_{datetime.now(MOSCOW_TZ).strftime('%Y%m%d_%H%M%S')}.zip"
                tg_send_kings_as_zip(
                    chat_id=chat_id,
                    issued_items=issued_items,
                    archive_name=archive_name
                )
            except Exception:
                logging.exception("issue_kings_bulk zip send failed")
                tg_send_message(
                    chat_id,
                    "Кинги выданы, но zip-архив не удалось отправить."
                )
        else:
            txt_failed = []

            for item in issued_items:
                try:
                    tg_send_message(
                        chat_id,
                        f"Готово ✅\n\n"
                        f"Кинг выдан.\n"
                        f"Название: {item['king_name']}\n"
                        f"Для кого: {king_for_whom}\n"
                        f"Цена: {item['price']}\n"
                        f"Гео: {item['geo']}"
                    )

                    tg_send_king_data_as_txt(
                        chat_id=chat_id,
                        king_name=item["king_name"],
                        data_text=item["data_text"]
                    )
                except Exception:
                    logging.exception(f"issue_kings_bulk send failed for {item['king_name']}")
                    txt_failed.append(item["king_name"])

            if txt_failed:
                tg_send_message(
                    chat_id,
                    "Эти кинги выданы, но txt не удалось отправить:\n" + "\n".join(txt_failed[:20])
                )

        send_kings_menu(chat_id, "Выбери следующее действие:")

    except Exception as e:
        logging.exception("issue_kings_bulk crashed")
        tg_send_message(chat_id, "Ошибка массовой выдачи king. Попробуй ещё раз.")
        send_kings_menu(chat_id, "Меню кингов:")

def confirm_crypto_king_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)

            if state.get("mode") != "crypto_king_found":
                send_kings_menu(chat_id, "Сначала выбери crypto king заново.")
                return

            king_for_whom = str(state.get("king_for_whom", "")).strip()
            if not king_for_whom:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найдено для кого выдавать crypto king. Начни заново.")
                return

            row_index = state.get("king_row")
            if not row_index:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найден выбранный crypto king. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS, force=True)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_kings_menu(chat_id, "Crypto king не найден в таблице. Начни заново.")
                return

            row = ensure_row_len(rows[row_index - 1], 26)
            sync_id = row[12]

            status = str(row[4]).strip().lower()

            if status == "taken":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот crypto king уже занят.")
                return

            if status == "ban":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот crypto king уже в ban.")
                return

            if status != "free":
                clear_state(user_id)
                send_kings_menu(chat_id, "Этот crypto king недоступен.")
                return

            king_name = str(state.get("king_name", "")).strip()
            if not king_name:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найдено название crypto king. Начни заново.")
                return

            current_name_in_row = str(row[0]).strip()
            if not current_name_in_row and crypto_king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое название.")
                state["mode"] = "awaiting_crypto_king_name"
                set_state(user_id, state)
                return

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            geo_value = str(row[7]).strip()
            data_text = get_full_king_data_from_row(row)

            parsed_crypto = parse_crypto_king_raw_data(data_text)

            if not parsed_crypto.get("geo") and geo_value:
                parsed_crypto["geo"] = geo_value

            set_state(user_id, {
                "mode": "awaiting_crypto_king_octo_proxy",
                "king_row": row_index,
                "king_name": king_name,
                "king_for_whom": king_for_whom,
                "parsed_crypto": parsed_crypto,
                "crypto_geo_value": geo_value,
                "crypto_data_text": data_text,
                "crypto_sync_id": sync_id,
                "crypto_today": today,
                "crypto_who_took_text": who_took_text,
            })

        send_text_input_prompt(
            chat_id,
            f"Теперь пришли proxy для Octo профиля:\n"
            f"{king_name}\n\n"
            f"Формат:\n"
            f"socks5://login:password@host:port"
        )
        return

    except Exception as e:
        logging.exception("confirm_crypto_king_issue crashed")
        tg_send_message(chat_id, "Ошибка подготовки выдачи crypto king. Попробуй ещё раз.")
        send_kings_menu(chat_id, "Меню кингов:")

def process_crypto_bulk_proxy_step(chat_id, user_id, username, proxy_text):
    state = get_state(user_id)

    if state.get("mode") != CRYPTO_BULK_MODE_PROXY:
        send_kings_menu(chat_id, "Сначала начни выдачу crypto king заново.")
        return

    queue = state.get("crypto_bulk_queue", [])
    current_index = int(state.get("crypto_bulk_current_index", 0))

    if current_index >= len(queue):
        finish_crypto_kings_bulk(chat_id, user_id)
        return

    current_item = dict(queue[current_index])
    king_name = current_item.get("king_name", "")
    row_index = current_item.get("row_index")

    proxy_raw = str(proxy_text or "").strip()

    skip_all = (
        proxy_raw == "__SKIP_ALL_PROXIES__"
        or state.get("crypto_bulk_skip_all_proxies") is True
    )

    proxy_data = None

    if not skip_all:
        proxy_data = parse_proxy_input(proxy_raw)
        if not proxy_data:
            tg_send_message(
                chat_id,
                f"Не удалось разобрать proxy для кинга {king_name}.\n"
                f"Пришли proxy заново."
            )
            set_state_with_custom_ttl(user_id, state, CRYPTO_BULK_PROXY_TTL)
            return

    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS, force=True)

    if row_index - 1 >= len(rows):
        current_item["octo_ok"] = False
        current_item["error_text"] = "строка пропала из таблицы"
    else:
        row = ensure_row_len(rows[row_index - 1], 13)

        if str(row[4]).strip().lower() != "free":
            current_item["octo_ok"] = False
            current_item["error_text"] = "king уже не free"
        else:
            data_text = get_full_king_data_from_row(row)
            parsed = parse_crypto_king_raw_data(data_text)

            current_item["data_text"] = data_text
            current_item["parsed_crypto"] = parsed

            ok = False
            error_text = ""

            try:
                octo_ok, octo_result = ensure_octo_profile_with_retry(
                    ensure_func=ensure_octo_profile_for_crypto_king,
                    profile_name=king_name,
                    parsed=parsed,
                    proxy_data=proxy_data
                )

                if octo_ok:
                    ok = True

                    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
                    who_took_text = f"@{username}" if username else "без username"
                    sync_id = row[12]

                    sheet_update_raw(
                        SHEET_CRYPTO_KINGS,
                        f"A{row_index}:I{row_index}",
                        [[
                            king_name,
                            row[1],
                            row[2],
                            row[3],
                            "taken",
                            state.get("king_for_whom", ""),
                            today,
                            row[7],
                            who_took_text
                        ]]
                    )

                    if sync_id:
                        sync_status_to_basebot(
                            BASEBOT_SHEET_CRYPTO_KINGS,
                            sync_id,
                            "taken"
                        )

                    append_issue_row_fixed([
                        king_name,
                        "KING",
                        row[1],
                        normalize_numeric_for_sheet(row[2]),
                        today,
                        row[3],
                        state.get("king_for_whom", "")
                    ])

                    invalidate_stats_cache()
                else:
                    error_text = str(octo_result)

            except Exception as e:
                logging.exception("process_crypto_bulk_proxy_step crashed")
                error_text = str(e)

            current_item["octo_ok"] = ok
            current_item["error_text"] = error_text
            current_item["proxy_skipped"] = skip_all

    results = state.get("crypto_bulk_results", [])
    results.append(current_item)
    state["crypto_bulk_results"] = results
    state["crypto_bulk_current_index"] = current_index + 1
    state["crypto_bulk_username"] = username

    set_state_with_custom_ttl(user_id, state, CRYPTO_BULK_PROXY_TTL)

    if state["crypto_bulk_current_index"] >= len(queue):
        finish_crypto_kings_bulk(chat_id, user_id)
        return

    start_crypto_kings_bulk_proxy_step(chat_id, user_id)

def append_king_to_issues_sheet(king_name, purchase_date, price, transfer_date, supplier, for_whom):
    append_issue_row_fixed([
        king_name,
        "KING",
        purchase_date,
        normalize_numeric_for_sheet(price),
        transfer_date,
        supplier,
        for_whom
    ])

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

def delete_last_king_issue_row(king_name):
    issue_info = find_last_king_issue_row(king_name)
    if not issue_info:
        return False

    sheet_delete_row_and_refresh(SHEET_ISSUES, issue_info["row_index"])
    return True

def find_last_bm_issue_row(bm_id):
    rows = get_sheet_rows_cached(SHEET_ISSUES)

    last_match = None
    target = str(bm_id).strip().lower()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 7:
            row = row + [''] * (7 - len(row))

        issue_name = str(row[0]).strip().lower()
        issue_type = str(row[1]).strip().lower()

        if issue_name == target and issue_type in ["бм", "bm"]:
            last_match = {
                "row_index": idx,
                "row": row
            }

    return last_match

def find_last_fp_issue_row(fp_link):
    rows = get_sheet_rows_cached(SHEET_ISSUES)

    last_match = None
    target = str(fp_link).strip().lower()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 7:
            row = row + [''] * (7 - len(row))

        issue_name = str(row[0]).strip().lower()
        issue_type = str(row[1]).strip().lower()

        if issue_name == target and issue_type == "fp":
            last_match = {
                "row_index": idx,
                "row": row
            }

    return last_match

def delete_last_bm_issue_row(bm_id):
    issue_info = find_last_bm_issue_row(bm_id)
    if not issue_info:
        return False

    sheet_delete_row_and_refresh(SHEET_ISSUES, issue_info["row_index"])
    return True


def delete_last_fp_issue_row(fp_link):
    issue_info = find_last_fp_issue_row(fp_link)
    if not issue_info:
        return False

    sheet_delete_row_and_refresh(SHEET_ISSUES, issue_info["row_index"])
    return True

def find_king_in_base_by_name(king_name):
    target = str(king_name).strip().lower()

    for sheet_name in [SHEET_KINGS, SHEET_CRYPTO_KINGS]:
        rows = get_sheet_rows_cached(sheet_name)

        for idx, row in enumerate(rows[1:], start=2):
            if len(row) < 12:
                row = row + [''] * (12 - len(row))

            existing_name = str(row[0]).strip().lower()

            if existing_name == target:
                return {
                    "sheet_name": sheet_name,
                    "row_index": idx,
                    "row": row
                }

    return None


def return_king_to_ban(king_name, comment_text=""):
    base_info = find_king_in_base_by_name(king_name)
    if not base_info:
        return False, "Кинг не найден в базах."

    row = base_info["row"]
    sheet_name = base_info["sheet_name"]

    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    status = str(row[4]).strip().lower()

    if status == "ban":
        return False, "Этот кинг уже в ban."

    sheet_update_and_refresh(
        sheet_name,
        f"E{base_info['row_index']}:F{base_info['row_index']}",
        [["ban", "ban"]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[12]
    
    if sync_id:
        if sheet_name == SHEET_KINGS:
            sync_status_to_basebot(BASEBOT_SHEET_KINGS, sync_id, "ban")
        elif sheet_name == SHEET_CRYPTO_KINGS:
            sync_status_to_basebot(BASEBOT_SHEET_CRYPTO_KINGS, sync_id, "ban")

    issue_info = find_last_king_issue_row(king_name)
    if issue_info:
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, f"Кинг '{king_name}' переведён в ban."

def return_king_to_free(king_name):
    base_info = find_king_in_base_by_name(king_name)
    if not base_info:
        return False, "Кинг не найден в базах."

    row = base_info["row"]
    sheet_name = base_info["sheet_name"]

    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    status = str(row[4]).strip().lower()

    if status == "free":
        return False, "Этот кинг уже free."

    old_king_name = str(row[0]).strip()

    sheet_update_and_refresh(
        sheet_name,
        f"A{base_info['row_index']}:I{base_info['row_index']}",
        [[
            "",          # A название очищаем
            row[1],      # B дата покупки
            row[2],      # C цена
            row[3],      # D поставщик
            "free",      # E статус
            "",          # F для кого
            "",          # G дата взятия
            row[7],      # H geo
            ""           # I кто взял
        ]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[12]
    
    if sync_id:
        if sheet_name == SHEET_KINGS:
            sync_status_to_basebot(BASEBOT_SHEET_KINGS, sync_id, "free")
        elif sheet_name == SHEET_CRYPTO_KINGS:
            sync_status_to_basebot(BASEBOT_SHEET_CRYPTO_KINGS, sync_id, "free")

    if old_king_name:
        delete_last_king_issue_row(old_king_name)

    invalidate_stats_cache()
    return True, f"Кинг '{king_name}' возвращён в free."

def find_crypto_king_in_base_by_name(king_name):
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)
    target = str(king_name).strip().lower()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        existing_name = str(row[0]).strip().lower()
        if existing_name == target:
            return {
                "row_index": idx,
                "row": row
            }

    return None


def return_crypto_king_to_ban(king_name, comment_text=""):
    base_info = find_crypto_king_in_base_by_name(king_name)
    if not base_info:
        return False, "Crypto king не найден в База_крипта_кинги."

    row = base_info["row"]
    if len(row) < 10:
        row = row + [''] * (10 - len(row))

    status = str(row[4]).strip().lower()
    if status == "ban":
        return False, "Этот crypto king уже в ban."

    sheet_update_and_refresh(
        SHEET_CRYPTO_KINGS,
        f"E{base_info['row_index']}:F{base_info['row_index']}",
        [["ban", "ban"]]
    )

    row = ensure_row_len(row, 26)
    sync_id = row[12]
    
    if sync_id:
        sync_status_to_basebot(BASEBOT_SHEET_CRYPTO_KINGS, sync_id, "ban")

    issue_info = find_last_king_issue_row(king_name)
    if issue_info:
        mark_issue_row_as_ban(issue_info["row_index"], comment_text)

    invalidate_stats_cache()
    return True, f"Crypto king '{king_name}' переведён в ban."
    
def build_king_search_text(king_name):
    target = str(king_name).strip().lower()
    if not target:
        return None

    found = find_king_in_base_by_name(target)
    if not found:
        return None

    row = found["row"]
    sheet_name = found["sheet_name"]

    source_title = "Crypto king" if sheet_name == SHEET_CRYPTO_KINGS else "Кинг"

    if len(row) < 12:
        row = row + [''] * (12 - len(row))

    name = row[0] or "без названия"
    price = row[2] or "не указана"
    status = row[4] or "не указан"
    for_whom = row[5] or "не указано"
    taken_date = row[6] or "не указана"
    geo = row[7] or "не указано"
    who_took = row[8] or "не указано"
    data_text = get_full_king_data_from_row(row) or ""

    meta_text = (
        f"{source_title}:\n"
        f"Название: {name}\n"
        f"Статус: {status}\n"
        f"Цена: {price}\n"
        f"Гео: {geo}\n"
        f"Дата взятия: {taken_date}\n"
        f"Кто взял: {who_took}\n"
        f"Для кого взял: {for_whom}"
    )

    return {
        "title": source_title,
        "king_name": name,
        "meta_text": meta_text,
        "data_text": data_text
    }
    
def build_stats_text():

    # ---------- КИНГИ ----------
    kings_free = 0
    kings_taken = 0
    kings_ban = 0
    kings_geo_stats = {}

    for source_sheet in [SHEET_KINGS, SHEET_CRYPTO_KINGS]:
        kings_rows = get_sheet_rows_cached(source_sheet)

        for row in kings_rows[1:]:
            if len(row) < 12:
                row = row + [''] * (12 - len(row))

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

    for row in accounts_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        status = str(row[8]).strip().lower()
        target = str(row[9]).strip().lower()

        if target == "ban" or status == "ban":
            accounts_ban += 1
        elif status == "free":
            accounts_free += 1
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

    # ---------- ФП ----------
    fps_rows = get_sheet_rows_cached(SHEET_FPS)

    fps_free = 0
    fps_taken = 0

    for row in fps_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        status = str(row[5]).strip().lower()

        if status == "free":
            fps_free += 1
        elif status == "taken":
            fps_taken += 1

    # ---------- PIXELS ----------
    pixels_rows = get_sheet_rows_cached(SHEET_PIXELS)

    pixels_free = 0
    pixels_taken = 0
    pixels_ban = 0

    for row in pixels_rows[1:]:
        if len(row) < 8:
            row = row + [''] * (8 - len(row))

        status = str(row[3]).strip().lower()

        if status == "free":
            pixels_free += 1
        elif status == "taken":
            pixels_taken += 1
        elif status == "ban":
            pixels_ban += 1

    # ---------- FARM KINGS ----------
    farm_kings_rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    farm_kings_free = 0
    farm_kings_taken = 0
    farm_kings_ban = 0
    farm_kings_geo_stats = {}

    for row in farm_kings_rows[1:]:
        if len(row) < 12:
            row = row + [''] * (12 - len(row))

        status = str(row[4]).strip().lower()
        geo = str(row[7]).strip()

        if status == "free":
            farm_kings_free += 1
            if geo:
                farm_kings_geo_stats[geo] = farm_kings_geo_stats.get(geo, 0) + 1
        elif status == "taken":
            farm_kings_taken += 1
        elif status == "ban":
            farm_kings_ban += 1

    # ---------- FARM BM ----------
    farm_bms_rows = get_sheet_rows_cached(SHEET_FARM_BMS)

    farm_bms_free = 0
    farm_bms_taken = 0

    for row in farm_bms_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        status = str(row[4]).strip().lower()

        if status == "free":
            farm_bms_free += 1
        elif status == "taken":
            farm_bms_taken += 1

    # ---------- FARM FP ----------
    farm_fps_rows = get_sheet_rows_cached(SHEET_FARM_FPS)

    farm_fps_free = 0
    farm_fps_taken = 0

    for row in farm_fps_rows[1:]:
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        status = str(row[5]).strip().lower()

        if status == "free":
            farm_fps_free += 1
        elif status == "taken":
            farm_fps_taken += 1

    geo_lines = []
    for geo, count in sorted(kings_geo_stats.items()):
        geo_lines.append(f"{geo}: {count}")

    if not geo_lines:
        geo_lines.append("нет свободных GEO")

    farm_geo_lines = []
    for geo, count in sorted(farm_kings_geo_stats.items()):
        farm_geo_lines.append(f"{geo}: {count}")

    if not farm_geo_lines:
        farm_geo_lines.append("нет свободных GEO")

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
        "БМы:\n\n"
        f"Свободные: {bms_free}\n"
        f"Выдано: {bms_taken}\n\n"
        "ФП:\n\n"
        f"Свободные: {fps_free}\n"
        f"Выдано: {fps_taken}\n\n"
        "Пиксели:\n\n"
        f"Свободные: {pixels_free}\n"
        f"Выдано: {pixels_taken}\n"
        f"В бане: {pixels_ban}\n\n"
        "Farm kings:\n\n"
        f"Свободные: {farm_kings_free}\n"
        f"Выдано: {farm_kings_taken}\n"
        f"В бане: {farm_kings_ban}\n\n"
        "По GEO:\n\n"
        + "\n".join(farm_geo_lines)
        + "\n\n"
        "Farm BM:\n\n"
        f"Свободные: {farm_bms_free}\n"
        f"Выдано: {farm_bms_taken}\n\n"
        "Farm FP:\n\n"
        f"Свободные: {farm_fps_free}\n"
        f"Выдано: {farm_fps_taken}"
    )

    return text


def send_free_kings(chat_id):
    free_rows = []

    for source_sheet in [SHEET_KINGS, SHEET_CRYPTO_KINGS]:
        rows = get_sheet_rows_cached(source_sheet)

        for row in rows[1:]:
            if len(row) < 12:
                row = row + [''] * (12 - len(row))

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
            def _read_main_data():
                with google_lock:
                    client = get_gspread_client()
                    main_spreadsheet = client.open_by_key(SPREADSHEET_ID)
            
                    return {
                        "accounts": main_spreadsheet.worksheet(SHEET_ACCOUNTS).get_all_values(),
                        "kings": main_spreadsheet.worksheet(SHEET_KINGS).get_all_values(),
                        "crypto_kings": main_spreadsheet.worksheet(SHEET_CRYPTO_KINGS).get_all_values(),
                        "issues": main_spreadsheet.worksheet(SHEET_ISSUES).get_all_values(),
                        "bms": main_spreadsheet.worksheet(SHEET_BMS).get_all_values(),
                        "fps": main_spreadsheet.worksheet(SHEET_FPS).get_all_values(),
                        "pixels": main_spreadsheet.worksheet(SHEET_PIXELS).get_all_values(),
                        "farm_kings": main_spreadsheet.worksheet(SHEET_FARM_KINGS).get_all_values(),
                        "farm_bms": main_spreadsheet.worksheet(SHEET_FARM_BMS).get_all_values(),
                        "farm_fps": main_spreadsheet.worksheet(SHEET_FARM_FPS).get_all_values(),
                    }

            main_data = google_read_with_retry(_read_main_data)

            def _write_backup():
                with google_lock:
                    client = get_gspread_client()
                    backup_spreadsheet = client.open_by_key(BACKUP_SPREADSHEET_ID)
            
                    backup_accounts = backup_spreadsheet.worksheet("backup_accounts")
                    backup_kings = backup_spreadsheet.worksheet("backup_kings")
                    backup_crypto_kings = backup_spreadsheet.worksheet("backup_crypto_kings")
                    backup_issues = backup_spreadsheet.worksheet("backup_issues")
                    backup_bms = backup_spreadsheet.worksheet("backup_bms")
                    backup_fps = backup_spreadsheet.worksheet("backup_fps")
                    backup_pixels = backup_spreadsheet.worksheet("backup_pixels")
                    backup_farm_kings = backup_spreadsheet.worksheet("backup_farm_kings")
                    backup_farm_bms = backup_spreadsheet.worksheet("backup_farm_bms")
                    backup_farm_fps = backup_spreadsheet.worksheet("backup_farm_fps")
            
                    backup_accounts.clear()
                    backup_kings.clear()
                    backup_crypto_kings.clear()
                    backup_issues.clear()
                    backup_bms.clear()
                    backup_fps.clear()
                    backup_pixels.clear()
                    backup_farm_kings.clear()
                    backup_farm_bms.clear()
                    backup_farm_fps.clear()
            
                    if main_data["accounts"]:
                        backup_accounts.append_rows(main_data["accounts"], value_input_option="USER_ENTERED")
            
                    if main_data["kings"]:
                        backup_kings.append_rows(main_data["kings"], value_input_option="USER_ENTERED")
            
                    if main_data["crypto_kings"]:
                        backup_crypto_kings.append_rows(main_data["crypto_kings"], value_input_option="USER_ENTERED")
            
                    if main_data["issues"]:
                        backup_issues.append_rows(main_data["issues"], value_input_option="USER_ENTERED")
            
                    if main_data["bms"]:
                        backup_bms.append_rows(main_data["bms"], value_input_option="USER_ENTERED")
            
                    if main_data["fps"]:
                        backup_fps.append_rows(main_data["fps"], value_input_option="USER_ENTERED")
            
                    if main_data["pixels"]:
                        backup_pixels.append_rows(main_data["pixels"], value_input_option="USER_ENTERED")
            
                    if main_data["farm_kings"]:
                        backup_farm_kings.append_rows(main_data["farm_kings"], value_input_option="USER_ENTERED")
            
                    if main_data["farm_bms"]:
                        backup_farm_bms.append_rows(main_data["farm_bms"], value_input_option="USER_ENTERED")
            
                    if main_data["farm_fps"]:
                        backup_farm_fps.append_rows(main_data["farm_fps"], value_input_option="USER_ENTERED")

            google_write_with_retry(_write_backup)

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

def cache_warmer_loop():
    while True:
        try:
            touch_background_heartbeat()
            refresh_sheet_cache(SHEET_ACCOUNTS)
            refresh_sheet_cache(SHEET_ISSUES)
            refresh_sheet_cache(SHEET_KINGS)
            refresh_sheet_cache(SHEET_CRYPTO_KINGS)
            refresh_sheet_cache(SHEET_BMS)
            refresh_sheet_cache(SHEET_FPS)
            refresh_sheet_cache(SHEET_PIXELS)
            refresh_sheet_cache(SHEET_FARM_KINGS)
            refresh_sheet_cache(SHEET_FARM_BMS)
            refresh_sheet_cache(SHEET_FARM_FPS)
            time.sleep(3)
        except Exception:
            logging.exception("cache_warmer_loop error")
            time.sleep(5)

def run_bot_diagnostics():
    report = []
    ok_count = 0
    fail_count = 0

    def add_result(name, ok, details=""):
        nonlocal ok_count, fail_count
        mark = "✅" if ok else "❌"
        line = f"{mark} {name}"
        if details:
            line += f" — {details}"
        report.append(line)

        if ok:
            ok_count += 1
        else:
            fail_count += 1

    # 1. Telegram
    try:
        if BOT_TOKEN and BASE_URL.startswith("https://api.telegram.org/bot"):
            add_result("Подключение к Telegram настроено", True)
        else:
            add_result("Подключение к Telegram настроено", False, "BOT_TOKEN или BASE_URL заполнены неправильно")
    except Exception as e:
        add_result("Подключение к Telegram настроено", False, str(e))

    # 2. Google auth
    try:
        client = get_gspread_client()
        add_result("Авторизация в Google Sheets", client is not None)
    except Exception as e:
        add_result("Авторизация в Google Sheets", False, str(e))

    # 3. Основная таблица
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        add_result("Основная таблица открывается", True, f"Название: {spreadsheet.title}")
    except Exception as e:
        add_result("Основная таблица открывается", False, str(e))

    # 4. Backup таблица
    try:
        client = get_gspread_client()
        backup_spreadsheet = client.open_by_key(BACKUP_SPREADSHEET_ID)
        add_result("Бэкап таблица открывается", True, f"Название: {backup_spreadsheet.title}")
    except Exception as e:
        add_result("Бэкап таблица открывается", False, str(e))

    # 5. Проверка всех основных листов (без BaseBot)
    sheets_to_check = [
        SHEET_ACCOUNTS,
        SHEET_ISSUES,
        SHEET_KINGS,
        SHEET_CRYPTO_KINGS,
        SHEET_BMS,
        SHEET_FPS,
        SHEET_PIXELS,
        SHEET_FARM_KINGS,
        SHEET_FARM_BMS,
        SHEET_FARM_FPS,
    ]

    for sheet_name in sheets_to_check:
        try:
            sheet = get_sheet(sheet_name)
            rows = sheet.get_all_values()
            add_result(f"Лист '{sheet_name}' доступен", True, f"Строк: {len(rows)}")
        except Exception as e:
            add_result(f"Лист '{sheet_name}' доступен", False, str(e))

    # 6. ENV Octo
    try:
        add_result("OCTO_API_TOKEN задан", bool(str(OCTO_API_TOKEN).strip()))
    except Exception as e:
        add_result("OCTO_API_TOKEN задан", False, str(e))

    try:
        add_result("OCTO_API_BASE задан", bool(str(OCTO_API_BASE).strip()))
    except Exception as e:
        add_result("OCTO_API_BASE задан", False, str(e))

    # 7. Базовые функции
    try:
        parse_date("15/02/2026")
        parse_price("123.45")
        normalize_numeric_for_sheet("100")
        normalize_gmt_value("GMT +3")
        normalize_currency_value("GBP_GB")
        extract_digits("ID пикселя: 2426379717814486")
        extract_pixel_name_from_data("Имя пикселя: px6\nID пикселя: 123")
        add_result("Базовые функции работают", True)
    except Exception as e:
        add_result("Базовые функции работают", False, str(e))

    # 8. Поисковые функции
    try:
        get_free_king_geos()
        get_free_crypto_king_geos()
        count_free_bms()
        count_free_farm_bms()
        count_free_fp_in_warehouse("Выдать фп 95 бот")
        add_result("Поисковые функции работают", True)
    except Exception as e:
        add_result("Поисковые функции работают", False, str(e))

    # 9. Статистика
    try:
        build_stats_text()
        add_result("Общая статистика собирается", True)
    except Exception as e:
        add_result("Общая статистика собирается", False, str(e))

    try:
        build_manager_stats_summary_text("test_user")
        build_manager_stats_text("test_user")
        add_result("Статистика accounts собирается", True)
    except Exception as e:
        add_result("Статистика accounts собирается", False, str(e))

    try:
        build_farmer_stats_summary_text("test_user")
        build_farmer_stats_text("test_user")
        add_result("Статистика farmers собирается", True)
    except Exception as e:
        add_result("Статистика farmers собирается", False, str(e))

    # 10. Структура таблиц
    try:
        structure_result = run_sheet_structure_checks()
        report.extend(structure_result["report"])
        ok_count += structure_result["ok_count"]
        fail_count += structure_result["fail_count"]
    except Exception as e:
        add_result("Проверка структуры таблиц", False, str(e))

    # 11. Дубли
    try:
        duplicates_result = run_duplicates_checks()
        report.extend(duplicates_result["report"])
        ok_count += duplicates_result["ok_count"]
        fail_count += duplicates_result["fail_count"]
    except Exception as e:
        add_result("Проверка дублей ID / BM / pixel ID", False, str(e))

    # 12. Склады ФП
    try:
        rows = get_sheet_rows_cached(SHEET_FPS)
        warehouses = sorted({
            str(row[4]).strip()
            for row in rows[1:]
            if len(row) > 4 and str(row[4]).strip()
        }, key=extract_warehouse_sort_key)

        add_result("Склады ФП читаются", True, f"Складов: {len(warehouses)}")
    except Exception as e:
        add_result("Склады ФП читаются", False, str(e))

    # 13. Склады farm FP
    try:
        rows = get_sheet_rows_cached(SHEET_FARM_FPS)
        warehouses = sorted({
            str(row[4]).strip()
            for row in rows[1:]
            if len(row) > 4 and str(row[4]).strip()
        }, key=extract_warehouse_sort_key)

        add_result("Склады farm FP читаются", True, f"Складов: {len(warehouses)}")
    except Exception as e:
        add_result("Склады farm FP читаются", False, str(e))

    # 14. Crypto bulk helpers
    try:
        build_crypto_bulk_result_text([], "test")
        send_crypto_bulk_followup_messages
        finish_crypto_kings_bulk
        process_crypto_bulk_proxy_step
        add_result("Bulk crypto функции подключены", True)
    except Exception as e:
        add_result("Bulk crypto функции подключены", False, str(e))

    # 15. Проверка state helper под long proxy wait
    try:
        test_state = {"mode": "test"}
        set_state_with_custom_ttl(999999, test_state, 60)
        loaded = get_state(999999)
        clear_state(999999)
        add_result("Custom TTL состояний работает", loaded.get("mode") == "test")
    except Exception as e:
        add_result("Custom TTL состояний работает", False, str(e))

    report.append("")
    report.append(f"Итого: ✅ {ok_count} | ❌ {fail_count}")

    return "\n".join(report)

def run_bot_diagnostics_async(chat_id):
    global bot_diagnostics_running

    acquired = bot_diagnostics_lock.acquire(blocking=False)
    if not acquired:
        tg_send_message(chat_id, "Проверка бота уже запущена. Дождись завершения.")
        return

    bot_diagnostics_running = True

    def worker():
        global bot_diagnostics_running
        try:
            tg_send_message(chat_id, "Запускаю полную проверку бота...")
            result = run_bot_diagnostics()
            tg_send_long_message(chat_id, result)
            send_admin_menu(chat_id, "Меню Admin:")
        except Exception as e:
            logging.exception("run_bot_diagnostics_async crashed")
            tg_send_message(chat_id, f"Ошибка проверки бота:\n{e}")
        finally:
            bot_diagnostics_running = False
            try:
                bot_diagnostics_lock.release()
            except Exception:
                pass

    threading.Thread(target=worker, daemon=True).start()

def run_sheet_structure_checks():
    report = []
    ok_count = 0
    fail_count = 0

    def add_result(name, ok, details=""):
        nonlocal ok_count, fail_count
        mark = "✅" if ok else "❌"
        line = f"{mark} {name}"
        if details:
            line += f" — {details}"
        report.append(line)
        if ok:
            ok_count += 1
        else:
            fail_count += 1

    checks = [
        (SHEET_ACCOUNTS, 14, "Лички"),
        (SHEET_ISSUES, 9, "Простые лички 26"),
        (SHEET_KINGS, 12, "Кинги"),
        (SHEET_CRYPTO_KINGS, 12, "Крипта кинги"),
        (SHEET_BMS, 9, "БМы"),
        (SHEET_FPS, 9, "ФП"),
        (SHEET_PIXELS, 8, "Пиксели"),
        (SHEET_FARM_KINGS, 12, "Фарм кинги"),
        (SHEET_FARM_BMS, 9, "Фарм БМ"),
        (SHEET_FARM_FPS, 9, "Фарм ФП"),
    ]

    for sheet_name, min_cols, title in checks:
        try:
            rows = get_sheet_rows_cached(sheet_name, force=True)

            if not rows:
                add_result(f"{title}: лист не пустой", False, "Лист пустой")
                continue

            header = rows[0]
            if len(header) < min_cols:
                add_result(
                    f"{title}: структура колонок",
                    False,
                    f"Ожидалось минимум {min_cols} колонок, сейчас {len(header)}"
                )
            else:
                add_result(
                    f"{title}: структура колонок",
                    True,
                    f"Колонок: {len(header)}"
                )

            bad_rows = 0
            for row in rows[1:]:
                if len(row) > 0 and len(row) < min_cols:
                    bad_rows += 1

            if bad_rows > 0:
                add_result(
                    f"{title}: короткие строки",
                    False,
                    f"Найдено строк с нехваткой колонок: {bad_rows}"
                )
            else:
                add_result(f"{title}: короткие строки", True)

        except Exception as e:
            add_result(f"{title}: проверка структуры", False, str(e))

    return {
        "ok_count": ok_count,
        "fail_count": fail_count,
        "report": report
    }

def run_duplicates_checks():
    report = []
    ok_count = 0
    fail_count = 0

    def add_result(name, ok, details=""):
        nonlocal ok_count, fail_count
        mark = "✅" if ok else "❌"
        line = f"{mark} {name}"
        if details:
            line += f" — {details}"
        report.append(line)

        if ok:
            ok_count += 1
        else:
            fail_count += 1

    # 1. Дубли ID личек (База_личек, колонка A = индекс 0)
    try:
        acc_dupes = find_duplicate_values_in_sheet(
            sheet_name=SHEET_ACCOUNTS,
            value_index=0,
            min_cols=14
        )

        if acc_dupes:
            preview = []
            for value, rows in list(acc_dupes.items())[:10]:
                preview.append(f"{value} (строки: {', '.join(map(str, rows[:10]))})")

            add_result(
                "Проверка дублей ID личек",
                False,
                f"Найдено дублей: {len(acc_dupes)}; " + " | ".join(preview)
            )
        else:
            add_result("Проверка дублей ID личек", True)
    except Exception as e:
        add_result("Проверка дублей ID личек", False, str(e))

    # 2. Дубли BM ID (База_БМ, колонка A = индекс 0)
    try:
        bm_dupes = find_duplicate_values_in_sheet(
            sheet_name=SHEET_BMS,
            value_index=0,
            min_cols=9
        )

        if bm_dupes:
            preview = []
            for value, rows in list(bm_dupes.items())[:10]:
                preview.append(f"{value} (строки: {', '.join(map(str, rows[:10]))})")

            add_result(
                "Проверка дублей BM ID",
                False,
                f"Найдено дублей: {len(bm_dupes)}; " + " | ".join(preview)
            )
        else:
            add_result("Проверка дублей BM ID", True)
    except Exception as e:
        add_result("Проверка дублей BM ID", False, str(e))

    # 3. Дубли pixel ID (База_пикселей, ID берется из H колонки / data_text)
    try:
        rows = get_sheet_rows_cached(SHEET_PIXELS, force=True)

        seen = {}
        pixel_dupes = {}

        for row_index, row in enumerate(rows[1:], start=2):
            if len(row) < 8:
                row = row + [''] * (8 - len(row))

            pixel_id = extract_pixel_id_from_data(row[7])

            if not pixel_id:
                continue

            if pixel_id in seen:
                if pixel_id not in pixel_dupes:
                    pixel_dupes[pixel_id] = [seen[pixel_id]]
                pixel_dupes[pixel_id].append(row_index)
            else:
                seen[pixel_id] = row_index

        if pixel_dupes:
            preview = []
            for value, rows_list in list(pixel_dupes.items())[:10]:
                preview.append(f"{value} (строки: {', '.join(map(str, rows_list[:10]))})")

            add_result(
                "Проверка дублей pixel ID",
                False,
                f"Найдено дублей: {len(pixel_dupes)}; " + " | ".join(preview)
            )
        else:
            add_result("Проверка дублей pixel ID", True)
    except Exception as e:
        add_result("Проверка дублей pixel ID", False, str(e))

    return {
        "ok_count": ok_count,
        "fail_count": fail_count,
        "report": report
    }

def find_duplicate_values_in_sheet(sheet_name, value_index, min_cols=1, normalize_func=None, skip_header=True):
    rows = get_sheet_rows_cached(sheet_name, force=True)

    seen = {}
    duplicates = {}

    start_row_index = 2 if skip_header else 1
    data_rows = rows[1:] if skip_header else rows

    for offset, row in enumerate(data_rows, start=start_row_index):
        if len(row) < min_cols:
            row = row + [''] * (min_cols - len(row))

        raw_value = row[value_index] if len(row) > value_index else ""
        value = str(raw_value).strip()

        if normalize_func:
            value = normalize_func(value)

        if not value:
            continue

        if value in seen:
            if value not in duplicates:
                duplicates[value] = [seen[value]]
            duplicates[value].append(offset)
        else:
            seen[value] = offset

    return duplicates

def find_row_in_sheet_by_sync_id(sheet_name, sync_id, sync_col_index=0, basebot=False):
    if basebot:
        rows = basebot_get_all_rows(sheet_name)
    else:
        rows = get_sheet_rows_cached(sheet_name, force=True)

    target = str(sync_id or "").strip()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) <= sync_col_index:
            continue

        if str(row[sync_col_index]).strip() == target:
            return {
                "row_index": idx,
                "row": row
            }

    return None

def sync_status_to_basebot(basebot_sheet_name, sync_id, new_status):
    if basebot_sheet_name in [BASEBOT_SHEET_KINGS, BASEBOT_SHEET_CRYPTO_KINGS, BASEBOT_SHEET_FARM_KINGS]:
        sync_col_index = BASEBOT_SYNC_COL_KINGS   # H
        status_col = "C"
    elif basebot_sheet_name in [BASEBOT_SHEET_BMS, BASEBOT_SHEET_FARM_BMS]:
        sync_col_index = BASEBOT_SYNC_COL_BMS     # F
        status_col = "D"
    elif basebot_sheet_name == BASEBOT_SHEET_PIXELS:
        sync_col_index = BASEBOT_SYNC_COL_PIXELS  # E
        status_col = "C"
    else:
        return False

    found = find_row_in_sheet_by_sync_id(
        sheet_name=basebot_sheet_name,
        sync_id=sync_id,
        sync_col_index=sync_col_index,
        basebot=True
    )

    if not found:
        return False

    row_index = found["row_index"]
    basebot_update_range(basebot_sheet_name, f"{status_col}{row_index}", [[new_status]])
    return True

def tg_send_inline_message_parts(chat_id, message_parts, inline_buttons=None):
    parts = [str(x).strip() for x in (message_parts or []) if str(x).strip()]
    if not parts:
        return

    for part in parts[:-1]:
        tg_send_message(chat_id, part)

    if inline_buttons:
        tg_send_inline_message(chat_id, parts[-1], inline_buttons)
    else:
        tg_send_message(chat_id, parts[-1])

def tg_send_long_message(chat_id, text, chunk_size=3500):
    text = str(text or "").strip()
    if not text:
        return

    while text:
        part = text[:chunk_size]

        if len(text) > chunk_size:
            split_pos = part.rfind("\n")
            if split_pos > 500:
                part = part[:split_pos]

        tg_send_message(chat_id, part.strip())
        text = text[len(part):].strip()

TELEGRAM_MESSAGE_LIMIT = 4096
SAFE_EDIT_LIMIT = 3500

def octo_headers():
    if not OCTO_API_TOKEN:
        raise RuntimeError("OCTO_API_TOKEN не задан")
    return {
        "X-Octo-Api-Token": OCTO_API_TOKEN,
        "Content-Type": "application/json",
    }


def octo_extract_unique_warehouses(added_items):
    unique = []
    seen = set()

    for item in added_items:
        warehouse = str(item.get("warehouse", "")).strip()
        if not warehouse or warehouse in seen:
            continue
        seen.add(warehouse)
        unique.append(warehouse)

    return unique


import re

def parse_proxy_input(text):
    raw = str(text or "").strip()

    match = re.match(
        r'^(?:(socks5|http|https)://)?([^:@]+):([^:@]+)@([^:]+):(\d+)$',
        raw,
        re.IGNORECASE
    )
    if match:
        ptype, login, password, host, port = match.groups()
        return {
            "type": (ptype or "socks5").lower(),
            "host": host.strip(),
            "port": int(port),
            "login": login.strip(),
            "password": password.strip(),
        }

    match = re.match(
        r'^(?:(socks5|http|https)://)?([^:]+):(\d+)$',
        raw,
        re.IGNORECASE
    )
    if match:
        ptype, host, port = match.groups()
        return {
            "type": (ptype or "socks5").lower(),
            "host": host.strip(),
            "port": int(port),
            "login": "",
            "password": "",
        }

    parts = [x.strip() for x in raw.split(":")]

    if len(parts) == 4:
        host, port, login, password = parts
        return {
            "type": "socks5",
            "host": host,
            "port": int(port),
            "login": login,
            "password": password
        }

    if len(parts) == 2:
        host, port = parts
        return {
            "type": "socks5",
            "host": host,
            "port": int(port),
            "login": "",
            "password": ""
        }

    return None

def octo_find_profile_by_title(profile_title):
    logging.info("OCTO_FIND_V3_RUNNING")
    headers = {
        "X-Octo-Api-Token": OCTO_API_TOKEN,
        "Content-Type": "application/json",
    }

    target = str(profile_title or "").strip().lower()

    for page in range(0, 10):
        url = f"{OCTO_API_BASE}/profiles?page={page}&page_len=100&fields=title"

        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()

        data = resp.json()

        items = []
        if isinstance(data, dict):
            if isinstance(data.get("data"), list):
                items = data["data"]
            elif isinstance(data.get("profiles"), list):
                items = data["profiles"]
            elif isinstance(data.get("items"), list):
                items = data["items"]
            elif isinstance(data.get("list"), list):
                items = data["list"]
        elif isinstance(data, list):
            items = data

        for item in items:
            title_val = str(item.get("title", "")).strip().lower()
            name_val = str(item.get("name", "")).strip().lower()

            if title_val == target or name_val == target:
                return item

        if not items:
            break

    return None

def octo_debug_tag_next_warehouse(next_warehouse_name, tag_name):
    result_lines = []

    try:
        result_lines.append(f"START next_warehouse_name={next_warehouse_name}")
        result_lines.append(f"START tag_name={tag_name}")

        warehouse_name = str(next_warehouse_name or "").strip()
        tag_name = str(tag_name or "").strip()

        result_lines.append(f"warehouse_name={warehouse_name}")
        result_lines.append(f"tag_name={tag_name}")

        if not warehouse_name or not tag_name:
            result_lines.append("ERROR: warehouse_name or tag_name empty")
            return "\n".join(result_lines)

        result_lines.append("before octo_find_profile_by_title")
        profile = octo_find_profile_by_title(warehouse_name)
        result_lines.append(f"profile found={bool(profile)}")

        if not profile:
            result_lines.append(f"NOT FOUND profile '{warehouse_name}'")
            return "\n".join(result_lines)

        profile_uuid = str(profile.get("uuid") or profile.get("id") or "").strip()
        result_lines.append(f"profile_uuid={profile_uuid}")

        result_lines.append("before octo_update_profile_tags_by_title")
        ok, msg = octo_update_profile_tags_by_title(
            profile_title=warehouse_name,
            tags_to_add=[tag_name]
        )
        result_lines.append(f"update_result ok={ok} msg={msg}")

        return "\n".join(result_lines)

    except Exception as e:
        logging.exception("octo_debug_tag_next_warehouse crashed")
        result_lines.append(f"EXCEPTION: {e}")
        return "\n".join(result_lines)

def octo_debug_list_profiles():
    headers = {
        "X-Octo-Api-Token": OCTO_API_TOKEN,
        "Content-Type": "application/json",
    }

    url = f"{OCTO_API_BASE}/profiles?page=0&page_len=100&fields=title"
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    data = resp.json()

    items = []
    if isinstance(data, dict):
        if isinstance(data.get("data"), list):
            items = data["data"]
        elif isinstance(data.get("profiles"), list):
            items = data["profiles"]
        elif isinstance(data.get("items"), list):
            items = data["items"]
        elif isinstance(data.get("list"), list):
            items = data["list"]
    elif isinstance(data, list):
        items = data

    lines = [f"Всего на странице: {len(items)}"]

    for i, item in enumerate(items[:20], start=1):
        title_val = str(item.get("title", "")).strip()
        name_val = str(item.get("name", "")).strip()
        uuid_val = str(item.get("uuid", item.get("id", ""))).strip()

        lines.append(
            f"{i}. title='{title_val}' | name='{name_val}' | id='{uuid_val}'"
        )

    return "\n".join(lines)

def normalize_octo_title(text):
    text = str(text or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text

def octo_update_profile_tags_by_title(profile_title, tags_to_add):
    profile_title = str(profile_title or "").strip()

    if not profile_title:
        return False, "Не указано название Octo профиля"

    if not isinstance(tags_to_add, list):
        tags_to_add = [str(tags_to_add).strip()] if str(tags_to_add).strip() else []

    tags_to_add = [str(x).strip() for x in tags_to_add if str(x).strip()]

    profile = octo_find_profile_by_title(profile_title)
    if not profile:
        return False, f"Octo профиль '{profile_title}' не найден"

    profile_uuid = str(profile.get("uuid") or profile.get("id") or "").strip()
    if not profile_uuid:
        return False, f"У профиля '{profile_title}' не найден id/uuid"

    headers = {
        "X-Octo-Api-Token": OCTO_API_TOKEN,
        "Content-Type": "application/json",
    }

    # всегда сохраняем базовые теги
    base_tags = ["Sido", "corby"]

    merged_tags = []
    for tag in base_tags + tags_to_add:
        tag = str(tag).strip()
        if tag and tag not in merged_tags:
            merged_tags.append(tag)

    url = f"{OCTO_API_BASE}/profiles/{profile_uuid}"
    payload = {
        "tags": merged_tags
    }

    resp = requests.patch(url, json=payload, headers=headers, timeout=60)

    if resp.status_code >= 400:
        try:
            err = resp.json()
        except Exception:
            err = resp.text
        return False, f"Octo API error {resp.status_code}: {err}"

    return True, (
        f"Тег(и) {', '.join(tags_to_add)} поставлены на профиль '{profile_title}'. "
        f"Итоговые теги: {', '.join(merged_tags)}"
    )

def tag_next_octo_fp_warehouse(next_warehouse_name, tag_name):
    warehouse_name = str(next_warehouse_name or "").strip()
    tag_name = str(tag_name or "").strip()

    if not warehouse_name or not tag_name:
        return False, "Не указано название склада или тег"

    try:
        return octo_update_profile_tags_by_title(
            profile_title=warehouse_name,
            tags_to_add=[tag_name]
        )
    except Exception as e:
        logging.exception("tag_next_octo_fp_warehouse crashed")
        return False, str(e)

def extract_octo_profile_uuid(octo_response):
    if not isinstance(octo_response, dict):
        return ""

    candidates = []

    candidates.append(octo_response.get("uuid"))
    candidates.append(octo_response.get("id"))

    data = octo_response.get("data")
    if isinstance(data, dict):
        candidates.append(data.get("uuid"))
        candidates.append(data.get("id"))

    for value in candidates:
        value = str(value or "").strip()
        if value:
            return value

    return ""

def build_octo_profile_payload(profile_name, proxy_data):
    payload = {
        "title": profile_name,
        "proxy": {
            "type": proxy_data.get("type", "socks5"),
            "host": proxy_data["host"],
            "port": proxy_data["port"],
            "login": proxy_data.get("login", ""),
            "password": proxy_data.get("password", "")
        }
    }

    if OCTO_FP_TEMPLATE_ID:
        payload["template_id"] = OCTO_FP_TEMPLATE_ID
    else:
        payload["tags"] = ["Sido", "corby"]
        payload["start_pages"] = ["https://www.facebook.com"]
        payload["bookmarks"] = [
            {
                "url": "https://www.facebook.com"
            },
            {
                "url": "https://2fa.cn"
            }
        ]
        payload["fingerprint"] = {
            "os": "win"
        }

    return payload

def build_crypto_king_octo_payload(profile_name, parsed, proxy_data=None):
    payload = {
        "title": profile_name,
        "tags": [OCTO_TAG_SIDO, OCTO_TAG_CORBY, OCTO_TAG_ACCOUNT_MANAGERS],
        "description": build_crypto_king_octo_description(parsed),
        "fingerprint": {
            "os": "win"
        },
        "start_pages": ["https://www.facebook.com"],
        "bookmarks": [
            {"url": "https://www.facebook.com"},
            {"url": "https://adsmanager.facebook.com"},
            {"url": "https://2fa.cn"}
        ]
    }

    if proxy_data:
        payload["proxy"] = {
            "type": proxy_data.get("type", "socks5"),
            "host": proxy_data["host"],
            "port": proxy_data["port"],
            "login": proxy_data.get("login", ""),
            "password": proxy_data.get("password", "")
        }

    if OCTO_FP_TEMPLATE_ID:
        payload["template_id"] = OCTO_FP_TEMPLATE_ID

    return payload

def build_farm_king_octo_payload(profile_name, parsed, proxy_data=None):
    payload = {
        "title": profile_name,
        "tags": [OCTO_TAG_SIDO, OCTO_TAG_CORBY, OCTO_TAG_FARMERS],
        "description": build_crypto_king_octo_description(parsed),
        "fingerprint": {
            "os": "win"
        },
        "start_pages": ["https://www.facebook.com"],
        "bookmarks": [
            {"url": "https://www.facebook.com"},
            {"url": "https://adsmanager.facebook.com"},
            {"url": "https://2fa.cn"}
        ]
    }

    if proxy_data:
        payload["proxy"] = {
            "type": proxy_data.get("type", "socks5"),
            "host": proxy_data["host"],
            "port": proxy_data["port"],
            "login": proxy_data.get("login", ""),
            "password": proxy_data.get("password", "")
        }

    if OCTO_FP_TEMPLATE_ID:
        payload["template_id"] = OCTO_FP_TEMPLATE_ID

    return payload


def ensure_octo_profile_for_farm_king(profile_name, parsed, proxy_data=None):
    existing = octo_find_profile_by_title(profile_name)
    if existing:
        try:
            octo_update_profile_tags_by_title(profile_name, [OCTO_TAG_FARMERS])
        except Exception:
            logging.exception("ensure_octo_profile_for_farm_king tag update failed for existing profile")
        return True, existing

    payload = build_farm_king_octo_payload(
        profile_name=profile_name,
        parsed=parsed,
        proxy_data=proxy_data
    )

    result = octo_create_profile(payload)

    try:
        octo_update_profile_tags_by_title(profile_name, [OCTO_TAG_FARMERS])
    except Exception:
        logging.exception("ensure_octo_profile_for_farm_king tag update failed after create")

    return True, result


def ensure_octo_profile_with_retry(ensure_func, profile_name, parsed, proxy_data=None, retries=3, delay=2):
    last_error = ""

    for attempt in range(1, retries + 1):
        try:
            ok, result = ensure_func(
                profile_name=profile_name,
                parsed=parsed,
                proxy_data=proxy_data
            )
            if ok:
                return True, result
            last_error = str(result)
        except Exception as e:
            logging.exception(
                f"ensure_octo_profile_with_retry crashed attempt={attempt} profile_name={profile_name}"
            )
            last_error = str(e)

        if attempt < retries:
            time.sleep(delay)

    return False, last_error or "Не удалось создать Octo профиль"

def normalize_crypto_cookies_for_import(cookies_raw):
    raw = str(cookies_raw or "").strip()
    if not raw:
        return None

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return {
                "format": "json",
                "text": json.dumps(parsed, ensure_ascii=False)
            }
    except Exception:
        pass

    return {
        "format": "txt",
        "text": raw
    }


def save_crypto_cookies_temp_file(profile_name, cookies_payload):
    safe_name = "".join(
        ch if ch.isalnum() or ch in ("_", "-", ".") else "_"
        for ch in str(profile_name or "profile")
    )
    suffix = ".json" if cookies_payload["format"] == "json" else ".txt"

    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=suffix,
        prefix=f"{safe_name}_cookies_",
        delete=False
    )
    tmp.write(cookies_payload["text"])
    tmp.flush()
    tmp.close()
    return tmp.name


def extract_octo_profile_uuid_from_result(result):
    if not isinstance(result, dict):
        return ""

    data = result.get("data")
    if isinstance(data, dict):
        return str(data.get("uuid") or data.get("id") or "").strip()

    return str(result.get("uuid") or result.get("id") or "").strip()


def try_import_crypto_king_cookies(profile_uuid, cookies_payload):
    profile_uuid = str(profile_uuid or "").strip()
    if not profile_uuid:
        return False, "profile_uuid пустой"

    if not cookies_payload or not cookies_payload.get("text"):
        return False, "cookies пустые"

    temp_path = save_crypto_cookies_temp_file(profile_uuid, cookies_payload)

    headers = {
        "X-Octo-Api-Token": OCTO_API_TOKEN,
    }

    # ВАЖНО:
    # если этот endpoint у тебя не совпадёт с реальным Octo API,
    # просто замени URL, а остальная логика уже готова.
    url = f"{OCTO_API_BASE}/profiles/{profile_uuid}/cookies/import"

    with open(temp_path, "rb") as f:
        files = {
            "file": (
                os.path.basename(temp_path),
                f.read(),
                "application/json" if cookies_payload["format"] == "json" else "text/plain"
            )
        }
        resp = requests.post(url, headers=headers, files=files, timeout=120)

    if resp.status_code >= 400:
        try:
            err = resp.json()
        except Exception:
            err = resp.text
        return False, f"Octo cookie import error {resp.status_code}: {err}"

    return True, "cookies imported"

def ensure_octo_profile_for_crypto_king(profile_name, parsed, proxy_data=None):
    existing = octo_find_profile_by_title(profile_name)
    if existing:
        return True, existing

    payload = build_crypto_king_octo_payload(
        profile_name=profile_name,
        parsed=parsed,
        proxy_data=proxy_data
    )

    result = octo_create_profile(payload)
    return True, result

def octo_create_profile(payload):
    headers = {
        "X-Octo-Api-Token": OCTO_API_TOKEN,
        "Content-Type": "application/json",
    }

    url = f"{OCTO_API_BASE}/profiles"

    logging.info(f"OCTO CREATE URL: {url}")
    logging.info(f"OCTO CREATE PAYLOAD: {json.dumps(payload, ensure_ascii=False)}")

    resp = requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=60
    )

    logging.info(f"OCTO STATUS: {resp.status_code}")
    logging.info(f"OCTO RESPONSE: {resp.text}")

    if resp.status_code >= 400:
        raise RuntimeError(f"Octo API error {resp.status_code}: {resp.text}")

    return resp.json()

def ensure_octo_profile_for_warehouse(profile_name, proxy_data):
    existing = octo_find_profile_by_title(profile_name)
    if existing:
        return True, existing

    payload = build_octo_profile_payload(profile_name, proxy_data)
    result = octo_create_profile(payload)
    return True, result

def tg_send_split_text(chat_id, text, chunk_size=3500):
    text = str(text or "").strip()
    if not text:
        return

    while text:
        part = text[:chunk_size]

        if len(text) > chunk_size:
            split_pos = part.rfind("\n")
            if split_pos > 300:
                part = part[:split_pos]

        tg_send_message(chat_id, part.strip())
        text = text[len(part):].strip()

def safe_replace_stats_message(chat_id, message_id, full_text, back_callback_data):
    text = str(full_text or "").strip()

    # если текст влезает — редактируем текущее сообщение
    if len(text) <= SAFE_EDIT_LIMIT:
        tg_edit_message_text(
            chat_id,
            message_id,
            text,
            inline_buttons=[[{
                "text": "Назад",
                "callback_data": back_callback_data
            }]]
        )
        return

    # если текст длинный — старое сообщение заменяем на короткое
    tg_edit_message_text(
        chat_id,
        message_id,
        "Полная статистика слишком большая, поэтому отправлена ниже отдельными сообщениями.",
        inline_buttons=[[{
            "text": "Назад",
            "callback_data": back_callback_data
        }]]
    )

    # а полную статистику отправляем кусками
    tg_send_long_message(chat_id, text)

def build_all_users_stats_messages():
    messages = []

    messages.append("Статистика всех")

    if ACCOUNTS_USERS:
        messages.append("=== ACCOUNTS ===")
        for user_id, username in ACCOUNTS_USERS.items():
            messages.append(build_manager_stats_text(username))
    else:
        messages.append("=== ACCOUNTS ===")
        messages.append("Нет accounts пользователей.")

    if FARMERS_USERS:
        messages.append("=== FARMERS ===")
        for user_id, username in FARMERS_USERS.items():
            messages.append(build_farmer_stats_text(username))
    else:
        messages.append("=== FARMERS ===")
        messages.append("Нет farmers пользователей.")

    return messages

def send_all_users_stats(chat_id):
    tg_send_message(chat_id, "Статистика всех")

    if ACCOUNTS_USERS:
        tg_send_message(chat_id, "=== ACCOUNTS ===")
        for user_id, username in ACCOUNTS_USERS.items():
            summary_text = build_manager_stats_summary_text(username)

            tg_send_inline_message(
                chat_id,
                summary_text,
                [[{
                    "text": "Полная статистика",
                    "callback_data": f"fullstats_accounts:{username}"
                }]]
            )

    if FARMERS_USERS:
        tg_send_message(chat_id, "=== FARMERS ===")
        for user_id, username in FARMERS_USERS.items():
            summary_text = build_farmer_stats_summary_text(username)

            tg_send_inline_message(
                chat_id,
                summary_text,
                [[{
                    "text": "Полная статистика",
                    "callback_data": f"fullstats_farmers:{username}"
                }]]
            )
            
# =========================
# MESSAGE HANDLER
# =========================            
def handle_message(msg):
    try:
        logging.info(f"HANDLE_MESSAGE START text={msg.get('text')} user_id={msg['from']['id']}")
        cleanup_states()
        touch_request_heartbeat()

        chat_id = msg["chat"]["id"]
        user_id = msg["from"]["id"]

        text = str(msg.get("text", "")).strip()
        is_menu_click = text in {
            MENU_ACCOUNTS, MENU_FARMERS, MENU_STATS, MENU_ADMIN,
            SUBMENU_ACCOUNTS_MAIN, SUBMENU_BACK_MAIN, BTN_BACK_TO_MENU,
            MENU_KINGS, MENU_BMS, MENU_FPS, MENU_PIXELS,
            FARM_MENU_KING, FARM_MENU_BM, FARM_MENU_FP,
            BTN_BACK_TO_FARMERS, BTN_BACK_FROM_ADMIN, BTN_BACK_FROM_ACCOUNTANTS,
            BTN_BACK_FROM_ADMIN_FARMERS, MENU_CANCEL
        }

        now = time.time()
        with user_action_lock:
            last = last_user_action.get(user_id, 0)

            if not is_menu_click and now - last < ACTION_COOLDOWN:
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

        if text.startswith("/octotagdebug "):
            try:
                payload = text[len("/octotagdebug "):].strip()
        
                if "|" not in payload:
                    tg_send_message(chat_id, "Формат:\n/octotagdebug название склада|тег")
                    return
        
                warehouse_name, tag_name = [x.strip() for x in payload.split("|", 1)]
        
                debug_result = octo_debug_tag_next_warehouse(warehouse_name, tag_name)
                tg_send_long_message(chat_id, debug_result)
            except Exception as e:
                tg_send_long_message(chat_id, f"octotagdebug error:\n{e}")
            return

        if text.startswith("/octofulltags "):
            try:
                profile_name = text[len("/octofulltags "):].strip()
                profile = octo_find_profile_by_title(profile_name)
        
                if not profile:
                    tg_send_message(chat_id, f"Профиль '{profile_name}' не найден")
                    return
        
                profile_uuid = str(profile.get("uuid") or profile.get("id") or "").strip()
                full_profile = octo_get_profile_by_uuid(profile_uuid)
        
                tg_send_long_message(chat_id, str(full_profile))
            except Exception as e:
                tg_send_long_message(chat_id, f"octofulltags error:\n{e}")
            return

        if text.startswith("/parsecryptoking "):
            raw_value = text[len("/parsecryptoking "):].strip()
            parsed = parse_crypto_king_raw_data(raw_value)
        
            answer = (
                "PARSED CRYPTO KING\n\n"
                f"GEO: {parsed.get('geo')}\n"
                f"FB Login: {parsed.get('fb_login')}\n"
                f"FB Password: {parsed.get('fb_password')}\n"
                f"Email: {parsed.get('email')}\n"
                f"Email Password: {parsed.get('email_password')}\n"
                f"Service: {parsed.get('service')}\n"
                f"2FA: {parsed.get('twofa')}\n"
                f"User-Agent: {parsed.get('user_agent')}\n"
                f"Cookies JSON found: {bool(parsed.get('cookies_json'))}\n"
                f"Cookies links: {parsed.get('cookies_links')}\n"
                f"Docs links: {parsed.get('docs_links')}\n"
                f"BM links: {parsed.get('bm_links')}\n"
                f"Extra pairs: {parsed.get('extra_pairs')}\n"
            )
        
            tg_send_long_message(chat_id, answer)
            return

        if text == "/octodebug":
            try:
                result = octo_debug_list_profiles()
                tg_send_long_message(chat_id, result)
            except Exception as e:
                tg_send_long_message(chat_id, f"Octo debug error:\n{e}")
            return

        if text == MENU_CANCEL:
            clear_state(user_id)
            send_main_menu(chat_id, "Действие отменено.", user_id=user_id)
            return

        if text == BTN_BACK_STEP:
            prev_state = go_back_state(user_id)

            if not prev_state:
                send_main_menu(chat_id, "Назад идти некуда.", user_id=user_id)
                return

            mode = prev_state.get("mode", "")

            if mode == "awaiting_issue_department":
                send_department_menu(chat_id, "Выбери для кого личка:")
                return

            if mode == "awaiting_issue_for_whom":
                send_person_menu(chat_id, prev_state.get("issue_department"))
                return

            if mode == "awaiting_issue_account_number":
                send_text_input_prompt(chat_id, "Теперь напиши номер лички или несколько номеров, каждый с новой строки.")
                return

            if mode == "awaiting_quick_issue_department":
                send_department_menu(chat_id, "Выбери для кого быстро выдать личку:")
                return

            if mode == "awaiting_quick_issue_for_whom":
                send_person_menu(chat_id, prev_state.get("issue_department"))
                return

            if mode == KING_OCTO_MODE_COUNT:
                tg_send_message(chat_id, "Сколько king нужно?", [
                    [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
                ])
                return

            if mode == KING_OCTO_MODE_GEO:
                send_king_geo_options(chat_id)
                return

            if mode == KING_OCTO_MODE_PRICE:
                send_king_price_options(chat_id, prev_state.get("king_geo", ""))
                return

            if mode == KING_OCTO_MODE_DEPARTMENT:
                send_department_menu(chat_id, "Выбери для кого king:")
                return

            if mode == KING_OCTO_MODE_FOR_WHOM:
                send_person_menu(chat_id, prev_state.get("king_department"))
                return

            if mode == KING_OCTO_MODE_NAME:
                send_text_input_prompt(chat_id, "Какое название будет у king?")
                return

            if mode == KING_OCTO_MODE_BULK_NAMES:
                tg_send_message(
                    chat_id,
                    f"Пришли {prev_state.get('kings_count_requested', 0)} названий для king.\nКаждое название с новой строки."
                )
                return

            if mode == "awaiting_king_geo":
                send_king_geo_options(chat_id)
                return

            if mode == "awaiting_king_price":
                send_king_price_options(chat_id, prev_state.get("king_geo", ""))
                return

            if mode == "awaiting_king_department":
                send_department_menu(chat_id, "Выбери для кого кинг:")
                return

            if mode == "awaiting_king_for_whom":
                send_person_menu(chat_id, prev_state.get("king_department"))
                return

            if mode == "awaiting_kings_count":
                tg_send_message(chat_id, "Сколько кингов нужно?", [
                    [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
                ])
                return

            if mode == "awaiting_king_names_bulk":
                tg_send_message(chat_id, f"Пришли {prev_state.get('kings_count', 0)} названий для кингов.\nКаждое название с новой строки.")
                return

            if mode == "awaiting_crypto_king_geo":
                send_crypto_king_geo_options(chat_id)
                return

            if mode == "awaiting_crypto_king_department":
                keyboard = [
                    [{"text": DEPT_CRYPTO}],
                    [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
                ]
                tg_send_message(chat_id, "Выбери для кого crypto king:", keyboard)
                return

            if mode == "awaiting_crypto_king_for_whom":
                send_person_menu(chat_id, DEPT_CRYPTO)
                return

            if mode == "awaiting_crypto_king_name":
                send_text_input_prompt(chat_id, "Какое название будет у crypto king?")
                return

            if mode == "awaiting_bm_department":
                send_department_menu(chat_id, "Выбери для кого БМ:")
                return

            if mode == "awaiting_bm_for_whom":
                send_person_menu(chat_id, prev_state.get("bm_department"))
                return

            if mode == "awaiting_bm_count":
                tg_send_message(chat_id, "Сколько БМов нужно?", [
                    [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
                ])
                return

            if mode == "awaiting_fp_department":
                send_department_menu(chat_id, "Выбери для кого ФП:")
                return

            if mode == "awaiting_return_fp_ban":
                send_text_input_prompt(chat_id, "Впиши ссылку ФП, которую нужно перевести в ban.")
                return

            if mode == "awaiting_farm_return_fp_ban":
                send_text_input_prompt(chat_id, "Впиши ссылку farm FP, которую нужно перевести в ban.")
                return

            if mode == "awaiting_fp_for_whom":
                send_person_menu(chat_id, prev_state.get("fp_department"))
                return

            if mode == "awaiting_fp_count":
                send_text_input_prompt(chat_id, "Сколько ФП нужно?")
                return

            if mode == "awaiting_pixel_department":
                send_department_menu(chat_id, "Выбери для кого Пиксели:")
                return

            if mode == "awaiting_pixel_for_whom":
                send_person_menu(chat_id, prev_state.get("pixel_department"))
                return

            if mode == "awaiting_pixel_count":
                send_text_input_prompt(chat_id, "Сколько Пикселей нужно?")
                return

            if mode == FARM_KING_OCTO_MODE_COUNT:
                send_text_input_prompt(chat_id, "Сколько farm king нужно?")
                return

            if mode == FARM_KING_OCTO_MODE_GEO:
                send_farm_king_geo_options(chat_id)
                return

            if mode == FARM_KING_OCTO_MODE_PRICE:
                send_farm_king_price_options(chat_id, prev_state.get("farm_king_geo", ""))
                return

            if mode == FARM_KING_OCTO_MODE_NAME:
                send_text_input_prompt(chat_id, "Какое название будет у farm king?")
                return

            if mode == FARM_KING_OCTO_MODE_BULK_NAMES:
                send_text_input_prompt(
                    chat_id,
                    f"Пришли {prev_state.get('farm_kings_count_requested', 0)} названий для farm king.\nКаждое название с новой строки."
                )
                return

            if mode == "awaiting_farm_king_geo":
                send_farm_king_geo_options(chat_id)
                return

            if mode == "awaiting_farm_kings_count":
                tg_send_message(chat_id, f"Сколько кингов нужно для GEO {prev_state.get('farm_king_geo', '')}?")
                return

            if mode == "awaiting_farm_king_names":
                tg_send_message(chat_id, f"Пришли {prev_state.get('farm_kings_count', 0)} названий для кингов.\nКаждое название с новой строки.")
                return

            if mode == "awaiting_farm_fp_count":
                send_text_input_prompt(chat_id, "Сколько FP нужно?")
                return

            send_main_menu(chat_id, "Возврат выполнен.", user_id=user_id)
            return

        # ========= ГЛАВНОЕ МЕНЮ =========
        if text == MENU_STATS:
            stats_text = build_stats_text()
            tg_send_message(chat_id, stats_text)
            send_main_menu(chat_id, "Главное меню:", user_id=user_id)
            return

        if text == MENU_MANAGER_STATS:
            summary_text = build_manager_stats_summary_text(username)

            tg_send_inline_message(
                chat_id,
                summary_text,
                [[{
                    "text": "Полная статистика",
                    "callback_data": f"fullstats_accounts:{username}"
                }]]
            )

            send_accounts_main_menu(chat_id, "Меню Accounts:")
            return

        if text == MENU_FARMER_STATS:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к этой статистике.")
                return

            summary_text = build_farmer_stats_summary_text(username)

            tg_send_inline_message(
                chat_id,
                summary_text,
                [[{
                    "text": "Полная статистика",
                    "callback_data": f"fullstats_farmers:{username}"
                }]]
            )

            send_farmers_menu(chat_id, "Меню Farmers:")
            return

        if text == MENU_ADMIN:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа к меню Admin.")
                return
        
            clear_state(user_id)
            send_admin_menu(chat_id, user_id=user_id)
            return

        if text == ADMIN_FARMERS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            clear_state(user_id)
            send_admin_farmers_menu(chat_id)
            return

        if text == BTN_BACK_FROM_ADMIN_FARMERS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            clear_state(user_id)
            send_admin_menu(chat_id)
            return

        if text == ADMIN_ACCOUNTANTS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            clear_state(user_id)
            send_admin_accountants_menu(chat_id)
            return

        if text == ADMIN_UPDATE_5M:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            sent, failed = notify_all_users_about_update()
            send_admin_menu(
                chat_id,
                f"Уведомление отправлено.\n\nУспешно: {sent}\nОшибок: {failed}"
            )
            return

        if text == ADMIN_ALL_STATS:
            try:
                send_all_users_stats(chat_id)
                send_admin_menu(chat_id, "Меню Admin:")
            except Exception as e:
                tg_send_message(chat_id, f"Ошибка в статистике всех:\n{e}")
            return

        if text == ADMIN_BOT_CHECK:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return
        
            run_bot_diagnostics_async(chat_id)
            return

        if text == BTN_BACK_FROM_ADMIN:
            clear_state(user_id)
            send_main_menu(chat_id, user_id=user_id)
            return

        if text == BTN_BACK_FROM_ACCOUNTANTS:
            clear_state(user_id)
            send_admin_menu(chat_id)
            return

        if text == BTN_CRYPTO_KING_BACK_TO_MENU:
            clear_state(user_id)
            send_main_menu(chat_id, "Главное меню:", user_id=user_id)
            return

        if text == MENU_ACCOUNTS:
            if not (is_admin(user_id) or is_accounts_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к разделу Accounts.")
                return

            clear_state(user_id)
            send_accounts_main_menu(chat_id)
            return

        if text == MENU_FARMERS:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к разделу Farmers.")
                return

            clear_state(user_id)
            send_farmers_menu(chat_id)
            return

        if text == SUBMENU_ACCOUNTS_MAIN:
            clear_state(user_id)
            set_state(user_id, {"last_accounts_section": "accounts"})
            send_accounts_menu(chat_id)
            return

        if text == MENU_KINGS:
            clear_state(user_id)
            set_state(user_id, {"last_accounts_section": "kings"})
            send_kings_menu(chat_id)
            return

        if text == MENU_BMS:
            clear_state(user_id)
            set_state(user_id, {"last_accounts_section": "bms"})
            send_bms_menu(chat_id)
            return

        if text == MENU_FPS:
            clear_state(user_id)
            set_state(user_id, {"last_accounts_section": "fps"})
            send_fps_menu(chat_id)
            return

        if text == MENU_PIXELS:
            clear_state(user_id)
            set_state(user_id, {"last_accounts_section": "pixels"})
            send_pixels_menu(chat_id)
            return

        if text == SUBMENU_BACK_MAIN:
            clear_state(user_id)
            send_main_menu(chat_id, user_id=user_id)
            return

        if text == BTN_BACK_TO_MENU:
            last_accounts_section = state.get("last_accounts_section", "")
            last_farmers_section = state.get("last_farmers_section", "")
            clear_state(user_id)

            if last_farmers_section == "kings":
                set_state(user_id, {"last_farmers_section": "kings"})
                send_farm_kings_menu(chat_id, "Меню Farm King:")
                return

            if last_farmers_section == "bms":
                set_state(user_id, {"last_farmers_section": "bms"})
                send_farm_bms_menu(chat_id, "Меню Farm BM:")
                return

            if last_farmers_section == "fps":
                set_state(user_id, {"last_farmers_section": "fps"})
                send_farm_fps_menu(chat_id, "Меню Farm FP:")
                return

            if last_accounts_section == "kings":
                set_state(user_id, {"last_accounts_section": "kings"})
                send_kings_menu(chat_id, "Меню кингов:")
                return

            if last_accounts_section == "bms":
                set_state(user_id, {"last_accounts_section": "bms"})
                send_bms_menu(chat_id, "Меню БМов:")
                return

            if last_accounts_section == "fps":
                set_state(user_id, {"last_accounts_section": "fps"})
                send_fps_menu(chat_id, "Меню ФП:")
                return

            if last_accounts_section == "pixels":
                set_state(user_id, {"last_accounts_section": "pixels"})
                send_pixels_menu(chat_id, "Меню Пикселей:")
                return

            send_main_menu(chat_id, user_id=user_id)
            return

        if text == BTN_BACK_TO_FARMERS:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к разделу Farmers.")
                return

            clear_state(user_id)
            send_farmers_menu(chat_id)
            return

        if text == FARM_MENU_KING:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к разделу Farmers.")
                return

            clear_state(user_id)
            set_state(user_id, {"last_farmers_section": "kings"})
            send_farm_kings_menu(chat_id)
            return

        if text == FARM_MENU_BM:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к разделу Farmers.")
                return

            clear_state(user_id)
            set_state(user_id, {"last_farmers_section": "bms"})
            send_farm_bms_menu(chat_id)
            return

        if text == FARM_MENU_FP:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к разделу Farmers.")
                return

            clear_state(user_id)
            set_state(user_id, {"last_farmers_section": "fps"})
            send_farm_fps_menu(chat_id)
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

        if text == ADMIN_ADD_CRYPTO_KINGS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_crypto_kings_txt"})
            send_add_kings_instructions(chat_id)
            return

        if text == ADMIN_ADD_BMS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_bms_text"})
            send_add_bms_instructions(chat_id)
            return

        if text == ADMIN_ADD_FPS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_fps_add"})
            send_add_fps_instructions(chat_id)
            return

        if text == ADMIN_ADD_PIXELS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_pixels_add"})
            send_add_pixels_instructions(chat_id)
            return

        if text == ADMIN_ADD_FARM_KINGS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_farm_kings_txt"})
            send_add_kings_instructions(chat_id)
            return

        if text == "📘 В меню":
            clear_state(user_id)
            send_main_menu(chat_id, "Главное меню:", user_id=user_id)
            return

        if text == ADMIN_ADD_FARM_BMS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_farm_bms_text"})
            send_add_bms_instructions(chat_id)
            return

        if text == ADMIN_ADD_FARM_FPS:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа.")
                return

            set_state(user_id, {"mode": "awaiting_farm_fps_add"})
            send_add_fps_instructions(chat_id)
            return

        # ========= ЛИЧКИ =========
        if text == SUBMENU_FREE:
            update_state(user_id, mode="awaiting_free_accounts_limit")
            send_free_accounts_limit_menu(chat_id, "Выбери лимит для показа свободных личек:")
            return

        if text == SUBMENU_SEARCH:
            update_state(user_id, mode="awaiting_search_account")
            send_text_input_prompt(chat_id, "Впиши номер лички для поиска.")
            return

        if text == SUBMENU_RETURN:
            update_state(user_id, mode="awaiting_account_return_action")
            send_return_action_menu(chat_id, "личкой")
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

        if text == BTN_ISSUE_MORE:
            if state.get("mode") != "quick_issue_continue":
                send_accounts_menu(chat_id, "Сначала начни быструю выдачу заново.")
                return
        
            for_whom = state.get("for_whom", "").strip()
            if not for_whom:
                clear_state(user_id)
                send_accounts_menu(chat_id, "Не найдено кому выдавать лички. Начни заново.")
                return
        
            result = issue_next_quick_account_for_person(
                for_whom=for_whom,
                username=username
            )
        
            if not result:
                clear_state(user_id)
                send_accounts_menu(chat_id, "Свободных личек больше нет.")
                return
        
            tg_send_message(
                chat_id,
                f"Готово ✅\n\n"
                f"Выдана личка: {result['account_number']}\n"
                f"Кому передали: {result['for_whom']}\n"
                f"Кто взял в боте: {result['who_took_text']}"
            )
        
            if result["account_url"]:
                tg_send_message(chat_id, f"Ссылка на личку:\n{result['account_url']}")
            else:
                tg_send_message(chat_id, "Ссылка на личку не найдена в колонке N.")
        
            keyboard = [
                [{"text": BTN_ISSUE_MORE}],
                [{"text": BTN_BACK_TO_MENU}]
            ]
            tg_send_message(chat_id, "Выдать еще личку?", keyboard)
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
            if state.get("mode") == "awaiting_return_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_account",
                    "return_account_number": state.get("return_account_number", "")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для лички.")
                return

            if state.get("mode") == "awaiting_return_king_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_king",
                    "return_king_name": state.get("return_king_name", ""),
                    "return_king_source": state.get("return_king_source", "normal")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для кинга.")
                return

            if state.get("mode") == "awaiting_return_bm_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_bm",
                    "return_bm_id": state.get("return_bm_id", "")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для БМа.")
                return

            if state.get("mode") == "awaiting_return_pixel_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_pixel",
                    "return_pixel_query": state.get("return_pixel_query", "")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для Пикселя.")
                return

            if state.get("mode") == "awaiting_farm_return_bm_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_farm_bm",
                    "return_farm_bm_id": state.get("return_farm_bm_id", "")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для farm BM.")
                return

            if state.get("mode") == "awaiting_return_fp_ban_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_fp",
                    "return_fp_link": state.get("return_fp_link", "")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для ФП.")
                return

            if state.get("mode") == "awaiting_farm_return_fp_ban_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_farm_fp",
                    "return_fp_link": state.get("return_fp_link", "")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для farm FP.")
                return

            send_accounts_menu(chat_id, "Сначала выбери действие заново.")
            return

        # ========= КИНГИ =========
        if text == SUBMENU_FREE_KINGS:
            if state.get("last_farmers_section") == "kings":
                send_free_farm_kings(chat_id)
                send_farm_kings_menu(chat_id, "Выбери следующее действие:")
                return

            send_free_kings(chat_id)
            send_accounts_main_menu(chat_id, "Меню Accounts:")
            return

        if text == SUBMENU_SEARCH_KING:
            if state.get("last_farmers_section") == "kings":
                update_state(user_id, mode="awaiting_farm_search_king_name")
                send_text_input_prompt(chat_id, "Впиши название кинга.")
                return

            update_state(user_id, mode="awaiting_search_king_name")
            send_text_input_prompt(chat_id, "Впиши название кинга.")
            return

        if text == SUBMENU_RETURN_KING:
            if state.get("last_farmers_section") == "kings":
                update_state(user_id, mode="awaiting_farm_king_return_action")
                send_return_action_menu(chat_id, "фарм кингом")
                return

            update_state(user_id, mode="awaiting_king_return_action")
            send_return_action_menu(chat_id, "кингом")
            return

        if text == SUBMENU_GET_KINGS:
            clear_state(user_id)
            set_state(user_id, {
                "mode": "awaiting_king_geo",
                "last_accounts_section": "kings"
            })
            send_king_geo_options(chat_id)
            return

        if text == SUBMENU_CRYPTO_KINGS:
            state = {
                "mode": CRYPTO_BULK_MODE_COUNT
            }
            set_state(user_id, state)
        
            tg_send_message(
                chat_id,
                "Сколько crypto king нужно?",
                keyboard=[
                    [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
                ]
            )
            return

        if text == BTN_KING_CONFIRM:
            if state.get("mode") == "crypto_king_found":
                confirm_crypto_king_issue(chat_id, user_id, username)
            else:
                confirm_king_issue(chat_id, user_id, username)
            return

        if text == BTN_KING_NEXT:
            if not state:
                send_kings_menu(chat_id, "Начни заново.")
                return
        
            if state.get("mode") in ["crypto_king_found", "awaiting_crypto_king_name"]:
                king_geo = state.get("king_geo", "").strip()
                if not king_geo:
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Не найдено GEO. Начни заново.")
                    return
            
                found = find_free_crypto_king_by_geo(
                    king_geo,
                    exclude_row=state.get("king_row")
                )
            
                if not found:
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Свободных crypto king с таким GEO больше нет.")
                    return
            
                show_found_crypto_king(chat_id, user_id, found)
                return
        
            king_geo = state.get("king_geo", "").strip()
            if not king_geo:
                clear_state(user_id)
                send_kings_menu(chat_id, "Не найдено GEO. Начни заново.")
                return
        
            found = find_free_king_by_geo(
                king_geo,
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

            set_state(user_id, {
                "mode": "awaiting_ban_reason_king",
                "return_king_name": state.get("return_king_name", ""),
                "return_king_source": state.get("return_king_source", "normal")
            })
            send_text_input_prompt(chat_id, "Напиши причину бана для кинга.")
            return

        if text == BTN_PIXEL_RETURN_FREE_CONFIRM:
            if state.get("mode") != "awaiting_return_pixel_free_confirm":
                send_pixels_menu(chat_id, "Сначала выбери действие заново.")
                return
        
            pixel_query = state.get("return_pixel_query", "")
            ok, message = return_pixel_to_free(pixel_query)
            clear_state(user_id)
            send_pixels_menu(chat_id, message)
            return

        if text == BTN_KING_RETURN_FREE_CONFIRM:
            if state.get("mode") != "awaiting_return_king_free_confirm":
                send_kings_menu(chat_id, "Сначала выбери действие заново.")
                return

            king_name = state.get("return_king_name", "")
            ok, message = return_king_to_free(king_name)
            clear_state(user_id)
            send_accounts_main_menu(chat_id, message)
            return


        if text == BTN_FARM_KING_RETURN_FREE_CONFIRM:
            if state.get("mode") != "awaiting_farm_return_king_free_confirm":
                send_farm_kings_menu(chat_id, "Сначала выбери действие заново.")
                return

            king_name = state.get("return_king_name", "")
            ok, message = return_farm_king_to_free(king_name)
            clear_state(user_id)
            send_farm_kings_menu(chat_id, message)
            return

        if text == BTN_FP_RETURN_FREE_CONFIRM:
            if state.get("mode") != "awaiting_return_fp_free_confirm":
                send_fps_menu(chat_id, "Сначала выбери действие заново.")
                return

            fp_link = state.get("return_fp_link", "")
            ok, message = return_fp_to_free(fp_link)
            clear_state(user_id)
            send_fps_menu(chat_id, message)
            return


        if text == BTN_FARM_FP_RETURN_FREE_CONFIRM:
            if state.get("mode") != "awaiting_farm_return_fp_free_confirm":
                send_farm_fps_menu(chat_id, "Сначала выбери действие заново.")
                return

            fp_link = state.get("return_fp_link", "")
            ok, message = return_farm_fp_to_free(fp_link)
            clear_state(user_id)
            send_farm_fps_menu(chat_id, message)
            return

        if text == BTN_BM_RETURN_FREE_CONFIRM:
            if state.get("mode") != "awaiting_return_bm_free_confirm":
                send_bms_menu(chat_id, "Сначала выбери действие заново.")
                return
        
            bm_id = state.get("return_bm_id", "")
            ok, message = return_bm_to_free(bm_id)
            clear_state(user_id)
            send_bms_menu(chat_id, message)
            return

        if text == BTN_FARM_BM_RETURN_FREE_CONFIRM:
            if state.get("mode") != "awaiting_farm_return_bm_free_confirm":
                send_farm_bms_menu(chat_id, "Сначала выбери действие заново.")
                return

            bm_id = state.get("return_farm_bm_id", "")
            ok, message = return_farm_bm_to_free(bm_id)
            clear_state(user_id)
            send_farm_bms_menu(chat_id, message)
            return

        # ========= БМы =========
        if text == SUBMENU_FREE_BMS:
            if state.get("last_farmers_section") == "bms":
                free_count = count_free_farm_bms()
                tg_send_message(chat_id, f"Свободных фарм BMов: {free_count}")
                send_farm_bms_menu(chat_id, "Выбери следующее действие:")
                return

            free_count = count_free_bms()
            tg_send_message(chat_id, f"Свободных БМов: {free_count}")
            send_accounts_main_menu(chat_id, "Меню Accounts:")
            return

        if text == SUBMENU_SEARCH_BM:
            if state.get("last_farmers_section") == "bms":
                update_state(user_id, mode="awaiting_farm_search_bm")
                tg_send_message(chat_id, "Впиши ID BM.")
                return

            update_state(user_id, mode="awaiting_search_bm")
            send_text_input_prompt(chat_id, "Впиши ID БМа для поиска.")
            return

        if text == SUBMENU_RETURN_BM:
            if state.get("last_farmers_section") == "bms":
                update_state(user_id, mode="awaiting_farm_bm_return_action")
                send_return_action_menu(chat_id, "farm BM")
                return
        
            update_state(user_id, mode="awaiting_bm_return_action")
            send_return_action_menu(chat_id, "БМ")
            return

        if text == SUBMENU_GET_BM:
            if state.get("last_farmers_section") == "bms":
                issue_farm_bm(chat_id, user_id, username)
                return

            clear_state(user_id)
            update_state(user_id, mode="awaiting_bm_department")
            send_department_menu(chat_id, "Выбери для кого БМ:")
            return

        if text == BTN_BM_CONFIRM:
            confirm_bm_issue(chat_id, user_id, username)
            return

        if text == BTN_BM_BAN_CONFIRM:
            if state.get("mode") == "awaiting_return_bm_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_bm",
                    "return_bm_id": state.get("return_bm_id", "")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для БМа.")
                return

            if state.get("mode") == "awaiting_farm_return_bm_confirm":
                set_state(user_id, {
                    "mode": "awaiting_ban_reason_farm_bm",
                    "return_farm_bm_id": state.get("return_farm_bm_id", "")
                })
                send_text_input_prompt(chat_id, "Напиши причину бана для farm BM.")
                return

            send_main_menu(chat_id, "Сначала выбери действие заново.", user_id=user_id)
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

        # ========= фп =========
        if text == SUBMENU_SEARCH_FP:
            if state.get("last_farmers_section") == "fps":
                update_state(user_id, mode="awaiting_farm_search_fp")
                send_text_input_prompt(chat_id, "Впиши ссылку FP.")
                return

            update_state(user_id, mode="awaiting_search_fp")
            send_text_input_prompt(chat_id, "Впиши ссылку ФП для поиска.")
            return

        if text == SUBMENU_RETURN_FP:
            update_state(user_id, mode="awaiting_fp_return_action")
            send_return_action_menu(chat_id, "ФП")
            return

        if text == SUBMENU_GET_FP:
            if state.get("last_farmers_section") == "fps":
                set_state(user_id, {"mode": "awaiting_farm_fp_count", "last_farmers_section": "fps"})
                send_text_input_prompt(chat_id, "Сколько FP нужно?")
                return

            clear_state(user_id)
            update_state(user_id, mode="awaiting_fp_department")
            send_department_menu(chat_id, "Выбери для кого ФП:")
            return

        if text == BTN_FP_CONFIRM:
            confirm_fp_issue(chat_id, user_id, username)
            return

        if text == BTN_FP_NEXT:
            if not state:
                send_fps_menu(chat_id, "Начни заново.")
                return

            found = find_free_fp(exclude_link=state.get("found_fp_link"))

            if not found:
                clear_state(user_id)
                send_fps_menu(chat_id, "Свободных ФП больше нет.")
                return

            show_found_fp(chat_id, user_id, found)
            return

        if text == SUBMENU_SEARCH_PIXEL:
            update_state(user_id, mode="awaiting_search_pixel")
            send_text_input_prompt(chat_id, "Впиши ID пикселя или часть данных Пикселя для поиска.")
            return

        if text == SUBMENU_RETURN_PIXEL:
            update_state(user_id, mode="awaiting_pixel_return_action")
            send_return_action_menu(chat_id, "Пикселем")
            return

        if text == SUBMENU_GET_PIXELS:
            clear_state(user_id)
            update_state(user_id, mode="awaiting_pixel_department")
            send_department_menu(chat_id, "Выбери для кого Пиксели:")
            return

        if text == BTN_PIXEL_CONFIRM:
            confirm_pixel_issue(chat_id, user_id, username)
            return

        if text == BTN_PIXEL_NEXT:
            if not state:
                send_pixels_menu(chat_id, "Начни заново.")
                return

            found = find_free_pixel(exclude_row=state.get("pixel_row"))

            if not found:
                clear_state(user_id)
                send_pixels_menu(chat_id, "Свободных Пикселей больше нет.")
                return

            show_found_pixel(chat_id, user_id, found)
            return

        if text == BTN_PIXEL_BAN_CONFIRM:
            if state.get("mode") != "awaiting_return_pixel_confirm":
                send_pixels_menu(chat_id, "Сначала выбери действие заново.")
                return

            set_state(user_id, {
                "mode": "awaiting_ban_reason_pixel",
                "return_pixel_query": state.get("return_pixel_query", "")
            })
            send_text_input_prompt(chat_id, "Напиши причину бана для Пикселя.")
            return
        
        # ========= FARMERS =========
        if text == FARM_SUBMENU_FREE_KINGS:
            send_free_farm_kings(chat_id)
            send_farm_kings_menu(chat_id, "Выбери следующее действие:")
            return

        if text == FARM_SUBMENU_GET_KINGS:
            clear_state(user_id)
            set_state(user_id, {
                "mode": FARM_KING_OCTO_MODE_COUNT
            })
            send_text_input_prompt(chat_id, "Сколько farm king нужно?")
            return

        if text == FARM_SUBMENU_RETURN_KING:
            update_state(user_id, mode="awaiting_farm_king_return_action")
            send_return_action_menu(chat_id, "фарм кингом")
            return

        if text == FARM_SUBMENU_SEARCH_KING:
            set_state(user_id, {"mode": "awaiting_farm_search_king_name"})
            send_text_input_prompt(chat_id, "Впиши название кинга.")
            return

        if text == FARM_SUBMENU_FREE_BMS:
            free_count = count_free_farm_bms()
            tg_send_message(chat_id, f"Свободных фарм BMов: {free_count}")
            send_farm_bms_menu(chat_id, "Выбери следующее действие:")
            return

        if text == FARM_SUBMENU_GET_BM:
            issue_farm_bm(chat_id, user_id, username)
            return

        if text == FARM_SUBMENU_SEARCH_BM:
            set_state(user_id, {"mode": "awaiting_farm_search_bm"})
            tg_send_message(chat_id, "Впиши ID BM.")
            return

        if text == FARM_SUBMENU_RETURN_BM:
            update_state(user_id, mode="awaiting_farm_bm_return_action")
            send_return_action_menu(chat_id, "farm BM")
            return

        if text == FARM_SUBMENU_GET_FP:
            set_state(user_id, {"mode": "awaiting_farm_fp_count"})
            send_text_input_prompt(chat_id, "Сколько FP нужно?")
            return

        if text == FARM_SUBMENU_SEARCH_FP:
            set_state(user_id, {"mode": "awaiting_farm_search_fp"})
            send_text_input_prompt(chat_id, "Впиши ссылку FP.")
            return

        if text == FARM_SUBMENU_RETURN_FP:
            update_state(user_id, mode="awaiting_farm_fp_return_action")
            send_return_action_menu(chat_id, "farm FP")
            return

        if text == BTN_FARM_KINGS_PARTIAL_CONFIRM:
            if state.get("mode") != "awaiting_farm_kings_partial_confirm":
                send_farm_kings_menu(chat_id, "Начни заново.")
                return

            count_needed = state.get("farm_kings_count", 0)

            state["mode"] = "awaiting_farm_king_names"
            set_state(user_id, state)

            send_text_input_prompt(
                chat_id,
                f"Пришли {count_needed} названий для кингов.\nКаждое название с новой строки."
            )
            return

        if text == BTN_FARM_KINGS_PARTIAL_CANCEL:
            clear_state(user_id)
            send_farm_kings_menu(chat_id, "Выдача отменена.")
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

        if state.get("mode") == "awaiting_farm_bms_text":
            result = add_bms_from_txt_content(text, target_sheet=SHEET_FARM_BMS)
            clear_state(user_id)
            tg_send_message(chat_id, result)

            if is_admin(user_id):
                send_admin_farmers_menu(chat_id, "Выбери следующее действие:")
            else:
                send_main_menu(chat_id, "Готово. Выбери следующее действие:", user_id=user_id)
            return

        if state.get("mode") == "awaiting_fps_add":
            result = add_fps_from_text(text)
            result_message = result["message"]
            new_warehouses = result["new_warehouses"]
        
            if new_warehouses and OCTO_API_TOKEN:
                set_state(user_id, {
                    "mode": "awaiting_octo_proxy_for_warehouse",
                    "octo_target_sheet": SHEET_FPS,
                    "octo_warehouses_queue": new_warehouses,
                    "octo_created_profiles": [],
                    "octo_failed_profiles": [],
                    "last_accounts_section": state.get("last_accounts_section", ""),
                    "last_farmers_section": state.get("last_farmers_section", ""),
                })
        
                tg_send_message(chat_id, result_message)
                send_text_input_prompt(
                    chat_id,
                    f"Теперь пришли proxy для Octo профиля склада:\n{new_warehouses[0]}\n\nФормат:\nip:port\nили\nip:port:login:password"
                )
                return
        
            clear_state(user_id)
            tg_send_message(chat_id, result_message)
        
            if is_admin(user_id):
                send_admin_menu(chat_id, "Выбери следующее действие:")
            else:
                send_main_menu(chat_id, "Готово. Выбери следующее действие:", user_id=user_id)
            return

        if state.get("mode") == "awaiting_farm_fps_add":
            result = add_fps_from_text(text, target_sheet=SHEET_FARM_FPS)
            result_message = result["message"]
            new_warehouses = result["new_warehouses"]
        
            if new_warehouses and OCTO_API_TOKEN:
                set_state(user_id, {
                    "mode": "awaiting_octo_proxy_for_warehouse",
                    "octo_target_sheet": SHEET_FARM_FPS,
                    "octo_warehouses_queue": new_warehouses,
                    "octo_created_profiles": [],
                    "octo_failed_profiles": [],
                    "last_accounts_section": state.get("last_accounts_section", ""),
                    "last_farmers_section": state.get("last_farmers_section", ""),
                })
        
                tg_send_message(chat_id, result_message)
                send_text_input_prompt(
                    chat_id,
                    f"Теперь пришли proxy для Octo профиля склада:\n{new_warehouses[0]}\n\nФормат:\nip:port\nили\nip:port:login:password"
                )
                return
        
            clear_state(user_id)
            tg_send_message(chat_id, result_message)
        
            if is_admin(user_id):
                send_admin_farmers_menu(chat_id, "Выбери следующее действие:")
            else:
                send_main_menu(chat_id, "Готово. Выбери следующее действие:", user_id=user_id)
            return

        if state.get("mode") == "awaiting_free_accounts_limit":
            if text not in [
                FREE_LIMIT_250,
                FREE_LIMIT_500,
                FREE_LIMIT_1200,
                FREE_LIMIT_1500,
                FREE_LIMIT_UNLIM
            ]:
                send_free_accounts_limit_menu(chat_id, "Нужно выбрать лимит кнопкой:")
                return

            clear_state(user_id)
            send_free_accounts(chat_id, text)
            send_accounts_main_menu(chat_id, "Меню Accounts:")
            return

        if state.get("mode") == "awaiting_account_return_action":
            if text == BTN_RETURN_TO_BAN:
                update_state(user_id, mode="awaiting_return_account")
                send_text_input_prompt(chat_id, "Впиши номер лички, которую нужно отправить в ban.")
                return

            if text == BTN_RETURN_TO_FREE:
                update_state(user_id, mode="awaiting_return_account_free")
                send_text_input_prompt(chat_id, "Впиши номер лички, которую нужно вернуть.")
                return

            send_return_action_menu(chat_id, "личкой")
            return


        if state.get("mode") == "awaiting_king_return_action":
            if text == BTN_RETURN_TO_BAN:
                update_state(user_id, mode="awaiting_return_king_name")
                send_text_input_prompt(chat_id, "Впиши название кинга, который нужно перевести в ban.")
                return

            if text == BTN_RETURN_TO_FREE:
                update_state(user_id, mode="awaiting_return_king_free_name")
                send_text_input_prompt(chat_id, "Впиши название кинга, который нужно вернуть.")
                return

            send_return_action_menu(chat_id, "кингом")
            return


        if state.get("mode") == "awaiting_farm_king_return_action":
            if text == BTN_RETURN_TO_BAN:
                update_state(user_id, mode="awaiting_farm_return_king_name")
                send_text_input_prompt(chat_id, "Впиши название кинга.")
                return

            if text == BTN_RETURN_TO_FREE:
                update_state(user_id, mode="awaiting_farm_return_king_free_name")
                send_text_input_prompt(chat_id, "Впиши название кинга, который нужно вернуть.")
                return

            send_return_action_menu(chat_id, "фарм кингом")
            return

        if state.get("mode") == "awaiting_pixel_return_action":
            if text == BTN_RETURN_TO_BAN:
                update_state(user_id, mode="awaiting_return_pixel")
                send_text_input_prompt(chat_id, "Впиши ID пикселя или часть данных Пикселя, который нужно перевести в ban.")
                return
        
            if text == BTN_RETURN_TO_FREE:
                update_state(user_id, mode="awaiting_return_pixel_free")
                send_text_input_prompt(chat_id, "Впиши ID пикселя или часть данных Пикселя, который нужно вернуть.")
                return
        
            send_return_action_menu(chat_id, "Пикселем")
            return

        if state.get("mode") == "awaiting_fp_return_action":
            if text == BTN_RETURN_TO_BAN:
                update_state(user_id, mode="awaiting_return_fp_ban")
                send_text_input_prompt(chat_id, "Впиши ссылку ФП, которую нужно перевести в ban.")
                return

            if text == BTN_RETURN_TO_FREE:
                update_state(user_id, mode="awaiting_return_fp_free")
                send_text_input_prompt(chat_id, "Впиши ссылку ФП, которую нужно вернуть.")
                return

            send_return_action_menu(chat_id, "ФП")
            return

        if state.get("mode") == "awaiting_bm_return_action":
            if text == BTN_RETURN_TO_BAN:
                update_state(user_id, mode="awaiting_return_bm")
                send_text_input_prompt(chat_id, "Впиши ID БМа, который нужно перевести в ban.")
                return
        
            if text == BTN_RETURN_TO_FREE:
                update_state(user_id, mode="awaiting_return_bm_free")
                send_text_input_prompt(chat_id, "Впиши ID БМа, который нужно вернуть.")
                return
        
            send_return_action_menu(chat_id, "БМ")
            return

        if state.get("mode") == "awaiting_farm_fp_return_action":
            if text == BTN_RETURN_TO_BAN:
                update_state(user_id, mode="awaiting_farm_return_fp_ban")
                send_text_input_prompt(chat_id, "Впиши ссылку farm FP, которую нужно перевести в ban.")
                return

            if text == BTN_RETURN_TO_FREE:
                update_state(user_id, mode="awaiting_farm_return_fp_free")
                send_text_input_prompt(chat_id, "Впиши ссылку farm FP, которую нужно вернуть.")
                return

            send_return_action_menu(chat_id, "farm FP")
            return

        if state.get("mode") == "awaiting_farm_bm_return_action":
            if text == BTN_RETURN_TO_BAN:
                update_state(user_id, mode="awaiting_farm_return_bm")
                send_text_input_prompt(chat_id, "Впиши ID farm BM, который нужно перевести в ban.")
                return

            if text == BTN_RETURN_TO_FREE:
                update_state(user_id, mode="awaiting_farm_return_bm_free")
                send_text_input_prompt(chat_id, "Впиши ID farm BM, который нужно вернуть.")
                return

            send_return_action_menu(chat_id, "farm BM")
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
            send_accounts_main_menu(chat_id, "Меню Accounts:")
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

        if state.get("mode") == "awaiting_return_fp_ban":
            fp_link = text.strip()

            if not fp_link:
                tg_send_message(chat_id, "Впиши ссылку ФП.")
                return

            found = find_fp_in_base(fp_link)
            if not found:
                clear_state(user_id)
                send_fps_menu(chat_id, "ФП не найдено.")
                return

            set_state(user_id, {
                "mode": "awaiting_return_fp_ban_confirm",
                "return_fp_link": fp_link
            })

            keyboard = [
                [{"text": BTN_RETURN_CONFIRM}, {"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                "Внимание: ФП будет переведено в ban.\nПодтвердить?",
                keyboard
            )
            return


        if state.get("mode") == "awaiting_return_fp_free":
            fp_link = text.strip()

            if not fp_link:
                tg_send_message(chat_id, "Впиши ссылку ФП.")
                return

            found = find_fp_in_base(fp_link)
            if not found:
                clear_state(user_id)
                send_fps_menu(chat_id, "ФП не найдено.")
                return

            set_state(user_id, {
                "mode": "awaiting_return_fp_free_confirm",
                "return_fp_link": fp_link
            })

            keyboard = [
                [{"text": BTN_FP_RETURN_FREE_CONFIRM}, {"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                "Внимание: ФП будет возвращено в free.\nПодтвердить?",
                keyboard
            )
            return


        if state.get("mode") == "awaiting_farm_return_fp_ban":
            fp_link = text.strip()

            if not fp_link:
                tg_send_message(chat_id, "Впиши ссылку FP.")
                return

            found = find_farm_fp_in_base(fp_link)
            if not found:
                clear_state(user_id)
                send_farm_fps_menu(chat_id, "FP не найдено.")
                return

            set_state(user_id, {
                "mode": "awaiting_farm_return_fp_ban_confirm",
                "return_fp_link": fp_link
            })

            keyboard = [
                [{"text": BTN_RETURN_CONFIRM}, {"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                "Внимание: farm FP будет переведено в ban.\nПодтвердить?",
                keyboard
            )
            return


        if state.get("mode") == "awaiting_farm_return_fp_free":
            fp_link = text.strip()

            if not fp_link:
                tg_send_message(chat_id, "Впиши ссылку FP.")
                return

            found = find_farm_fp_in_base(fp_link)
            if not found:
                clear_state(user_id)
                send_farm_fps_menu(chat_id, "FP не найдено.")
                return

            set_state(user_id, {
                "mode": "awaiting_farm_return_fp_free_confirm",
                "return_fp_link": fp_link
            })

            keyboard = [
                [{"text": BTN_FARM_FP_RETURN_FREE_CONFIRM}, {"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                "Внимание: farm FP будет возвращено в free.\nПодтвердить?",
                keyboard
            )
            return

        if state.get("mode") == "awaiting_return_account_free":
            account_number = text.strip()

            if not account_number:
                tg_send_message(chat_id, "Впиши номер лички.")
                return

            found = find_account_in_base(account_number)
            if not found:
                clear_state(user_id)
                send_accounts_menu(chat_id, "Личка не найдена.")
                return

            ok, message = return_account_to_free(account_number)
            clear_state(user_id)
            send_accounts_main_menu(chat_id, message)
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

            clean_name = normalize_person_name(text)

            set_state(user_id, {
                "mode": "awaiting_issue_account_number",
                "for_whom": clean_name,
                "issue_department": state.get("issue_department")
            })

            send_text_input_prompt(chat_id, "Теперь напиши номер лички или несколько номеров, каждый с новой строки.")
            return

        if state.get("mode") == "awaiting_issue_account_number":
            account_numbers = [x.strip() for x in text.splitlines() if x.strip()]

            if not account_numbers:
                tg_send_message(chat_id, "Впиши номер лички.")
                return

            # если одна личка — оставляем старую логику с подтверждением
            if len(account_numbers) == 1:
                account_number = account_numbers[0]

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
                    "currency": row[12] if len(row) > 12 else ""
                }

                show_found_account(chat_id, user_id, found_data)
                return

            # если несколько личек — выдаем сразу все
            result = issue_accounts_bulk(
                account_numbers=account_numbers,
                for_whom=state["for_whom"],
                username=username
            )

            clear_state(user_id)

            issued = result["issued"]
            not_found = result["not_found"]
            not_available = result["not_available"]
            who_took_text = result["who_took_text"]

            if not issued:
                text_result = "Не удалось выдать ни одной лички."

                if not_found:
                    text_result += "\n\nНе найдены:\n" + "\n".join(not_found[:50])

                if not_available:
                    text_result += "\n\nНе свободны:\n" + "\n".join(not_available[:50])

                send_accounts_menu(chat_id, text_result)
                return

            tg_send_message(
                chat_id,
                f"Готово ✅\n\n"
                f"Выдано личек: {len(issued)}\n"
                f"Кому передали: {state['for_whom']}\n"
                f"Кто взял в боте: {who_took_text}"
            )

            blocks = []
            for i, item in enumerate(issued, start=1):
                block = (
                    f"{i}. {item['account_number']}\n"
                    f"Склады: {item['warehouses']}\n"
                    f"Дата покупки: {item['purchase_date']}\n"
                    f"Цена: {item['price']}\n"
                    f"Валюта: {item['currency']}"
                )

                if item["account_url"]:
                    block += f"\nСсылка: {item['account_url']}"

                blocks.append(block)

            current_text = ""
            for block in blocks:
                chunk = block + "\n\n"
                if len(current_text) + len(chunk) > 3500:
                    tg_send_message(chat_id, current_text.strip())
                    current_text = chunk
                else:
                    current_text += chunk

            if current_text.strip():
                tg_send_message(chat_id, current_text.strip())

            if not_found:
                tg_send_message(chat_id, "Не найдены:\n" + "\n".join(not_found[:50]))

            if not_available:
                tg_send_message(chat_id, "Не свободны:\n" + "\n".join(not_available[:50]))

            send_accounts_menu(chat_id, "Выбери следующее действие:")
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

            clean_name = normalize_person_name(text)

            state["mode"] = "quick_account_found"
            state["for_whom"] = clean_name
            set_state(user_id, state)

            found = find_oldest_free_account()

            if not found:
                clear_state(user_id)
                send_accounts_menu(chat_id, "Свободных личек сейчас нет.")
                return

            show_found_account(chat_id, user_id, found)
            return

        # ========= СОСТОЯНИЯ: КИНГИ =========
        if state.get("mode") == "awaiting_crypto_king_geo":
            geos = get_free_crypto_king_geos()

            if text not in geos:
                send_crypto_king_geo_options(chat_id)
                return

            set_state(user_id, {
                "mode": CRYPTO_SINGLE_MODE_PRICE,
                "king_geo": text
            })

            send_crypto_king_price_options(chat_id, text)
            return

        if state.get("mode") == CRYPTO_SINGLE_MODE_PRICE:
            geo = str(state.get("king_geo", "")).strip()
            prices = get_free_crypto_king_prices_by_geo(geo)
            selected_price = normalize_price_key(text)

            if selected_price not in prices:
                send_crypto_king_price_options(chat_id, geo)
                return

            set_state(user_id, {
                "mode": "awaiting_crypto_king_department",
                "king_geo": geo,
                "king_price": selected_price
            })

            keyboard = [
                [{"text": DEPT_CRYPTO}],
                [{"text": MENU_CANCEL}]
            ]
            tg_send_message(chat_id, "Выбери для кого crypto king:", keyboard)
            return

        if state.get("mode") == CRYPTO_BULK_MODE_COUNT:
            count_text = str(text).strip()

            if not count_text.isdigit():
                tg_send_message(chat_id, "Пришли количество числом.")
                return

            count_needed = int(count_text)
            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            state["crypto_kings_count"] = count_needed
            state["mode"] = "awaiting_crypto_king_geo_bulk"
            set_state(user_id, state)

            send_crypto_king_geo_options(chat_id)
            return

        if state.get("mode") == "awaiting_crypto_king_geo_bulk":
            geo = str(text).strip()
            geos = get_free_crypto_king_geos()

            if geo not in geos:
                send_crypto_king_geo_options(chat_id)
                return

            state["crypto_bulk_geo"] = geo
            state["mode"] = CRYPTO_BULK_MODE_PRICE
            set_state(user_id, state)

            send_crypto_king_price_options(chat_id, geo)
            return

        if state.get("mode") == CRYPTO_BULK_MODE_PRICE:
            geo = str(state.get("crypto_bulk_geo", "")).strip()
            prices = get_free_crypto_king_prices_by_geo(geo)
            selected_price = normalize_price_key(text)

            if selected_price not in prices:
                send_crypto_king_price_options(chat_id, geo)
                return

            free_items = find_free_crypto_kings_by_geo_and_price(
                state.get("crypto_kings_count", 0),
                geo,
                selected_price
            )

            if len(free_items) < int(state.get("crypto_kings_count", 0)):
                tg_send_message(
                    chat_id,
                    f"Недостаточно свободных crypto king по GEO {geo} и цене {selected_price}.\n"
                    f"Доступно: {len(free_items)}"
                )
                return

            state["crypto_bulk_price"] = selected_price
            state["crypto_bulk_selected_rows"] = free_items
            state["crypto_bulk_department"] = DEPT_CRYPTO
            state["mode"] = "awaiting_crypto_king_person_bulk"
            set_state(user_id, state)

            send_person_menu(chat_id, DEPT_CRYPTO)
            return

        if state.get("mode") == "awaiting_crypto_king_person_bulk":
            department = state.get("crypto_bulk_department")

            allowed_names = []
            if department == DEPT_CRYPTO:
                allowed_names = CRYPTO_NAMES
            elif department == DEPT_GAMBLA:
                allowed_names = GAMBLA_NAMES

            if text not in allowed_names:
                send_person_menu(chat_id, department)
                return

            state["king_for_whom"] = normalize_person_name(text)
            state["mode"] = CRYPTO_BULK_MODE_NAMES
            set_state(user_id, state)

            selected_rows = state.get("crypto_bulk_selected_rows", [])

            tg_send_message(
                chat_id,
                f"Пришли названия для {len(selected_rows)} crypto king.\n"
                f"Каждое название с новой строки."
            )
            return

        if state.get("mode") == CRYPTO_BULK_MODE_NAMES:
            lines = [str(x).strip() for x in str(text).splitlines() if str(x).strip()]
            selected_rows = state.get("crypto_bulk_selected_rows", [])

            if len(lines) != len(selected_rows):
                tg_send_message(
                    chat_id,
                    f"Нужно прислать {len(selected_rows)} названий.\n"
                    f"Сейчас получено: {len(lines)}"
                )
                return

            duplicates = []
            for king_name in lines:
                if crypto_king_name_exists(king_name):
                    duplicates.append(king_name)

            if duplicates:
                tg_send_message(
                    chat_id,
                    "Эти названия уже существуют:\n" + "\n".join(duplicates[:20])
                )
                return

            queue = []
            for item, king_name in zip(selected_rows, lines):
                queue.append({
                    "row_index": item["row_index"],
                    "purchase_date": item["purchase_date"],
                    "price": item["price"],
                    "supplier": item["supplier"],
                    "geo": item["geo"],
                    "king_name": king_name,
                    "data_text": item.get("data_text", "")
                })

            state["crypto_bulk_queue"] = queue
            state["crypto_bulk_results"] = []
            state["crypto_bulk_current_index"] = 0
            state["crypto_bulk_username"] = username
            set_state_with_custom_ttl(user_id, state, CRYPTO_BULK_PROXY_TTL)
            
            send_crypto_bulk_found_preview_once(chat_id, user_id)
            return

        if state.get("mode") == CRYPTO_BULK_MODE_PROXY:
            process_crypto_bulk_proxy_step(
                chat_id=chat_id,
                user_id=user_id,
                username=username,
                proxy_text=text
            )
            return

        if state.get("mode") == "awaiting_crypto_king_department":
            keyboard = [[{"text": DEPT_CRYPTO}], [{"text": MENU_CANCEL}]]

            if text != DEPT_CRYPTO:
                tg_send_message(chat_id, "Выбери отдел Крипта.", keyboard)
                return

            state["mode"] = "awaiting_crypto_king_person"
            set_state(user_id, state)

            send_person_menu(chat_id, DEPT_CRYPTO)
            return

        if state.get("mode") == "awaiting_crypto_king_person":
            if text not in CRYPTO_NAMES:
                send_person_menu(chat_id, DEPT_CRYPTO)
                return

            state["king_for_whom"] = normalize_person_name(text)
            state["mode"] = "awaiting_crypto_king_name"
            set_state(user_id, state)

            tg_send_message(chat_id, "Какое название будет у crypto king?")
            return

        if state.get("mode") == "awaiting_crypto_king_name":
            king_name = str(text).strip()

            if not king_name:
                tg_send_message(chat_id, "Название не должно быть пустым.")
                return

            if crypto_king_name_exists(king_name):
                tg_send_message(chat_id, "Такое название уже существует. Пришли другое.")
                return

            geo_value = str(state.get("king_geo", "")).strip()
            price_value = str(state.get("king_price", "")).strip()

            found = find_free_crypto_king_by_geo_and_price(geo_value, price_value)

            if not found:
                clear_state(user_id)
                send_kings_menu(
                    chat_id,
                    f"Свободный crypto king не найден.\nГео: {geo_value}\nЦена: {price_value}"
                )
                return

            state["king_name"] = king_name
            set_state(user_id, state)

            show_found_crypto_king(chat_id, user_id, found)
            return

        if state.get("mode") == KING_OCTO_MODE_COUNT:
            count_text = str(text).strip()

            if not count_text.isdigit():
                tg_send_message(chat_id, "Пришли количество числом.")
                return

            count_needed = int(count_text)
            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            state["kings_count_requested"] = count_needed
            state["mode"] = KING_OCTO_MODE_GEO
            set_state(user_id, state)

            send_king_geo_options(chat_id)
            return

        if state.get("mode") == KING_OCTO_MODE_GEO:
            geos = get_free_king_geos()

            if text not in geos:
                send_king_geo_options(chat_id)
                return

            state["king_geo"] = text
            state["mode"] = KING_OCTO_MODE_PRICE
            set_state(user_id, state)

            send_king_price_options(chat_id, text)
            return

        if state.get("mode") == KING_OCTO_MODE_PRICE:
            geo = str(state.get("king_geo", "")).strip()
            prices = get_free_king_prices_by_geo(geo)
            selected_price = normalize_price_key(text)

            if selected_price not in prices:
                send_king_price_options(chat_id, geo)
                return

            state["king_price"] = selected_price
            state["mode"] = KING_OCTO_MODE_DEPARTMENT
            set_state(user_id, state)

            send_department_menu(chat_id, "Выбери для кого king:")
            return

        if state.get("mode") == KING_OCTO_MODE_DEPARTMENT:
            if text not in [DEPT_CRYPTO, DEPT_GAMBLA]:
                send_department_menu(chat_id, "Нужно выбрать отдел кнопкой:")
                return

            state["king_department"] = text
            state["mode"] = KING_OCTO_MODE_FOR_WHOM
            set_state(user_id, state)

            send_person_menu(chat_id, text)
            return

        if state.get("mode") == KING_OCTO_MODE_FOR_WHOM:
            allowed_names = []

            if state.get("king_department") == DEPT_CRYPTO:
                allowed_names = CRYPTO_NAMES
            elif state.get("king_department") == DEPT_GAMBLA:
                allowed_names = GAMBLA_NAMES

            if text not in allowed_names:
                send_person_menu(chat_id, state.get("king_department"))
                return

            clean_name = normalize_person_name(text)
            count_needed = int(state.get("kings_count_requested", 0) or 0)

            state["king_for_whom"] = clean_name

            if count_needed <= 1:
                state["mode"] = KING_OCTO_MODE_NAME
                set_state(user_id, state)

                send_text_input_prompt(chat_id, "Какое название будет у king?")
                return

            free_items = find_free_kings_by_geo_and_price(
                count_needed,
                state.get("king_geo", ""),
                state.get("king_price", "")
            )

            if len(free_items) < count_needed:
                tg_send_message(
                    chat_id,
                    f"Недостаточно свободных king по GEO {state.get('king_geo', '')} и цене {state.get('king_price', '')}.\n"
                    f"Доступно: {len(free_items)}"
                )
                return

            state["king_selected_rows"] = free_items
            state["mode"] = KING_OCTO_MODE_BULK_NAMES
            set_state(user_id, state)

            tg_send_message(
                chat_id,
                f"Пришли {count_needed} названий для king.\n"
                f"Каждое название с новой строки."
            )
            return

        if state.get("mode") == KING_OCTO_MODE_NAME:
            king_name = str(text).strip()

            if not king_name:
                tg_send_message(chat_id, "Напиши название king.")
                return

            if king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое.")
                return

            found = find_free_king_by_geo_and_price(
                state.get("king_geo", ""),
                state.get("king_price", "")
            )

            if not found:
                clear_state(user_id)
                send_kings_menu(
                    chat_id,
                    f"Свободный king не найден.\nГео: {state.get('king_geo', '')}\nЦена: {state.get('king_price', '')}"
                )
                return

            state["king_name"] = king_name
            set_state(user_id, state)

            show_found_king_octo(chat_id, user_id, found)
            return

        if state.get("mode") == KING_OCTO_MODE_BULK_NAMES:
            names = [x.strip() for x in str(text).splitlines() if x.strip()]
            count_needed = int(state.get("kings_count_requested", 0) or 0)

            if len(names) != count_needed:
                tg_send_message(
                    chat_id,
                    f"Нужно прислать ровно {count_needed} названий.\nСейчас получено: {len(names)}"
                )
                return

            lower_names = [x.lower() for x in names]
            if len(lower_names) != len(set(lower_names)):
                tg_send_message(chat_id, "Названия не должны повторяться.")
                return

            duplicates = []
            for name in names:
                if king_name_exists(name):
                    duplicates.append(name)

            if duplicates:
                tg_send_message(
                    chat_id,
                    "Эти названия уже существуют:\n" + "\n".join(duplicates[:20])
                )
                return

            selected_rows = state.get("king_selected_rows", [])
            if len(selected_rows) != count_needed:
                selected_rows = find_free_kings_by_geo_and_price(
                    count_needed,
                    state.get("king_geo", ""),
                    state.get("king_price", "")
                )

            if len(selected_rows) < count_needed:
                tg_send_message(
                    chat_id,
                    f"Недостаточно свободных king по GEO {state.get('king_geo', '')} и цене {state.get('king_price', '')}.\n"
                    f"Доступно: {len(selected_rows)}"
                )
                return

            queue = []
            for item, king_name in zip(selected_rows, names):
                queue.append({
                    "row_index": item["row_index"],
                    "purchase_date": item["purchase_date"],
                    "price": item["price"],
                    "supplier": item["supplier"],
                    "geo": item["geo"],
                    "king_name": king_name,
                    "data_text": item.get("data_text", "")
                })

            state["kings_bulk_queue"] = queue
            state["kings_bulk_results"] = []
            state["kings_bulk_current_index"] = 0
            state["kings_bulk_username"] = username

            set_state_with_custom_ttl(user_id, state, KING_BULK_PROXY_TTL)

            send_kings_bulk_found_preview_once(chat_id, user_id)
            return

        if state.get("mode") == KING_OCTO_MODE_BULK_PROXY:
            process_kings_bulk_proxy_step(
                chat_id=chat_id,
                user_id=user_id,
                username=username,
                proxy_text=text
            )
            return
        
        if state.get("mode") == "awaiting_king_geo":
            geos = get_free_king_geos()
        
            if text not in geos:
                send_king_geo_options(chat_id)
                return
        
            state["king_geo"] = text
            state["mode"] = "awaiting_king_price"
            set_state(user_id, state)
        
            send_king_price_options(chat_id, text)
            return

        if state.get("mode") == "awaiting_king_price":
            geo = str(state.get("king_geo", "")).strip()
            prices = get_free_king_prices_by_geo(geo)
            selected_price = normalize_price_key(text)
        
            if selected_price not in prices:
                send_king_price_options(chat_id, geo)
                return
        
            state["king_price"] = selected_price
            state["mode"] = "awaiting_king_department"
            set_state(user_id, state)
        
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

            clean_name = normalize_person_name(text)

            state["mode"] = "awaiting_kings_count"
            state["king_for_whom"] = clean_name
            set_state(user_id, state)

            tg_send_message(chat_id, "Сколько кингов нужно?", [
                [{"text": BTN_BACK_STEP}, {"text": MENU_CANCEL}]
            ])
            return

        if state.get("mode") == "awaiting_kings_count":
            try:
                count_needed = int(text.strip())
            except Exception:
                tg_send_message(chat_id, "Впиши число.")
                return

            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            rows = get_sheet_rows_cached(SHEET_KINGS)
            candidates = []

            for idx, row in enumerate(rows[1:], start=2):
                if len(row) < 10:
                    row = row + [''] * (10 - len(row))

                if str(row[4]).strip().lower() != "free":
                    continue
                if str(row[7]).strip() != state.get("king_geo"):
                    continue

                purchase_date = parse_date(row[1]) or datetime.max
                candidates.append({
                    "row_index": idx,
                    "purchase_date_obj": purchase_date
                })

            candidates.sort(key=lambda x: x["purchase_date_obj"])
            selected = candidates[:count_needed]

            if len(selected) < count_needed:
                clear_state(user_id)
                send_kings_menu(chat_id, f"Недостаточно свободных кингов с таким GEO. Доступно: {len(selected)}")
                return

            state["mode"] = "awaiting_king_names_bulk"
            state["kings_count"] = count_needed
            state["king_rows"] = selected
            set_state(user_id, state)

            tg_send_message(
                chat_id,
                f"Пришли {count_needed} названий для кингов.\nКаждое название с новой строки."
            )
            return

        if state.get("mode") == "awaiting_king_names_bulk":
            names = [x.strip() for x in text.splitlines() if x.strip()]
            count_needed = state.get("kings_count", 0)

            if len(names) != count_needed:
                tg_send_message(chat_id, f"Нужно прислать ровно {count_needed} названий, каждое с новой строки.")
                return

            lower_names = [x.lower() for x in names]
            if len(lower_names) != len(set(lower_names)):
                tg_send_message(chat_id, "Названия не должны повторяться.")
                return

            for name in names:
                if king_name_exists(name):
                    tg_send_message(chat_id, f"Название '{name}' уже существует.")
                    return

            issue_kings_bulk(chat_id, user_id, username, names)
            return

        if state.get("mode") == "awaiting_king_name":
            king_name = text.strip()

            if not king_name:
                send_text_input_prompt(chat_id, "Напиши название кинга.")
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
                send_text_input_prompt(chat_id, "Впиши название кинга.")
                return

            result = build_king_search_text(king_name)
            clear_state(user_id)

            if not result:
                send_kings_menu(chat_id, "Кинг не найден.")
                return

            tg_send_king_search_result_as_txt(
                chat_id=chat_id,
                title=result["title"],
                king_name=result["king_name"],
                meta_text=result["meta_text"],
                data_text=result["data_text"]
            )

            send_kings_menu(chat_id, "Меню кингов:")
            return
            
        if state.get("mode") == "awaiting_return_king_name":
            king_name = text.strip()

            if not king_name:
                send_text_input_prompt(chat_id, "Впиши название кинга.")
                return

            normal_king = find_king_in_base_by_name(king_name)
            crypto_king = find_crypto_king_in_base_by_name(king_name)

            if not normal_king and not crypto_king:
                clear_state(user_id)
                send_kings_menu(chat_id, "Кинг не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_return_king_confirm",
                "return_king_name": king_name,
                "return_king_source": "crypto" if crypto_king and not normal_king else "normal"
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

        if state.get("mode") == "awaiting_return_king_free_name":
            king_name = text.strip()

            if not king_name:
                send_text_input_prompt(chat_id, "Впиши название кинга.")
                return

            found = find_king_in_base_by_name(king_name)
            if not found:
                clear_state(user_id)
                send_kings_menu(chat_id, "Кинг не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_return_king_free_confirm",
                "return_king_name": king_name
            })

            keyboard = [
                [{"text": BTN_KING_RETURN_FREE_CONFIRM}, {"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                f"Внимание: кинг '{king_name}' будет возвращён в free.\nПодтвердить?",
                keyboard
            )
            return


        if state.get("mode") == "awaiting_farm_return_king_free_name":
            king_name = text.strip()

            if not king_name:
                send_text_input_prompt(chat_id, "Впиши название кинга.")
                return

            found = find_farm_king_in_base_by_name(king_name)
            if not found:
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Кинг не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_farm_return_king_free_confirm",
                "return_king_name": king_name
            })

            keyboard = [
                [{"text": BTN_FARM_KING_RETURN_FREE_CONFIRM}, {"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                f"Внимание: farm king '{king_name}' будет возвращён в free.\nПодтвердить?",
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

            clean_name = normalize_person_name(text)

            state["mode"] = "awaiting_bm_count"
            state["bm_for_whom"] = clean_name
            set_state(user_id, state)

            send_text_input_prompt(chat_id, "Сколько БМов нужно?")
            return

        if state.get("mode") == "awaiting_bm_count":
            try:
                count_needed = int(text.strip())
            except Exception:
                tg_send_message(chat_id, "Впиши число.")
                return

            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            issue_bms_bulk(chat_id, user_id, username, count_needed)
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
            send_accounts_main_menu(chat_id, "Меню Accounts:")
            return

        if state.get("mode") == "awaiting_return_bm":
            bm_id = text.strip()

            if not bm_id:
                tg_send_message(chat_id, "Впиши ID БМа.")
                return

            bm_info = find_bm_in_base(bm_id)
            if not bm_info:
                clear_state(user_id)
                send_bms_menu(chat_id, "БМ не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_return_bm_confirm",
                "return_bm_id": bm_id
            })

            keyboard = [
                [{"text": BTN_BM_BAN_CONFIRM}],
                [{"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                f"Внимание: БМ '{bm_id}' будет перемещён в ban.\nПодтвердить?",
                keyboard
            )
            return

        if state.get("mode") == "awaiting_return_bm_free":
            bm_id = text.strip()
        
            if not bm_id:
                tg_send_message(chat_id, "Впиши ID БМа.")
                return
        
            bm_info = find_bm_in_base(bm_id)
            if not bm_info:
                clear_state(user_id)
                send_bms_menu(chat_id, "БМ не найден.")
                return
        
            set_state(user_id, {
                "mode": "awaiting_return_bm_free_confirm",
                "return_bm_id": bm_id
            })
        
            keyboard = [
                [{"text": BTN_BM_RETURN_FREE_CONFIRM}, {"text": MENU_CANCEL}]
            ]
        
            tg_send_message(
                chat_id,
                f"Внимание: БМ '{bm_id}' будет возвращён в free.\nПодтвердить?",
                keyboard
            )
            return

        if state.get("mode") == "awaiting_pixels_add":
            result = add_pixels_from_text(text)
            clear_state(user_id)
            tg_send_message(chat_id, result)

            if is_admin(user_id):
                send_admin_menu(chat_id, "Выбери следующее действие:")
            else:
                send_main_menu(chat_id, "Готово. Выбери следующее действие:", user_id=user_id)
            return


        if state.get("mode") == "awaiting_search_pixel":
            pixel_query = text.strip()

            if not pixel_query:
                tg_send_message(chat_id, "Впиши ID пикселя или данные Пикселя.")
                return

            result = build_pixel_search_text(pixel_query)
            clear_state(user_id)

            if not result:
                send_pixels_menu(chat_id, "Пиксель не найден.")
                return

            tg_send_message(chat_id, result)
            send_pixels_menu(chat_id, "Выбери следующее действие:")
            return


        if state.get("mode") == "awaiting_return_pixel":
            pixel_query = text.strip()

            if not pixel_query:
                tg_send_message(chat_id, "Впиши ID пикселя или данные Пикселя.")
                return

            found = find_pixel_in_base_by_data(pixel_query)
            if not found:
                clear_state(user_id)
                send_pixels_menu(chat_id, "Пиксель не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_return_pixel_confirm",
                "return_pixel_query": pixel_query
            })

            keyboard = [
                [{"text": BTN_PIXEL_BAN_CONFIRM}, {"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                "Внимание: Пиксель будет перемещён в ban.\nПодтвердить?",
                keyboard
            )
            return

        if state.get("mode") == "awaiting_return_pixel_free":
            pixel_query = text.strip()
        
            if not pixel_query:
                tg_send_message(chat_id, "Впиши ID пикселя или данные Пикселя.")
                return
        
            found = find_pixel_in_base_by_data(pixel_query)
            if not found:
                clear_state(user_id)
                send_pixels_menu(chat_id, "Пиксель не найден.")
                return
        
            set_state(user_id, {
                "mode": "awaiting_return_pixel_free_confirm",
                "return_pixel_query": pixel_query
            })
        
            keyboard = [
                [{"text": BTN_PIXEL_RETURN_FREE_CONFIRM}, {"text": MENU_CANCEL}]
            ]
        
            tg_send_message(
                chat_id,
                "Внимание: Пиксель будет возвращён в free.\nПодтвердить?",
                keyboard
            )
            return

        if state.get("mode") == "awaiting_pixel_department":
            if text not in [DEPT_CRYPTO, DEPT_GAMBLA]:
                send_department_menu(chat_id, "Нужно выбрать отдел кнопкой:")
                return

            state["mode"] = "awaiting_pixel_for_whom"
            state["pixel_department"] = text
            set_state(user_id, state)

            send_person_menu(chat_id, text)
            return


        if state.get("mode") == "awaiting_pixel_for_whom":
            allowed_names = []

            if state.get("pixel_department") == DEPT_CRYPTO:
                allowed_names = CRYPTO_NAMES
            elif state.get("pixel_department") == DEPT_GAMBLA:
                allowed_names = GAMBLA_NAMES

            if text not in allowed_names:
                send_person_menu(chat_id, state.get("pixel_department"))
                return

            clean_name = normalize_person_name(text)

            set_state(user_id, {
                "mode": "awaiting_pixel_count",
                "pixel_department": state.get("pixel_department"),
                "pixel_for_whom": clean_name
            })

            send_text_input_prompt(chat_id, "Сколько Пикселей нужно?")
            return

        if state.get("mode") == "awaiting_pixel_count":
            try:
                count_needed = int(text.strip())
            except Exception:
                tg_send_message(chat_id, "Впиши число.")
                return

            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            issue_pixels_bulk(chat_id, user_id, username, count_needed)
            return
        
                # ========= СОСТОЯНИЯ: фп =========
        if state.get("mode") == "awaiting_search_fp":
            lines = [x.strip() for x in str(text).splitlines() if x.strip()]
        
            if not lines:
                tg_send_message(chat_id, "Впиши ссылку ФП.")
                return
        
            results = build_fp_search_texts(lines)
            clear_state(user_id)
        
            found_any = False
        
            for item in results:
                tg_send_message(chat_id, item["text"])
                if item["found"]:
                    found_any = True
        
            if not found_any:
                send_fps_menu(chat_id, "Ни одно ФП не найдено.")
                return
        
            send_accounts_main_menu(chat_id, "Меню Accounts:")
            return

        if state.get("mode") == "awaiting_fp_department":
            if text not in [DEPT_CRYPTO, DEPT_GAMBLA]:
                send_department_menu(chat_id, "Нужно выбрать отдел кнопкой:")
                return

            state["mode"] = "awaiting_fp_for_whom"
            state["fp_department"] = text
            set_state(user_id, state)

            send_person_menu(chat_id, text)
            return

        if state.get("mode") == "awaiting_fp_for_whom":
            allowed_names = []

            if state.get("fp_department") == DEPT_CRYPTO:
                allowed_names = CRYPTO_NAMES
            elif state.get("fp_department") == DEPT_GAMBLA:
                allowed_names = GAMBLA_NAMES

            if text not in allowed_names:
                send_person_menu(chat_id, state.get("fp_department"))
                return

            clean_name = normalize_person_name(text)

            state["mode"] = "awaiting_fp_count"
            state["fp_for_whom"] = clean_name
            set_state(user_id, state)

            send_text_input_prompt(chat_id, "Сколько ФП нужно?")
            return

        if state.get("mode") == "awaiting_fp_count":
            try:
                count_needed = int(text.strip())
            except Exception:
                tg_send_message(chat_id, "Впиши число.")
                return

            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            issue_fps_bulk(chat_id, user_id, username, count_needed)
            return

        # ========= СОСТОЯНИЯ: FARM KING =========
        if state.get("mode") == FARM_KING_OCTO_MODE_SINGLE_PROXY:
            try:
                proxy_raw = text.strip()

                if proxy_raw == "__SKIP_PROXY_FARM_SINGLE__":
                    proxy_data = None
                else:
                    proxy_data = parse_proxy_input(proxy_raw)
                
                    if not proxy_data:
                        send_text_input_prompt(
                            chat_id,
                            "Неверный формат proxy.\n\nИспользуй:\nsocks5://login:password@host:port"
                        )
                        return

                king_row = state.get("farm_king_row")
                king_name = str(state.get("farm_king_name", "")).strip()
                parsed_farm_king = state.get("parsed_farm_king", {}) or {}
                geo_value = str(state.get("farm_king_geo_value", "")).strip()
                data_text = str(state.get("farm_king_data_text", "")).strip()
                sync_id = state.get("farm_king_sync_id")
                today = str(state.get("farm_king_today", "")).strip()
                who_took_text = str(state.get("farm_king_who_took_text", "")).strip()

                if not king_row or not king_name:
                    clear_state(user_id)
                    send_farm_kings_menu(chat_id, "Потеряны данные farm king. Начни заново.")
                    return

                rows = get_sheet_rows_cached(SHEET_FARM_KINGS, force=True)

                if king_row - 1 >= len(rows):
                    clear_state(user_id)
                    send_farm_kings_menu(chat_id, "Farm king не найден в таблице. Начни заново.")
                    return

                row = ensure_row_len(rows[king_row - 1], 13)
                status = str(row[4]).strip().lower()

                if status == "taken":
                    clear_state(user_id)
                    send_farm_kings_menu(chat_id, "Этот farm king уже занят.")
                    return

                if status == "ban":
                    clear_state(user_id)
                    send_farm_kings_menu(chat_id, "Этот farm king уже в ban.")
                    return

                if status != "free":
                    clear_state(user_id)
                    send_farm_kings_menu(chat_id, "Этот farm king недоступен.")
                    return

                octo_ok = False
                octo_msg = ""
                octo_result = None
                profile_uuid = ""

                cookies_ok = False
                cookies_msg = ""
                cookies_payload = None

                try:
                    octo_ok, octo_result = ensure_octo_profile_with_retry(
                        ensure_func=ensure_octo_profile_for_farm_king,
                        profile_name=king_name,
                        parsed=parsed_farm_king,
                        proxy_data=proxy_data
                    )
                    octo_msg = str(octo_result)
                except Exception as octo_error:
                    logging.exception("FARM_KING_PROXY_STEP: Octo create crashed")
                    octo_msg = str(octo_error)

                if not octo_ok:
                    tg_send_long_message(
                        chat_id,
                        f"❌ Не удалось создать Octo профиль\n{octo_msg or 'неизвестная ошибка'}"
                    )
                    return

                try:
                    profile_uuid = extract_octo_profile_uuid_from_result(octo_result)
                    cookies_payload = normalize_crypto_cookies_for_import(
                        parsed_farm_king.get("cookies_json", "")
                    )

                    if cookies_payload and profile_uuid:
                        cookies_ok, cookies_msg = try_import_crypto_king_cookies(
                            profile_uuid=profile_uuid,
                            cookies_payload=cookies_payload
                        )
                    else:
                        cookies_msg = "cookies не найдены или profile_uuid пустой"
                except Exception as cookies_error:
                    logging.exception("FARM_KING_PROXY_STEP: cookies import crashed")
                    cookies_msg = str(cookies_error)

                sheet_update_and_refresh(
                    SHEET_FARM_KINGS,
                    f"A{king_row}:L{king_row}",
                    [[
                        king_name,
                        row[1],
                        row[2],
                        row[3],
                        "taken",
                        "farm",
                        today,
                        geo_value,
                        who_took_text,
                        row[9],
                        row[10],
                        row[11]
                    ]]
                )

                if sync_id:
                    sync_status_to_basebot(BASEBOT_SHEET_FARM_KINGS, sync_id, "taken")

                append_king_to_issues_sheet(
                    king_name=king_name,
                    purchase_date=row[1],
                    price=row[2],
                    transfer_date=today,
                    supplier=row[3],
                    for_whom="farm"
                )

                invalidate_stats_cache()

                preview_message_id = state.get("farm_king_preview_message_id")

                set_state(user_id, {
                    "mode": "farm_king_octo_issued",
                    "last_farm_king_name": king_name,
                    "last_farm_king_data_text": data_text,
                    "farm_king_preview_message_id": preview_message_id
                })

                if preview_message_id:
                    mark_farm_king_octo_preview_as_issued(
                        chat_id=chat_id,
                        message_id=preview_message_id,
                        king_name=king_name,
                        price=row[2],
                        geo_value=parsed_farm_king.get("geo", geo_value)
                    )

                tg_send_inline_message(
                    chat_id,
                    f"✅ Farm king заведен в Octo\n\n"
                    f"✏️Название: {king_name}\n"
                    f"👨‍💻Для кого: farm\n"
                    f"💵Цена: {row[2]}\n"
                    f"🌐Гео: {parsed_farm_king.get('geo', geo_value)}",
                    [[{
                        "text": "📄 Скачать txt",
                        "callback_data": f"download_farm_king_txt:{user_id}"
                    }]]
                )

                tg_send_message(
                    chat_id,
                    f"Вручную проверь и выставь:\n"
                    f"• User-Agent\n"
                    f"• расширения\n\n"
                    f"• куки\n\n"
                    f"User-Agent:\n{parsed_farm_king.get('user_agent', '')}"
                )

                if parsed_farm_king.get("bm_links") or parsed_farm_king.get("bm_email_pairs"):
                    bm_parts = []

                    if parsed_farm_king.get("bm_links"):
                        bm_parts.append("BM ссылки:")
                        bm_parts.extend(parsed_farm_king["bm_links"])

                    if parsed_farm_king.get("bm_email_pairs"):
                        if bm_parts:
                            bm_parts.append("")
                        bm_parts.append("Почты/пароли от BM:")
                        bm_parts.extend(parsed_farm_king["bm_email_pairs"])

                    tg_send_long_message(
                        chat_id,
                        "По этому farm king ещё есть BM данные:\n\n" + "\n".join(bm_parts)
                    )

                if parsed_farm_king.get("cookies_links"):
                    tg_send_long_message(
                        chat_id,
                        "Cookies даны ссылкой. Импорт в профиль нужно сделать вручную:\n\n" +
                        "\n".join(parsed_farm_king["cookies_links"])
                    )

                return

            except Exception:
                logging.exception("farm king issue crashed")
                tg_send_message(chat_id, "Ошибка выдачи farm king")

        if state.get("mode") == FARM_KING_OCTO_MODE_COUNT:
            try:
                count_needed = int(text.strip())
            except Exception:
                tg_send_message(chat_id, "Впиши число.")
                return

            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            state["farm_kings_count_requested"] = count_needed
            state["mode"] = FARM_KING_OCTO_MODE_GEO
            set_state(user_id, state)

            send_farm_king_geo_options(chat_id)
            return

        if state.get("mode") == FARM_KING_OCTO_MODE_GEO:
            geos = get_free_farm_king_geos()

            if text not in geos:
                send_farm_king_geo_options(chat_id)
                return

            state["farm_king_geo"] = text
            count_needed = int(state.get("farm_kings_count_requested", 0) or 0)

            if count_needed <= 1:
                state["mode"] = FARM_KING_OCTO_MODE_NAME
                set_state(user_id, state)
                send_text_input_prompt(chat_id, "Какое название будет у farm king?")
                return

            free_items = find_free_farm_kings(
                count_needed,
                geo=text
            )

            if len(free_items) < count_needed:
                tg_send_message(
                    chat_id,
                    f"Недостаточно свободных farm king по GEO {text}.\n"
                    f"Доступно: {len(free_items)}"
                )
                return

            state["mode"] = FARM_KING_OCTO_MODE_BULK_NAMES
            set_state(user_id, state)

            send_text_input_prompt(
                chat_id,
                f"Пришли {count_needed} названий для farm king.\nКаждое название с новой строки."
            )
            return

        if state.get("mode") == FARM_KING_OCTO_MODE_NAME:
            king_name = str(text).strip()
        
            if not king_name:
                tg_send_message(chat_id, "Напиши название farm king.")
                return
        
            if farm_king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует в фарм базе.")
                return
        
            found_list = find_free_farm_kings(1, geo=state.get("farm_king_geo", ""))
            found_item = found_list[0] if found_list else None
        
            if not found_item:
                clear_state(user_id)
                send_farm_kings_menu(
                    chat_id,
                    f"Свободный farm king не найден.\nГео: {state.get('farm_king_geo', '')}"
                )
                return
        
            row = ensure_row_len(found_item["row"], 13)
        
            found = {
                "row_index": found_item["row_index"],
                "purchase_date": row[1],
                "price": row[2],
                "supplier": row[3],
                "geo": row[7],
                "data_text": get_full_king_data_from_row(row),
                "row": row
            }
        
            state["farm_king_name"] = king_name
            set_state(user_id, state)
        
            show_found_farm_king_octo(chat_id, user_id, found)
            return

        if state.get("mode") == FARM_KING_OCTO_MODE_BULK_NAMES:
            names = [x.strip() for x in str(text).splitlines() if x.strip()]
            count_needed = int(state.get("farm_kings_count_requested", 0) or 0)

            if len(names) != count_needed:
                tg_send_message(
                    chat_id,
                    f"Нужно прислать ровно {count_needed} названий.\nСейчас получено: {len(names)}"
                )
                return

            lower_names = [x.lower() for x in names]
            if len(lower_names) != len(set(lower_names)):
                tg_send_message(chat_id, "Названия не должны повторяться.")
                return

            duplicates = []
            for name in names:
                if farm_king_name_exists(name):
                    duplicates.append(name)

            if duplicates:
                tg_send_message(
                    chat_id,
                    "Эти названия уже существуют:\n" + "\n".join(duplicates[:20])
                )
                return

            selected_rows = find_free_farm_kings(
                count_needed,
                geo=state.get("farm_king_geo", "")
            )

            if len(selected_rows) < count_needed:
                tg_send_message(
                    chat_id,
                    f"Недостаточно свободных farm king по GEO {state.get('farm_king_geo', '')}.\n"
                    f"Доступно: {len(selected_rows)}"
                )
                return

            queue = []
            for item, king_name in zip(selected_rows, names):
                queue.append({
                    "row_index": item["row_index"],
                    "purchase_date": item["row"][1],
                    "price": item["row"][2],
                    "supplier": item["row"][3],
                    "geo": item["row"][7],
                    "king_name": king_name,
                    "data_text": get_full_king_data_from_row(item["row"])
                })

            state["farm_kings_bulk_queue"] = queue
            state["farm_kings_bulk_results"] = []
            state["farm_kings_bulk_current_index"] = 0
            state["farm_kings_bulk_username"] = username

            set_state_with_custom_ttl(user_id, state, FARM_KING_BULK_PROXY_TTL)

            send_farm_kings_bulk_found_preview_once(chat_id, user_id)
            return

        if state.get("mode") == FARM_KING_OCTO_MODE_BULK_PROXY:
            process_farm_kings_bulk_proxy_step(
                chat_id=chat_id,
                user_id=user_id,
                username=username,
                proxy_text=text
            )
            return
        
        if state.get("mode") == "awaiting_farm_king_geo":
            geos = get_free_farm_king_geos()

            if text not in geos:
                send_farm_king_geo_options(chat_id)
                return

            set_state(user_id, {
                "mode": "awaiting_farm_kings_count",
                "farm_king_geo": text
            })

            send_text_input_prompt(chat_id, f"Сколько кингов нужно для GEO {text}?")
            return

        if state.get("mode") == "awaiting_farm_kings_count":
            try:
                count_needed = int(text.strip())
            except Exception:
                tg_send_message(chat_id, "Впиши число.")
                return

            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            selected_geo = state.get("farm_king_geo")
            found = find_free_farm_kings(count_needed, geo=selected_geo)

            all_for_geo = find_free_farm_kings(999999, geo=selected_geo)
            available_count = len(all_for_geo)

            if available_count == 0:
                clear_state(user_id)
                send_farm_kings_menu(chat_id, f"Свободных кингов с GEO {selected_geo} нет.")
                return

            if available_count < count_needed:
                set_state(user_id, {
                    "mode": "awaiting_farm_kings_partial_confirm",
                    "farm_king_geo": selected_geo,
                    "farm_kings_count": available_count,
                    "farm_king_rows": all_for_geo
                })

                keyboard = [
                    [{"text": BTN_FARM_KINGS_PARTIAL_CONFIRM}, {"text": BTN_FARM_KINGS_PARTIAL_CANCEL}]
                ]

                tg_send_message(
                    chat_id,
                    f"Есть только {available_count} кингов с GEO {selected_geo}.\nВыдаю?",
                    keyboard
                )
                return

            set_state(user_id, {
                "mode": "awaiting_farm_king_names",
                "farm_king_geo": selected_geo,
                "farm_kings_count": count_needed,
                "farm_king_rows": found
            })
            
            send_text_input_prompt(
                chat_id,
                f"Пришли {count_needed} названий для кингов.\nКаждое название с новой строки."
            )
            return

        if state.get("mode") == "awaiting_farm_king_names":
            names = [x.strip() for x in text.splitlines() if x.strip()]
            count_needed = state.get("farm_kings_count", 0)

            if len(names) != count_needed:
                tg_send_message(chat_id, f"Нужно прислать ровно {count_needed} названий, каждое с новой строки.")
                return

            lower_names = [x.lower() for x in names]
            if len(lower_names) != len(set(lower_names)):
                tg_send_message(chat_id, "Названия не должны повторяться.")
                return

            for name in names:
                if farm_king_name_exists(name):
                    tg_send_message(chat_id, f"Название '{name}' уже существует в фарм базе.")
                    return

            issue_farm_kings(chat_id, user_id, username, names)
            return

        if state.get("mode") == "awaiting_farm_search_king_name":
            king_name = text.strip()

            if not king_name:
                send_text_input_prompt(chat_id, "Впиши название кинга.")
                return

            result = build_farm_king_search_text(king_name)
            clear_state(user_id)

            if not result:
                send_farm_kings_menu(chat_id, "Кинг не найден.")
                return

            tg_send_king_search_result_as_txt(
                chat_id=chat_id,
                title=result["title"],
                king_name=result["king_name"],
                meta_text=result["meta_text"],
                data_text=result["data_text"]
            )

            send_farm_kings_menu(chat_id, "Выбери следующее действие:")
            return

        if state.get("mode") == "awaiting_farm_return_king_name":
            king_name = text.strip()

            if not king_name:
                send_text_input_prompt(chat_id, "Впиши название кинга.")
                return

            found = find_farm_king_in_base_by_name(king_name)
            if not found:
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Кинг не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_ban_reason_farm_king",
                "return_king_name": king_name
            })
            send_text_input_prompt(chat_id, "Напиши причину бана для farm king.")
            return


        if state.get("mode") == "awaiting_ban_reason_farm_king":
            comment_text = text.strip()

            if not comment_text:
                send_text_input_prompt(chat_id, "Напиши причину бана для farm king.")
                return

            king_name = state.get("return_king_name", "")
            ok, message = return_farm_king_to_ban(king_name, comment_text)
            clear_state(user_id)
            send_farm_kings_menu(chat_id, message)
            return

        # ========= СОСТОЯНИЯ: FARM BM =========
        if state.get("mode") == "awaiting_farm_search_bm":
            bm_id = text.strip()

            if not bm_id:
                send_text_input_prompt(chat_id, "Впиши ID BM.")
                return

            result = build_farm_bm_search_text(bm_id)
            clear_state(user_id)

            if not result:
                send_farm_bms_menu(chat_id, "BM не найден.")
                return

            tg_send_message(chat_id, result)
            send_farm_bms_menu(chat_id, "Выбери следующее действие:")
            return

        if state.get("mode") == "awaiting_farm_return_bm":
            bm_id = text.strip()

            if not bm_id:
                tg_send_message(chat_id, "Впиши ID BM.")
                return

            found = find_farm_bm_in_base(bm_id)
            if not found:
                clear_state(user_id)
                send_farm_bms_menu(chat_id, "BM не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_farm_return_bm_confirm",
                "return_farm_bm_id": bm_id
            })

            keyboard = [
                [{"text": BTN_BM_BAN_CONFIRM}],
                [{"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                f"Внимание: BM '{bm_id}' будет перемещён в ban.\nПодтвердить?",
                keyboard
            )
            return

        if state.get("mode") == "awaiting_farm_return_bm_free":
            bm_id = text.strip()

            if not bm_id:
                tg_send_message(chat_id, "Впиши ID farm BM.")
                return

            found = find_farm_bm_in_base(bm_id)
            if not found:
                clear_state(user_id)
                send_farm_bms_menu(chat_id, "BM не найден.")
                return

            set_state(user_id, {
                "mode": "awaiting_farm_return_bm_free_confirm",
                "return_farm_bm_id": bm_id
            })

            keyboard = [
                [{"text": BTN_FARM_BM_RETURN_FREE_CONFIRM}, {"text": MENU_CANCEL}]
            ]

            tg_send_message(
                chat_id,
                f"Внимание: farm BM '{bm_id}' будет возвращён в free.\nПодтвердить?",
                keyboard
            )
            return

        # ========= СОСТОЯНИЯ: FARM FP =========
        if state.get("mode") == "awaiting_farm_fp_count":
            try:
                count_needed = int(text.strip())
            except Exception:
                tg_send_message(chat_id, "Впиши число.")
                return

            if count_needed <= 0:
                tg_send_message(chat_id, "Количество должно быть больше нуля.")
                return

            clear_state(user_id)
            issue_farm_fps(chat_id, user_id, username, count_needed)
            return

        if state.get("mode") == "awaiting_farm_search_fp":
            fp_link = text.strip()

            if not fp_link:
                send_text_input_prompt(chat_id, "Впиши ссылку FP.")
                return

            result = build_farm_fp_search_text(fp_link)
            clear_state(user_id)

            if not result:
                send_farm_fps_menu(chat_id, "FP не найдено.")
                return

            tg_send_message(chat_id, result)
            send_farm_fps_menu(chat_id, "Выбери следующее действие:")
            return

        if state.get("mode") == "awaiting_ban_reason_account":
            comment_text = text.strip()
            if not comment_text:
                send_text_input_prompt(chat_id, "Напиши причину бана для лички.")
                return

            account_number = state.get("return_account_number", "")
            ok, message = return_account_to_ban(account_number, comment_text)
            clear_state(user_id)
            send_accounts_main_menu(chat_id, message)
            return


        if state.get("mode") == "awaiting_ban_reason_king":
            comment_text = text.strip()
            if not comment_text:
                send_text_input_prompt(chat_id, "Напиши причину бана для кинга.")
                return

            king_name = state.get("return_king_name", "")
            source = state.get("return_king_source", "normal")

            if source == "crypto":
                ok, message = return_crypto_king_to_ban(king_name, comment_text)
            else:
                ok, message = return_king_to_ban(king_name, comment_text)

            clear_state(user_id)
            send_kings_menu(chat_id, message)
            return


        if state.get("mode") == "awaiting_ban_reason_bm":
            comment_text = text.strip()
            if not comment_text:
                send_text_input_prompt(chat_id, "Напиши причину бана для БМа.")
                return

            bm_id = state.get("return_bm_id", "")
            ok, message = return_bm_to_ban(bm_id, comment_text)
            clear_state(user_id)
            send_bms_menu(chat_id, message)
            return


        if state.get("mode") == "awaiting_ban_reason_pixel":
            comment_text = text.strip()
            if not comment_text:
                send_text_input_prompt(chat_id, "Напиши причину бана для Пикселя.")
                return

            pixel_query = state.get("return_pixel_query", "")
            ok, message = return_pixel_to_ban(pixel_query, comment_text)
            clear_state(user_id)
            send_pixels_menu(chat_id, message)
            return


        if state.get("mode") == "awaiting_ban_reason_farm_bm":
            comment_text = text.strip()
            if not comment_text:
                send_text_input_prompt(chat_id, "Напиши причину бана для farm BM.")
                return

            bm_id = state.get("return_farm_bm_id", "")
            ok, message = return_farm_bm_to_ban(bm_id, comment_text)
            clear_state(user_id)
            send_farm_bms_menu(chat_id, message)
            return


        if state.get("mode") == "awaiting_ban_reason_fp":
            comment_text = text.strip()
            if not comment_text:
                send_text_input_prompt(chat_id, "Напиши причину бана для ФП.")
                return

            fp_link = state.get("return_fp_link", "")
            ok, message = return_fp_to_ban(fp_link, comment_text)
            clear_state(user_id)
            send_fps_menu(chat_id, message)
            return


        if state.get("mode") == "awaiting_ban_reason_farm_fp":
            comment_text = text.strip()
            if not comment_text:
                send_text_input_prompt(chat_id, "Напиши причину бана для farm FP.")
                return

            fp_link = state.get("return_fp_link", "")
            ok, message = return_farm_fp_to_ban(fp_link, comment_text)
            clear_state(user_id)
            send_farm_fps_menu(chat_id, message)
            return

        if state.get("mode") == "awaiting_crypto_king_octo_proxy":
            try:
                logging.info("CRYPTO_PROXY_STEP: entered")
        
                proxy_raw = text.strip()
                logging.info(f"CRYPTO_PROXY_STEP: proxy_raw={proxy_raw}")
        
                proxy_data = parse_proxy_input(proxy_raw)
                logging.info(f"CRYPTO_PROXY_STEP: proxy_data={proxy_data}")
        
                if not proxy_data:
                    send_text_input_prompt(
                        chat_id,
                        "Неверный формат proxy.\n\nИспользуй:\nsocks5://login:password@host:port"
                    )
                    return
        
                king_row = state.get("king_row")
                king_name = str(state.get("king_name", "")).strip()
                king_for_whom = str(state.get("king_for_whom", "")).strip()
                parsed_crypto = state.get("parsed_crypto", {}) or {}
                geo_value = str(state.get("crypto_geo_value", "")).strip()
                data_text = str(state.get("crypto_data_text", "")).strip()
                sync_id = state.get("crypto_sync_id")
                today = str(state.get("crypto_today", "")).strip()
                who_took_text = str(state.get("crypto_who_took_text", "")).strip()
        
                if not king_row or not king_name or not king_for_whom:
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Потеряны данные crypto king. Начни заново.")
                    return
        
                rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS, force=True)
        
                if king_row - 1 >= len(rows):
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Crypto king не найден в таблице. Начни заново.")
                    return
        
                row = ensure_row_len(rows[king_row - 1], 13)
                status = str(row[4]).strip().lower()
        
                if status == "taken":
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Этот crypto king уже занят.")
                    return
        
                if status == "ban":
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Этот crypto king уже в ban.")
                    return
        
                if status != "free":
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Этот crypto king недоступен.")
                    return
        
                octo_ok = False
                octo_msg = ""
                octo_result = None
                profile_uuid = ""
        
                cookies_ok = False
                cookies_msg = ""
                cookies_payload = None
        
                try:
                    octo_ok, octo_result = ensure_octo_profile_with_retry(
                        ensure_func=ensure_octo_profile_for_crypto_king,
                        profile_name=king_name,
                        parsed=parsed_crypto,
                        proxy_data=proxy_data
                    )
                    octo_msg = str(octo_result)
                    logging.info(f"CRYPTO_PROXY_STEP: octo_ok={octo_ok}, octo_result={octo_result}")
                except Exception as octo_error:
                    logging.exception("CRYPTO_PROXY_STEP: Octo create crashed")
                    octo_msg = str(octo_error)

                if not octo_ok:
                    tg_send_long_message(
                        chat_id,
                        f"❌ Не удалось создать Octo профиль\n{octo_msg or 'неизвестная ошибка'}"
                    )
                    return
        
                if octo_ok:
                    try:
                        profile_uuid = extract_octo_profile_uuid_from_result(octo_result)
                        cookies_payload = normalize_crypto_cookies_for_import(
                            parsed_crypto.get("cookies_json", "")
                        )
        
                        if cookies_payload and profile_uuid:
                            cookies_ok, cookies_msg = try_import_crypto_king_cookies(
                                profile_uuid=profile_uuid,
                                cookies_payload=cookies_payload
                            )
                        else:
                            cookies_msg = "cookies не найдены или profile_uuid пустой"
                    except Exception as cookies_error:
                        logging.exception("CRYPTO_PROXY_STEP: cookies import crashed")
                        cookies_msg = str(cookies_error)
        
                sheet_update_and_refresh(
                    SHEET_CRYPTO_KINGS,
                    f"A{king_row}:L{king_row}",
                    [[
                        king_name,
                        row[1],
                        row[2],
                        row[3],
                        "taken",
                        king_for_whom,
                        today,
                        geo_value,
                        who_took_text,
                        row[9],
                        row[10],
                        row[11]
                    ]]
                )
        
                if sync_id:
                    sync_status_to_basebot(BASEBOT_SHEET_CRYPTO_KINGS, sync_id, "taken")
        
                append_king_to_issues_sheet(
                    king_name=king_name,
                    purchase_date=row[1],
                    price=row[2],
                    transfer_date=today,
                    supplier=row[3],
                    for_whom=king_for_whom
                )
        
                invalidate_stats_cache()
        
                preview_message_id = state.get("crypto_preview_message_id")
        
                set_state(user_id, {
                    "mode": "crypto_king_issued",
                    "last_crypto_king_name": king_name,
                    "last_crypto_king_data_text": data_text,
                    "crypto_preview_message_id": preview_message_id,
                })
        
                if preview_message_id:
                    mark_crypto_king_preview_as_issued(
                        chat_id=chat_id,
                        message_id=preview_message_id,
                        king_name=king_name,
                        king_for_whom=king_for_whom,
                        price=row[2],
                        geo_value=parsed_crypto.get("geo", geo_value)
                    )
        
                tg_send_inline_message(
                    chat_id,
                    f"✅ Кинг заведен в Octo\n\n"
                    f"✏️Название: {king_name}\n"
                    f"👨‍💻Для кого: {king_for_whom}\n"
                    f"💵Цена: {row[2]}\n"
                    f"🌐Гео: {parsed_crypto.get('geo', geo_value)}",
                    [[{
                        "text": "📄 Скачать txt",
                        "callback_data": f"download_crypto_txt:{user_id}"
                    }]]
                )

                tg_send_message(
                    chat_id,
                    f"Вручную проверь и выставь:\n"
                    f"• User-Agent\n"
                    f"• расширения\n\n"
                    f"• куки\n\n"
                    f"User-Agent:\n{parsed_crypto.get('user_agent', '')}"
                )
        
                if not octo_ok and octo_msg:
                    tg_send_long_message(chat_id, f"Ошибка Octo:\n{octo_msg}")
        
                if parsed_crypto.get("bm_links") or parsed_crypto.get("bm_email_pairs"):
                    bm_parts = []
        
                    if parsed_crypto.get("bm_links"):
                        bm_parts.append("BM ссылки:")
                        bm_parts.extend(parsed_crypto["bm_links"])
        
                    if parsed_crypto.get("bm_email_pairs"):
                        if bm_parts:
                            bm_parts.append("")
                        bm_parts.append("Почты/пароли от BM:")
                        bm_parts.extend(parsed_crypto["bm_email_pairs"])
        
                    tg_send_long_message(
                        chat_id,
                        "По этому crypto king ещё есть BM данные:\n\n" + "\n".join(bm_parts)
                    )
        
                if parsed_crypto.get("cookies_links"):
                    tg_send_long_message(
                        chat_id,
                        "Cookies даны ссылкой. Импорт в профиль нужно сделать вручную:\n\n" +
                        "\n".join(parsed_crypto["cookies_links"])
                    )
        
                return

            except Exception as e:
                 logging.exception("crypto king issue crashed")
                 tg_send_message(chat_id, "Ошибка выдачи crypto king")

        if state.get("mode") == "awaiting_octo_proxy_for_warehouse":
            proxy_raw = text.strip()
            proxy_data = parse_proxy_input(text)

            if not proxy_data:
                send_text_input_prompt(
                    chat_id,
                    "Неверный формат proxy.\n\nИспользуй:\nip:port\nили\nip:port:login:password"
                )
                return

            warehouses_queue = list(state.get("octo_warehouses_queue", []))
            created_profiles = list(state.get("octo_created_profiles", []))
            failed_profiles = list(state.get("octo_failed_profiles", []))
            octo_target_sheet = state.get("octo_target_sheet", "")
            last_accounts_section = state.get("last_accounts_section", "")
            last_farmers_section = state.get("last_farmers_section", "")

            if not warehouses_queue:
                clear_state(user_id)
                tg_send_message(chat_id, "Очередь складов для Octo пуста.")
                send_main_menu(chat_id, user_id=user_id)
                return

            current_warehouse = warehouses_queue.pop(0)

            try:
                ok, result = ensure_octo_profile_for_warehouse(current_warehouse, proxy_data)

                if not ok:
                    failed_profiles.append(f"{current_warehouse}: {result}")

                    if warehouses_queue:
                        set_state(user_id, {
                            "mode": "awaiting_octo_proxy_for_warehouse",
                            "octo_target_sheet": octo_target_sheet,
                            "octo_warehouses_queue": warehouses_queue,
                            "octo_created_profiles": created_profiles,
                            "octo_failed_profiles": failed_profiles,
                            "last_accounts_section": last_accounts_section,
                            "last_farmers_section": last_farmers_section,
                        })

                        send_text_input_prompt(
                            chat_id,
                            f"Ошибка Octo для склада {current_warehouse}:\n{result}\n\n"
                            f"Пришли proxy для следующего склада:\n{warehouses_queue[0]}\n\n"
                            f"Формат:\nip:port\nили\nip:port:login:password"
                        )
                        return

                    clear_state(user_id)

                    summary = (
                        "Создание Octo профилей завершено.\n\n"
                        f"Успешно: {len(created_profiles)}\n"
                        f"Ошибок: {len(failed_profiles)}"
                    )

                    if created_profiles:
                        summary += "\n\nСозданы:\n" + "\n".join(created_profiles[:20])

                    if failed_profiles:
                        summary += "\n\nОшибки:\n" + "\n".join(failed_profiles[:20])

                    tg_send_long_message(chat_id, summary)

                    if is_admin(user_id):
                        send_admin_menu(chat_id, "Меню Admin:")
                    else:
                        send_main_menu(chat_id, "Главное меню:", user_id=user_id)
                    return

                profile_uuid = extract_octo_profile_uuid(result)

                if not profile_uuid:
                    existing = octo_find_profile_by_title(current_warehouse)
                    profile_uuid = extract_octo_profile_uuid(existing)

                if not profile_uuid:
                    failed_profiles.append(f"{current_warehouse}: не удалось получить profile_uuid")

                    if warehouses_queue:
                        set_state(user_id, {
                            "mode": "awaiting_octo_proxy_for_warehouse",
                            "octo_target_sheet": octo_target_sheet,
                            "octo_warehouses_queue": warehouses_queue,
                            "octo_created_profiles": created_profiles,
                            "octo_failed_profiles": failed_profiles,
                            "last_accounts_section": last_accounts_section,
                            "last_farmers_section": last_farmers_section,
                        })

                        send_text_input_prompt(
                            chat_id,
                            f"Для склада {current_warehouse} профиль создался, но profile_uuid не найден.\n\n"
                            f"Пришли proxy для следующего склада:\n{warehouses_queue[0]}\n\n"
                            f"Формат:\nip:port\nили\nip:port:login:password"
                        )
                        return

                    logging.info("OCTO_PROXY_HANDLER_V2")

                    clear_state(user_id)

                    summary = (
                        "Создание Octo профилей завершено.\n\n"
                        f"Успешно: {len(created_profiles)}\n"
                        f"Ошибок: {len(failed_profiles)}"
                    )

                    if created_profiles:
                        summary += "\n\nСозданы:\n" + "\n".join(created_profiles[:20])

                    if failed_profiles:
                        summary += "\n\nОшибки:\n" + "\n".join(failed_profiles[:20])

                    tg_send_long_message(chat_id, summary)

                    if is_admin(user_id):
                        send_admin_menu(chat_id, "Меню Admin:")
                    else:
                        send_main_menu(chat_id, "Главное меню:", user_id=user_id)
                    return

                created_profiles.append(current_warehouse)

                set_state(user_id, {
                    "mode": "awaiting_octo_king_data",
                    "octo_profile_uuid": profile_uuid,
                    "octo_profile_name": current_warehouse,
                    "octo_warehouse_name": current_warehouse,
                    "octo_target_sheet": octo_target_sheet,
                    "octo_warehouses_queue": warehouses_queue,
                    "octo_created_profiles": created_profiles,
                    "octo_failed_profiles": failed_profiles,
                    "last_accounts_section": last_accounts_section,
                    "last_farmers_section": last_farmers_section,
                })

                tg_send_message(chat_id, f"Octo профиль создан/готов:\n{current_warehouse}")
                send_octo_king_data_instructions(chat_id, current_warehouse)
                return

            except Exception as e:
                logging.exception("Octo profile create crashed")
                failed_profiles.append(f"{current_warehouse}: {e}")

                if warehouses_queue:
                    set_state(user_id, {
                        "mode": "awaiting_octo_proxy_for_warehouse",
                        "octo_target_sheet": octo_target_sheet,
                        "octo_warehouses_queue": warehouses_queue,
                        "octo_created_profiles": created_profiles,
                        "octo_failed_profiles": failed_profiles,
                        "last_accounts_section": last_accounts_section,
                        "last_farmers_section": last_farmers_section,
                    })

                    send_text_input_prompt(
                        chat_id,
                        f"Ошибка Octo для склада {current_warehouse}:\n{e}\n\n"
                        f"Пришли proxy для следующего склада:\n{warehouses_queue[0]}\n\n"
                        f"Формат:\nip:port\nили\nip:port:login:password"
                    )
                    return

                clear_state(user_id)

                summary = (
                    "Создание Octo профилей завершено.\n\n"
                    f"Успешно: {len(created_profiles)}\n"
                    f"Ошибок: {len(failed_profiles)}"
                )

                if created_profiles:
                    summary += "\n\nСозданы:\n" + "\n".join(created_profiles[:20])

                if failed_profiles:
                    summary += "\n\nОшибки:\n" + "\n".join(failed_profiles[:20])

                tg_send_long_message(chat_id, summary)

                if is_admin(user_id):
                    send_admin_menu(chat_id, "Меню Admin:")
                else:
                    send_main_menu(chat_id, "Главное меню:", user_id=user_id)
                return

        if state.get("mode") == "awaiting_octo_king_data":
            raw_king_text = text.strip()

            if not raw_king_text:
                send_octo_king_data_instructions(chat_id, state.get("octo_warehouse_name", ""))
                return

            profile_uuid = str(state.get("octo_profile_uuid", "")).strip()
            warehouse_name = str(state.get("octo_warehouse_name", "")).strip()
            warehouses_queue = list(state.get("octo_warehouses_queue", []))
            created_profiles = list(state.get("octo_created_profiles", []))
            failed_profiles = list(state.get("octo_failed_profiles", []))
            octo_target_sheet = state.get("octo_target_sheet", "")
            last_accounts_section = state.get("last_accounts_section", "")
            last_farmers_section = state.get("last_farmers_section", "")

            if not profile_uuid:
                clear_state(user_id)
                tg_send_message(chat_id, "Не найден profile_uuid Octo. Начни заново.")
                return

            parsed = parse_king_data_block(raw_king_text)
            description_text = build_octo_description_from_king_data(parsed)

            try:
                octo_update_profile_description(profile_uuid, description_text)
            except Exception as e:
                logging.exception("octo_update_profile_description crashed")
                tg_send_message(chat_id, f"Профиль создан, но описание в Octo не обновилось:\n{e}")
                return

            tg_send_message(
                chat_id,
                f"Готово ✅\n\n"
                f"Профиль Octo обновлен.\n"
                f"Склад: {warehouse_name}\n"
                f"Описание кинга записано."
            )

            if warehouses_queue:
                set_state(user_id, {
                    "mode": "awaiting_octo_proxy_for_warehouse",
                    "octo_target_sheet": octo_target_sheet,
                    "octo_warehouses_queue": warehouses_queue,
                    "octo_created_profiles": created_profiles,
                    "octo_failed_profiles": failed_profiles,
                    "last_accounts_section": last_accounts_section,
                    "last_farmers_section": last_farmers_section,
                })

                send_text_input_prompt(
                    chat_id,
                    f"Пришли proxy для следующего склада:\n{warehouses_queue[0]}\n\nФормат:\nip:port\nили\nip:port:login:password"
                )
                return

            clear_state(user_id)

            summary = (
                "Создание Octo профилей завершено.\n\n"
                f"Успешно: {len(created_profiles)}\n"
                f"Ошибок: {len(failed_profiles)}"
            )

            if created_profiles:
                summary += "\n\nСозданы:\n" + "\n".join(created_profiles[:20])

            if failed_profiles:
                summary += "\n\nОшибки:\n" + "\n".join(failed_profiles[:20])

            tg_send_long_message(chat_id, summary)

            if is_admin(user_id):
                send_admin_menu(chat_id, "Меню Admin:")
            else:
                send_main_menu(chat_id, "Главное меню:", user_id=user_id)
            return

        send_main_menu(chat_id, "Не понял команду. Выбери кнопку из меню:", user_id=user_id)

    except Exception as e:
        logging.exception("handle_message crashed")
        notify_admin_about_error(
            "handle_message",
            str(e),
            extra_text=f"user_id={user_id}, chat_id={chat_id}, text={text}"
        )
        try:
            error_text = str(e)

            if "Google Sheets временно перегружен" in error_text:
                tg_send_message(chat_id, error_text)
            else:
                tg_send_message(chat_id, "Произошла ошибка. Попробуй ещё раз.")

            send_main_menu(chat_id, "Главное меню:", user_id=user_id)
        except Exception:
            pass

def handle_callback_query(callback_query):
    try:
        touch_request_heartbeat()
        callback_id = callback_query["id"]
        data = callback_query.get("data", "")
        chat_id = callback_query["message"]["chat"]["id"]
        message_id = callback_query["message"]["message_id"]
        user_id = callback_query["from"]["id"]

        if not has_access(user_id):
            tg_answer_callback_query(callback_id, "Нет доступа")
            return

        if data.startswith("fullstats_accounts:"):
            username = data.split(":", 1)[1]
            tg_answer_callback_query(callback_id)
            full_text = build_manager_stats_text(username)

            safe_replace_stats_message(
                chat_id=chat_id,
                message_id=message_id,
                full_text=full_text,
                back_callback_data=f"backstats_accounts:{username}"
            )
            return

        if data.startswith("fullstats_farmers:"):
            username = data.split(":", 1)[1]
            tg_answer_callback_query(callback_id)
            full_text = build_farmer_stats_text(username)

            safe_replace_stats_message(
                chat_id=chat_id,
                message_id=message_id,
                full_text=full_text,
                back_callback_data=f"backstats_farmers:{username}"
            )
            return

        if data.startswith("backstats_accounts:"):
            username = data.split(":", 1)[1]
            summary_text = build_manager_stats_summary_text(username)

            tg_answer_callback_query(callback_id)
            tg_edit_message_text(
                chat_id,
                message_id,
                summary_text,
                inline_buttons=[[{
                    "text": "Полная статистика",
                    "callback_data": f"fullstats_accounts:{username}"
                }]]
            )
            return

        if data.startswith("backstats_farmers:"):
            username = data.split(":", 1)[1]
            summary_text = build_farmer_stats_summary_text(username)

            tg_answer_callback_query(callback_id)
            tg_edit_message_text(
                chat_id,
                message_id,
                summary_text,
                inline_buttons=[[{
                    "text": "Полная статистика",
                    "callback_data": f"fullstats_farmers:{username}"
                }]]
            )
            return

        if data.startswith("download_farm_king_txt:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            state = get_state(user_id)

            king_name = str(state.get("last_farm_king_name", "")).strip()
            data_text = str(state.get("last_farm_king_data_text", "")).strip()

            if not king_name or not data_text:
                tg_answer_callback_query(callback_id, "Нет данных для txt файла")
                return

            tg_answer_callback_query(callback_id, "Отправляю txt...")
            tg_send_king_data_as_txt(
                chat_id=chat_id,
                king_name=king_name,
                data_text=data_text
            )
            return jsonify({"ok": True})

        if data == f"download_farm_king_bulk_zip:{user_id}":
            state = get_state(user_id)
            results = state.get("farm_kings_bulk_results", [])

            success_items = [x for x in results if x.get("octo_ok")]
            if not success_items:
                tg_answer_callback_query(callback_id, "Нет файлов для скачивания")
                return jsonify({"ok": True})

            try:
                archive_name = f"farm_kings_{datetime.now(MOSCOW_TZ).strftime('%Y%m%d_%H%M%S')}.zip"
                tg_send_kings_as_zip(
                    chat_id=chat_id,
                    issued_items=success_items,
                    archive_name=archive_name
                )
                tg_answer_callback_query(callback_id, "Zip отправлен")
            except Exception:
                logging.exception("download_farm_king_bulk_zip failed")
                tg_answer_callback_query(callback_id, "Не удалось отправить zip")

            return jsonify({"ok": True})

        if data.startswith(f"download_farm_king_bulk_txt:{user_id}:"):
            state = get_state(user_id)
            results = state.get("farm_kings_bulk_results", [])

            success_items = [x for x in results if x.get("octo_ok")]
            if not success_items:
                tg_answer_callback_query(callback_id, "Нет txt для скачивания")
                return jsonify({"ok": True})

            try:
                item = success_items[0]
                tg_send_king_data_as_txt(
                    chat_id=chat_id,
                    king_name=item.get("king_name", "farm_king"),
                    data_text=item.get("data_text", "")
                )
                tg_answer_callback_query(callback_id, "Txt отправлен")
            except Exception:
                logging.exception("download_farm_king_bulk_txt failed")
                tg_answer_callback_query(callback_id, "Не удалось отправить txt")

            return jsonify({"ok": True})

        if data == f"farm_kings_bulk_skip_all_proxies:{user_id}":
            state = get_state(user_id)
            state["farm_kings_bulk_skip_all_proxies"] = True

            if message_id:
                state["farm_kings_bulk_proxy_message_id"] = message_id

            set_state_with_custom_ttl(user_id, state, FARM_KING_BULK_PROXY_TTL)

            if message_id:
                tg_edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text="✅ Прокси пропущены",
                    inline_buttons=[]
                )

            tg_answer_callback_query(callback_id, "Все оставшиеся прокси будут пропущены")

            process_farm_kings_bulk_proxy_step(
                chat_id=chat_id,
                user_id=user_id,
                username=state.get("farm_kings_bulk_username", ""),
                proxy_text="__SKIP_ALL_PROXIES__"
            )

            return jsonify({"ok": True})

        if data.startswith("confirm_farm_kings_bulk_octo:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            tg_answer_callback_query(callback_id)

            state = get_state(user_id)

            if message_id:
                state["farm_kings_bulk_confirm_message_id"] = message_id
                set_state_with_custom_ttl(user_id, state, FARM_KING_BULK_PROXY_TTL)

                tg_edit_message_text(
                    chat_id,
                    message_id,
                    "✅ Список farm king подтверждён",
                    inline_buttons=[]
                )

            start_farm_kings_bulk_proxy_step(chat_id, user_id)
            return jsonify({"ok": True})

        if data.startswith("cancel_farm_kings_bulk_octo:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            tg_answer_callback_query(callback_id, "Выдача отменена")
            clear_state(user_id)

            if message_id:
                tg_edit_message_text(
                    chat_id,
                    message_id,
                    "❌ Выдача farm king отменена",
                    inline_buttons=[]
                )

            send_farm_kings_menu(chat_id, "Меню Farm King:")
            return jsonify({"ok": True})

        if data.startswith("farm_king_skip_proxy:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            tg_answer_callback_query(callback_id, "Прокси пропущен")

            if message_id:
                tg_edit_message_text(
                    chat_id,
                    message_id,
                    "✅ Прокси пропущен",
                    inline_buttons=[]
                )

            handle_message({
                "chat": {"id": chat_id},
                "from": {"id": user_id, "username": callback_query["from"].get("username", "")},
                "text": "__SKIP_PROXY_FARM_SINGLE__"
            })
            return jsonify({"ok": True})

        if data.startswith("confirm_farm_king_octo:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            tg_answer_callback_query(callback_id)

            state = get_state(user_id)
            state["farm_king_preview_message_id"] = message_id
            set_state(user_id, state)

            confirm_farm_king_octo_issue(
                chat_id,
                user_id,
                callback_query["from"].get("username", "")
            )
            return jsonify({"ok": True})

        if data.startswith("other_farm_king_octo:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            state = get_state(user_id)

            current_row = state.get("farm_king_row")
            geo_value = str(state.get("farm_king_geo", "")).strip()

            if not current_row or not geo_value:
                tg_answer_callback_query(callback_id, "Сначала начни выдачу заново")
                send_farm_kings_menu(chat_id, "Меню Farm King:")
                return

            found_list = find_free_farm_kings(50, geo=geo_value)
            found = None

            for item in found_list:
                if item["row_index"] != current_row:
                    row = ensure_row_len(item["row"], 13)

                    found = {
                        "row_index": item["row_index"],
                        "purchase_date": row[1],
                        "price": row[2],
                        "supplier": row[3],
                        "geo": row[7],
                        "data_text": get_full_king_data_from_row(row),
                        "row": row
                    }
                    break

            if not found:
                tg_answer_callback_query(callback_id, "Другого farm king нет")
                return

            tg_answer_callback_query(callback_id, "Нашёл другой farm king")

            if message_id:
                edit_found_farm_king_octo_preview(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    found=found
                )
            else:
                show_found_farm_king_octo(chat_id, user_id, found)

            return jsonify({"ok": True})

        if data == f"download_crypto_bulk_zip:{user_id}":
            query_id = callback_query["id"]
        
            state = get_state(user_id)
            results = state.get("crypto_bulk_results", [])
        
            success_items = [x for x in results if x.get("octo_ok")]
            if not success_items:
                tg_answer_callback_query(query_id, "Нет файлов для скачивания")
                return jsonify({"ok": True})
        
            try:
                archive_name = f"crypto_kings_{datetime.now(MOSCOW_TZ).strftime('%Y%m%d_%H%M%S')}.zip"
                tg_send_kings_as_zip(
                    chat_id=chat_id,
                    issued_items=success_items,
                    archive_name=archive_name
                )
                tg_answer_callback_query(query_id, "Zip отправлен")
            except Exception:
                logging.exception("download_crypto_bulk_zip failed")
                tg_answer_callback_query(query_id, "Не удалось отправить zip")
        
            return jsonify({"ok": True})
        
        
        if data.startswith(f"download_crypto_bulk_txt:{user_id}:"):
            query_id = callback_query["id"]
        
            state = get_state(user_id)
            results = state.get("crypto_bulk_results", [])
        
            success_items = [x for x in results if x.get("octo_ok")]
            if not success_items:
                tg_answer_callback_query(query_id, "Нет txt для скачивания")
                return jsonify({"ok": True})
        
            try:
                item = success_items[0]
                tg_send_king_data_as_txt(
                    chat_id=chat_id,
                    king_name=item.get("king_name", "king"),
                    data_text=item.get("data_text", "")
                )
                tg_answer_callback_query(query_id, "Txt отправлен")
            except Exception:
                logging.exception("download_crypto_bulk_txt failed")
                tg_answer_callback_query(query_id, "Не удалось отправить txt")
        
            return jsonify({"ok": True})

        if data == f"crypto_bulk_skip_all_proxies:{user_id}":
            query_id = callback_query["id"]
            message = callback_query.get("message", {})
            message_id = message.get("message_id")
        
            state = get_state(user_id)
            state["crypto_bulk_skip_all_proxies"] = True
        
            if message_id:
                state["crypto_bulk_proxy_message_id"] = message_id
        
            set_state_with_custom_ttl(user_id, state, CRYPTO_BULK_PROXY_TTL)
        
            if message_id:
                tg_edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text="✅ Прокси пропущены",
                    inline_buttons=[]
                )
        
            tg_answer_callback_query(query_id, "Все оставшиеся прокси будут пропущены")
        
            process_crypto_bulk_proxy_step(
                chat_id=chat_id,
                user_id=user_id,
                username=state.get("crypto_bulk_username", ""),
                proxy_text="__SKIP_ALL_PROXIES__"
            )
        
            return jsonify({"ok": True})

        if data.startswith("download_crypto_txt:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            state = get_state(user_id)

            king_name = str(state.get("last_crypto_king_name", "")).strip()
            data_text = str(state.get("last_crypto_king_data_text", "")).strip()

            if not king_name or not data_text:
                tg_answer_callback_query(callback_id, "Нет данных для txt файла")
                return

            tg_answer_callback_query(callback_id, "Отправляю txt...")
            tg_send_king_data_as_txt(
                chat_id=chat_id,
                king_name=king_name,
                data_text=data_text
            )
            return

        if data.startswith("confirm_crypto_bulk_item:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            tg_answer_callback_query(callback_id)

            state = get_state(user_id)

            if message_id:
                state["crypto_bulk_confirm_message_id"] = message_id
                set_state_with_custom_ttl(user_id, state, CRYPTO_BULK_PROXY_TTL)

                tg_edit_message_text(
                    chat_id,
                    message_id,
                    "✅ Список crypto king подтверждён",
                    inline_buttons=[]
                )

            start_crypto_kings_bulk_proxy_step(chat_id, user_id)
            return jsonify({"ok": True})


        if data.startswith("cancel_crypto_bulk:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            tg_answer_callback_query(callback_id, "Выдача отменена")
            clear_state(user_id)

            if message_id:
                tg_edit_message_text(
                    chat_id,
                    message_id,
                    "❌ Выдача crypto king отменена",
                    inline_buttons=[]
                )

            send_kings_menu(chat_id, "Меню кингов:")
            return jsonify({"ok": True})

        if data.startswith("confirm_crypto_king:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            tg_answer_callback_query(callback_id)

            state = get_state(user_id)
            state["crypto_preview_message_id"] = message_id
            set_state(user_id, state)

            confirm_crypto_king_issue(
                chat_id,
                user_id,
                callback_query["from"].get("username", "")
            )
            return
            
        if data.startswith("other_crypto_king:"):
            target_user_id = data.split(":", 1)[1]

            if str(user_id) != str(target_user_id):
                tg_answer_callback_query(callback_id, "Это не ваша кнопка")
                return

            state = get_state(user_id)

            current_row = state.get("king_row")
            geo_value = str(state.get("king_geo", "")).strip()
            price_value = str(state.get("king_price", "")).strip()

            if not current_row or not geo_value or not price_value:
                tg_answer_callback_query(callback_id, "Сначала начни выдачу заново")
                send_kings_menu(chat_id, "Меню кингов:")
                return

            found = find_free_crypto_king_by_geo_and_price(
                geo=geo_value,
                price=price_value,
                exclude_row=current_row
            )

            if not found:
                tg_answer_callback_query(callback_id, "Другого crypto king нет")
                return

            tg_answer_callback_query(callback_id, "Нашёл другого crypto king")

            if message_id:
                edit_found_crypto_king_preview(
                    chat_id=chat_id,
                    message_id=message_id,
                    user_id=user_id,
                    found=found
                )
            else:
                show_found_crypto_king(chat_id, user_id, found)

        return jsonify({"ok": True})

    except Exception as e:
        logging.exception("handle_callback_query crashed")
        try:
            tg_answer_callback_query(callback_query.get("id", ""), "Ошибка")
        except Exception:
            pass

        try:
            notify_admin_about_error(
                "handle_callback_query",
                str(e),
                extra_text=(
                    f"user_id={callback_query.get('from', {}).get('id')}, "
                    f"chat_id={callback_query.get('message', {}).get('chat', {}).get('id')}, "
                    f"data={callback_query.get('data', '')}"
                )
            )
        except Exception:
            pass

        return jsonify({"ok": True})

def process_incoming_message(msg):
    if msg.get("text"):
        handle_message(msg)
    elif msg.get("document"):
        handle_document_message(msg)

# =========================
# FLASK
# =========================
@app.route("/", methods=["GET"])
def index():
    return "ok", 200

@app.route("/health", methods=["GET"])
def health():
    now = time.time()

    request_stale = now - last_request_time > WATCHDOG_TIMEOUT
    background_stale = now - last_background_time > WATCHDOG_TIMEOUT

    if background_stale:
        return jsonify({
            "ok": False,
            "error": "background threads stale"
        }), 503

    return jsonify({
        "ok": True,
        "last_request_age": int(now - last_request_time),
        "last_background_age": int(now - last_background_time),
    }), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(silent=True) or {}
        update_id = update.get("update_id")

        cleanup_processed_updates()

        if is_duplicate_update(update_id):
            logging.info(f"SKIP DUPLICATE update_id={update_id}")
            return jsonify({"ok": True})

        callback_query = update.get("callback_query")
        if callback_query:
            handle_callback_query(callback_query)
            return jsonify({"ok": True})

        msg = update.get("message") or update.get("edited_message")

        logging.info(
            f"WEBHOOK update_id={update_id} has_message={bool(msg)} has_callback={bool(callback_query)}"
        )

        if msg:
            process_incoming_message(msg)

        return jsonify({"ok": True})

    except Exception as e:
        logging.exception(f"webhook error: {e}")
        notify_admin_about_error("webhook", str(e))
        return jsonify({"ok": True})

@app.route("/fastadscheck-import", methods=["POST", "OPTIONS"])
def fastadscheck_import():
    allowed_origin = "https://app.ffb.vn"

    def _corsify(resp):
        resp.headers["Access-Control-Allow-Origin"] = allowed_origin
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp

    try:
        if request.method == "OPTIONS":
            return _corsify(make_response("", 204))

        payload = request.get_json(silent=True) or {}

        token = str(payload.get("token", "")).strip()
        rows = payload.get("rows", [])

        expected_token = os.environ.get("FASTADCHECK_IMPORT_TOKEN", "").strip()

        if not expected_token:
            return _corsify(jsonify({
                "ok": False,
                "error": "FASTADCHECK_IMPORT_TOKEN не задан на сервере"
            })), 500

        if token != expected_token:
            return _corsify(jsonify({
                "ok": False,
                "error": "unauthorized"
            })), 403

        if not isinstance(rows, list) or not rows:
            return _corsify(jsonify({
                "ok": False,
                "error": "rows пустой или неверный формат"
            })), 400

        with accounts_lock:
            existing_rows = get_sheet_rows_cached(SHEET_ACCOUNTS, force=True)

            indexed = {}
            for idx, row in enumerate(existing_rows[1:], start=2):
                row = ensure_row_len(row, 14)
                account_number = str(row[0]).strip()
                if account_number:
                    indexed[account_number] = {
                        "row_index": idx,
                        "row": row
                    }

            updated = 0
            not_found = []
            skipped = []
            batch_updates = []

            for item in rows:
                account_number = str(item.get("account_id", "")).strip()
                limit_value = item.get("limit_usd")
                threshold_value = item.get("threshold_usd")
                gmt_value = str(item.get("gmt", "")).strip()
                currency_value = str(item.get("currency", "")).strip().upper()
                account_url = str(item.get("account_url", "")).strip()

                if not account_number or limit_value is None or threshold_value is None or gmt_value == "":
                    skipped.append(account_number or "unknown")
                    continue

                found = indexed.get(account_number)
                if not found:
                    not_found.append(account_number)
                    continue

                row_index = found["row_index"]
                row = found["row"]

                parsed_limit = parse_limit_number(limit_value)
                if parsed_limit == "unlim":
                    limit_to_store = "unlim"
                else:
                    limit_to_store = normalize_numeric_for_sheet(limit_value)

                values_en = [[
                    limit_to_store,                                # E
                    normalize_numeric_for_sheet(threshold_value),  # F
                    str(gmt_value),                                # G
                    row[7] if len(row) > 7 else "",                # H
                    row[8] if len(row) > 8 else "",                # I
                    row[9] if len(row) > 9 else "",                # J
                    row[10] if len(row) > 10 else "",              # K
                    row[11] if len(row) > 11 else "",              # L
                    currency_value or "",                          # M
                    account_url or ""                              # N
                ]]

                batch_updates.append({
                    "range": f"E{row_index}:N{row_index}",
                    "values": values_en
                })

                updated += 1

            if batch_updates:
                sheet_batch_update_raw(SHEET_ACCOUNTS, batch_updates)

            refresh_sheet_cache(SHEET_ACCOUNTS)
            invalidate_stats_cache()

        return _corsify(jsonify({
            "ok": True,
            "updated": updated,
            "not_found": not_found[:100],
            "skipped": skipped[:100],
            "received": len(rows)
        })), 200

    except Exception as e:
        logging.exception("fastadscheck_import crashed")
        notify_admin_about_error("fastadscheck_import", str(e))
        return _corsify(jsonify({
            "ok": False,
            "error": str(e)
        })), 500

@app.route("/fastadscheck-add", methods=["POST"])
def fastadscheck_add():
    try:
        payload = request.get_json(silent=True) or {}

        token = str(payload.get("token", "")).strip()
        rows = payload.get("rows", [])

        expected_token = os.environ.get("FASTADCHECK_IMPORT_TOKEN", "").strip()

        if not expected_token:
            return jsonify({
                "ok": False,
                "error": "FASTADCHECK_IMPORT_TOKEN не задан на сервере"
            }), 500

        if token != expected_token:
            return jsonify({
                "ok": False,
                "error": "unauthorized"
            }), 403

        if not isinstance(rows, list) or not rows:
            return jsonify({
                "ok": False,
                "error": "rows пустой или неверный формат"
            }), 400

        with accounts_lock:
            existing_rows = get_sheet_rows_cached(SHEET_ACCOUNTS)
            existing_accounts = set()

            for row in existing_rows[1:]:
                if row and len(row) > 0 and str(row[0]).strip():
                    existing_accounts.add(str(row[0]).strip())

            to_append = []
            duplicates = 0
            skipped = []

            for item in rows:
                account_id = str(item.get("account_id", "")).strip()
                gmt_value = str(item.get("gmt", "")).strip()
                currency_value = str(item.get("currency", "")).strip().upper()
                account_url = str(item.get("account_url", "")).strip()
                limit_value = item.get("limit_usd")
                threshold_value = item.get("threshold_usd")

                if not account_id:
                    skipped.append("empty_account_id")
                    continue

                if account_id in existing_accounts:
                    duplicates += 1
                    continue

                parsed_limit = parse_limit_number(limit_value)
                if parsed_limit == "unlim":
                    limit_to_store = "unlim"
                else:
                    limit_to_store = normalize_numeric_for_sheet(limit_value) if limit_value is not None else ""

                to_append.append([
                    account_id,
                    "",
                    "",
                    "",
                    limit_to_store,
                    normalize_numeric_for_sheet(threshold_value) if threshold_value is not None else "",
                    gmt_value,
                    "",
                    "free",
                    "",
                    "",
                    "",
                    currency_value,
                    account_url
                ])

                existing_accounts.add(account_id)

            if to_append:
                sheet_append_rows_and_refresh(
                    SHEET_ACCOUNTS,
                    to_append,
                    value_input_option="USER_ENTERED"
                )
                invalidate_stats_cache()

        return jsonify({
            "ok": True,
            "added": len(to_append),
            "duplicates": duplicates,
            "skipped": skipped[:100],
            "received": len(rows)
        }), 200

    except Exception as e:
        logging.exception("fastadscheck_add crashed")
        notify_admin_about_error("fastadscheck_add", str(e))
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500

@app.route("/basebot-delete-sync", methods=["POST"])
def basebot_delete_sync():
    try:
        payload = request.get_json(silent=True) or {}

        token = str(payload.get("token", "")).strip()
        sheet_type = str(payload.get("sheet_type", "")).strip()
        sync_id = str(payload.get("sync_id", "")).strip()

        expected_token = os.environ.get("BASEBOT_SYNC_TOKEN", "").strip()

        if token != expected_token:
            return jsonify({"ok": False, "error": "unauthorized"}), 403

        if not sheet_type or not sync_id:
            return jsonify({"ok": False, "error": "sheet_type or sync_id missing"}), 400

        mapping = {
            "kings": (SHEET_KINGS, 12),
            "crypto_kings": (SHEET_CRYPTO_KINGS, 12),
            "farm_kings": (SHEET_FARM_KINGS, 12),
            "bms": (SHEET_BMS, 9),
            "pixels": (SHEET_PIXELS, 8),
        }

        if sheet_type not in mapping:
            return jsonify({"ok": False, "error": "unknown sheet_type"}), 400

        resource_sheet, sync_col_index = mapping[sheet_type]

        found = find_row_in_sheet_by_sync_id(
            sheet_name=resource_sheet,
            sync_id=sync_id,
            sync_col_index=sync_col_index,
            basebot=False
        )

        if not found:
            return jsonify({"ok": True, "deleted": False, "reason": "not_found_in_resourcehub"}), 200

        sheet_delete_row_and_refresh(resource_sheet, found["row_index"])
        invalidate_stats_cache()

        return jsonify({"ok": True, "deleted": True}), 200

    except Exception as e:
        logging.exception("basebot_delete_sync crashed")
        return jsonify({"ok": False, "error": str(e)}), 500

def run_auto_healthcheck_once():
    try:
        rows_accounts = get_sheet_rows_cached(SHEET_ACCOUNTS, force=True)
        rows_issues = get_sheet_rows_cached(SHEET_ISSUES, force=True)
        rows_kings = get_sheet_rows_cached(SHEET_KINGS, force=True)
        rows_crypto = get_sheet_rows_cached(SHEET_CRYPTO_KINGS, force=True)
        rows_bms = get_sheet_rows_cached(SHEET_BMS, force=True)
        rows_fps = get_sheet_rows_cached(SHEET_FPS, force=True)
        rows_pixels = get_sheet_rows_cached(SHEET_PIXELS, force=True)
        rows_farm_kings = get_sheet_rows_cached(SHEET_FARM_KINGS, force=True)
        rows_farm_bms = get_sheet_rows_cached(SHEET_FARM_BMS, force=True)
        rows_farm_fps = get_sheet_rows_cached(SHEET_FARM_FPS, force=True)

        all_checks = [
            ("База_личек", rows_accounts, 14),
            ("Простые лички 26", rows_issues, 9),
            ("База_кингов", rows_kings, 12),
            ("База_крипта_кинги", rows_crypto, 12),
            ("База_БМ", rows_bms, 9),
            ("База_ФП", rows_fps, 9),
            ("База_пикселей", rows_pixels, 8),
            ("База фарм кинги", rows_farm_kings, 12),
            ("База фарм бм", rows_farm_bms, 9),
            ("База фарм фп", rows_farm_fps, 9),
        ]

        for title, rows, min_cols in all_checks:
            if not rows:
                raise RuntimeError(f"Лист '{title}' пустой или не читается")

            header = rows[0]
            if len(header) < min_cols:
                raise RuntimeError(
                    f"Лист '{title}' сломан: колонок меньше нормы ({len(header)} < {min_cols})"
                )

        # Доп. проверка статистики
        build_stats_text()
        build_manager_stats_summary_text("test_user")
        build_farmer_stats_summary_text("test_user")
        duplicates_result = run_duplicates_checks()
        if duplicates_result["fail_count"] > 0:
            raise RuntimeError(
                "Найдены дубли в таблицах: " + " | ".join(duplicates_result["report"][:10])
            )

        return True

    except Exception as e:
        notify_admin_about_error("auto_healthcheck", str(e))
        logging.exception("run_auto_healthcheck_once crashed")
        return False


def auto_healthcheck_loop():
    while True:
        try:
            touch_background_heartbeat()
            cleanup_error_notifications()
            run_auto_healthcheck_once()
            time.sleep(1800)  # каждые 30 минут
        except Exception as e:
            notify_admin_about_error("auto_healthcheck_loop", str(e))
            logging.exception("auto_healthcheck_loop crashed")
            time.sleep(60)
        
if __name__ == "__main__":
    # cache_thread = threading.Thread(target=cache_warmer_loop, daemon=True)
    # cache_thread.start()

    backup_thread = threading.Thread(target=backup_scheduler_loop, daemon=True)
    backup_thread.start()

    watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
    watchdog_thread.start()

    auto_health_thread = threading.Thread(target=auto_healthcheck_loop, daemon=True)
    auto_health_thread.start()

    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
