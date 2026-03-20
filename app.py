from flask_cors import CORS
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
CORS(app)
logging.basicConfig(level=logging.INFO)

# =========================
# ENV
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
BACKUP_SPREADSHEET_ID = os.environ.get("BACKUP_SPREADSHEET_ID", "")
EXCHANGE_API_BASE = os.environ.get("EXCHANGE_API_BASE", "https://api.exchangerate.host")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан")

if not SPREADSHEET_ID:
    raise RuntimeError("SPREADSHEET_ID не задан")

if not SERVICE_ACCOUNT_JSON:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON не задан")

if not BACKUP_SPREADSHEET_ID:
    raise RuntimeError("BACKUP_SPREADSHEET_ID не задан")

# =========================
# ACCESS CONTROL
# =========================

ADMINS = {
    7573650707: "JackGrazer_Deputy_Head_Account",
    7681133609: "Cillian_Murphy_Head_of_Account",
    7172090459: "JackieChan_FarmLead",
    7389698288: "andrewgarfield_farmlead",
}

FP_WAREHOUSE_NOTIFY_ADMIN_ID = 7573650707

ACCOUNTS_USERS = {
    7953116439: "WillemDafoe_Accmanager",
    8334712952: "Ariana_Grande_Account_manager",
    7851493919: "CateBlanchettAccountManager",
    7426931469: "JimCarrey_AccountManager",
}

FARMERS_USERS = {
    8482380951: "josephgordonlevitt_farmer",
    8554652263: "leesungkyoung_farmer",
    8389730381: "JaimeMurray_farmer",
    8589105033: "owenwilson_farmer",
    8503147017: "zendaya_farmer",
}

def is_admin(user_id):
    return user_id in ADMINS

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

FARM_SUBMENU_GET_KINGS = 'Взять кинги'
FARM_SUBMENU_FREE_KINGS = 'Свободные кинги'
FARM_SUBMENU_RETURN_KING = 'Вернуть кинг'
FARM_SUBMENU_SEARCH_KING = 'Поиск кинга'

BTN_FARM_KINGS_PARTIAL_CONFIRM = 'Выдать'
BTN_FARM_KINGS_PARTIAL_CANCEL = 'Отмена'

FARM_SUBMENU_GET_BM = 'Получить BM'
FARM_SUBMENU_FREE_BMS = 'Свободные BMы'
FARM_SUBMENU_SEARCH_BM = 'Поиск BMа'

FARM_SUBMENU_GET_FP = 'Выдать FP'
FARM_SUBMENU_SEARCH_FP = 'Поиск FP'

BTN_BACK_TO_FARMERS = 'Назад в Farmers'
MENU_KINGS = 'Кинги'
MENU_BMS = 'БМ'
MENU_FPS = 'ФП'
MENU_STATS = 'Статистика'
MENU_MANAGER_STATS = 'Статистика менеджера'
MENU_FARMER_STATS = 'Статистика фармера'
MENU_ADMIN = 'Admin'
MENU_CANCEL = 'Отмена'

SUBMENU_GET_PIXELS = 'Получить Пиксели'
SUBMENU_SEARCH_PIXEL = 'Найти Пиксель'
SUBMENU_RETURN_PIXEL = 'Вернуть Пиксель'
SUBMENU_ACCOUNTS_MAIN = 'Лички'
SUBMENU_BACK_MAIN = 'В меню'

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

FREE_LIMIT_250 = '250'
FREE_LIMIT_500 = '500'
FREE_LIMIT_1200 = '1200'
FREE_LIMIT_1500 = '1500'
FREE_LIMIT_UNLIM = 'unlim'

SUBMENU_FREE_KINGS = 'Свободные кинги'
SUBMENU_GET_KINGS = 'Получить кинг'
SUBMENU_RETURN_KING = 'Вернуть кинг'
SUBMENU_SEARCH_KING = 'Поиск кинга'

SUBMENU_GET_BM = 'Получить БМ'
SUBMENU_FREE_BMS = 'Свободные БМы'
SUBMENU_SEARCH_BM = 'Поиск БМа'

SUBMENU_GET_FP = 'Выдать ФП'
SUBMENU_SEARCH_FP = 'Поиск ФП'

ADMIN_ACCOUNTANTS = 'Акаунтеры'
ADMIN_FARMERS = 'Фармеры'
ADMIN_BOT_CHECK = 'Проверка бота'
ADMIN_BACKUP = 'Бэкап таблиц'
ADMIN_UPDATE_5M = 'Обновление 5м'
ADMIN_ALL_STATS = 'Статистика всех'
SUBMENU_CRYPTO_KINGS = 'Крипта кинги'
ADMIN_ADD_CRYPTO_KINGS = 'Добавить crypto king'

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
BTN_PIXEL_BAN_CONFIRM = 'Подтвердить ban'
BTN_BACK_FROM_ADMIN_FARMERS = 'Назад в Admin'
BTN_BM_CONFIRM = 'Выдать БМ'
BTN_BM_NEXT = 'Другой БМ'
BTN_BACK_FROM_ADMIN = 'Назад из Admin'
BTN_BACK_FROM_ACCOUNTANTS = 'Назад из Акаунтеры'

BTN_BACK_TO_MENU = 'В меню'

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
BTN_FP_CONFIRM = 'Выдать ФП'
BTN_FP_NEXT = 'Другое ФП'

# Память состояний пользователей (для старта хватит)
user_states = {}
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
TABLE_CACHE_TTL = 3  # сек; можно 3-5

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
    with google_lock:
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
    with google_lock:
        sheet = get_sheet(sheet_name)
        sheet.update(cell_range, values)

    refresh_sheet_cache(sheet_name)

def sheet_update_raw(sheet_name, cell_range, values):
    with google_lock:
        sheet = get_sheet(sheet_name)
        sheet.update(cell_range, values)

def sheet_append_row_and_refresh(sheet_name, row, value_input_option="USER_ENTERED"):
    with google_lock:
        sheet = get_sheet(sheet_name)
        sheet.append_row(row, value_input_option=value_input_option)

    refresh_sheet_cache(sheet_name)


def sheet_append_rows_and_refresh(sheet_name, rows, value_input_option="USER_ENTERED"):
    with google_lock:
        sheet = get_sheet(sheet_name)
        sheet.append_rows(rows, value_input_option=value_input_option)

    refresh_sheet_cache(sheet_name)

def get_next_empty_row_in_issues():
    rows = get_sheet_rows_cached(SHEET_ISSUES)
    return len(rows) + 1


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

        with google_lock:
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
            timeout=20
        )

        if resp.status_code != 200:
            logging.warning(f"Telegram send failed: {resp.text}")

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

    except Exception as e:
        logging.error(f"tg_send_inline_message error: {e}")

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
        [{"text": BTN_BACK_TO_FARMERS}]
    ]
    tg_send_message(chat_id, text, keyboard)

def send_farm_fps_menu(chat_id, text="Меню Farm FP:"):
    keyboard = [
        [{"text": FARM_SUBMENU_GET_FP}],
        [{"text": FARM_SUBMENU_SEARCH_FP}],
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

def get_free_crypto_king_geos():
    rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

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


def send_crypto_king_geo_options(chat_id):
    geos = get_free_crypto_king_geos()

    if not geos:
        send_kings_menu(chat_id, "Нет свободных crypto king ни по одному GEO.")
        return

    keyboard = []
    for geo in geos:
        keyboard.append([{"text": geo}])

    keyboard.append([{"text": MENU_CANCEL}])

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


def show_found_crypto_king(chat_id, user_id, found):
    state = get_state(user_id)

    state["mode"] = "crypto_king_found"
    state["king_row"] = found["row_index"]
    set_state(user_id, state)

    text = (
        "Найден crypto king:\n\n"
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

def add_kings_from_txt_content(file_text, target_sheet=SHEET_KINGS):
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

    sheet_append_rows_and_refresh(target_sheet, to_append)
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
    parsed_rows, errors = parse_bms_txt(file_text)

    if not parsed_rows:
        if errors:
            return "Ничего не добавил.\n\nОшибки:\n" + "\n".join(errors[:10])
        return "Ничего не добавил. Не удалось разобрать текст."

    existing_rows = get_sheet_rows_cached(target_sheet)
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
        sheet_append_rows_and_refresh(target_sheet, to_append)
        invalidate_stats_cache()

    message = (
        f"Готово ✅\n"
        f"Добавлено BM: {len(to_append)}\n"
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
            result_message = add_kings_from_txt_content(file_text, target_sheet=SHEET_KINGS)
        elif mode == "awaiting_bms_text":
            result_message = add_bms_from_txt_content(file_text, target_sheet=SHEET_BMS)
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

def add_fps_from_text(text, target_sheet=SHEET_FPS):
    existing_rows = get_sheet_rows_cached(target_sheet)
    existing_links = set()

    for row in existing_rows[1:]:
        if row and row[0].strip():
            existing_links.add(row[0].strip())

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    to_append = []
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

        to_append.append([
            fp_link,
            purchase_date.strftime("%d/%m/%Y"),
            price,
            supplier,
            warehouse,
            "free",
            "",
            "",
            ""
        ])

        existing_links.add(fp_link)

    if to_append:
        sheet_append_rows_and_refresh(target_sheet, to_append)
        invalidate_stats_cache()

    message = (
        f"Готово ✅\n"
        f"Добавлено FP: {len(to_append)}\n"
        f"Дубликатов пропущено: {duplicates}\n"
        f"Ошибок: {len(errors)}"
    )

    if errors:
        message += "\n\nОшибки:\n" + "\n".join(errors[:10])
        if len(errors) > 10:
            message += f"\n... и ещё {len(errors) - 10}"

    return message

def add_pixels_from_text(text, target_sheet=SHEET_PIXELS):
    lines = text.splitlines()

    blocks = []
    current_header = None
    current_data_lines = []

    for raw_line in lines:
        line = raw_line.rstrip()

        # пустая строка = конец блока
        if not line.strip():
            if current_header is not None:
                blocks.append((current_header, current_data_lines))
                current_header = None
                current_data_lines = []
            continue

        # если header еще не начат — это первая строка блока
        if current_header is None:
            current_header = line.strip()
            current_data_lines = []
        else:
            current_data_lines.append(line)

    # последний блок
    if current_header is not None:
        blocks.append((current_header, current_data_lines))

    to_append = []
    errors = []

    for i, (header, data_lines) in enumerate(blocks, start=1):
        fields = [x.strip() for x in header.split(";")]

        if len(fields) != 3:
            errors.append(f"Блок {i}: должно быть 3 поля в первой строке: дата покупки; цена; поставщик")
            continue

        purchase_date_raw, price_raw, supplier = fields

        purchase_date = parse_date(purchase_date_raw)
        if not purchase_date:
            errors.append(f"Блок {i}: неверная дата покупки '{purchase_date_raw}'")
            continue

        price = parse_price(price_raw)
        if price is None:
            errors.append(f"Блок {i}: неверная цена '{price_raw}'")
            continue

        if not supplier:
            errors.append(f"Блок {i}: не указан поставщик")
            continue

        data_text = "\n".join([x.rstrip() for x in data_lines]).strip()
        if not data_text:
            errors.append(f"Блок {i}: не указаны данные Пикселя")
            continue

        to_append.append([
            purchase_date.strftime("%d/%m/%Y"),  # A дата покупки
            price,                               # B цена
            supplier,                            # C поставщик
            "free",                              # D статус
            "",                                  # E кому выдали
            "",                                  # F дата взятия
            "",                                  # G кто взял
            data_text                            # H данные
        ])

    if to_append:
        sheet_append_rows_and_refresh(target_sheet, to_append)
        invalidate_stats_cache()

    message = (
        f"Готово ✅\n"
        f"Добавлено Пикселей: {len(to_append)}\n"
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
        f"У кого купили: {row[3] or 'не указан'}\n"
        f"Склад: {row[4] or 'не указан'}\n"
        f"Статус: {row[5] or 'не указан'}\n"
        f"Для кого: {row[6] or 'не указано'}\n"
        f"Кто взял: {row[7] or 'не указано'}\n"
        f"Дата выдачи: {row[8] or 'не указана'}"
    )

    return text

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
        text = (
            f"Склад {warehouse_name} закончился.\n"
            f"Нужно открыть доступ к складу {next_warehouse}."
        )
    else:
        text = (
            f"Склад {warehouse_name} закончился.\n"
            f"Следующего склада для выдачи не найдено."
        )

    tg_send_message(FP_WAREHOUSE_NOTIFY_ADMIN_ID, text)

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
        f"Склад: {found['warehouse']}\n"
        f"Для кого: {state['fp_for_whom']}"
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

            row_index = state.get("fp_row")
            if not row_index:
                send_fps_menu(chat_id, "Не найдено выбранное ФП. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_FPS)

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

            if status != "free":
                clear_state(user_id)
                send_fps_menu(chat_id, "Это ФП недоступно.")
                return

            fp_link = row[0]
            warehouse_name = row[4]
            today = datetime.now().strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            sheet_update_and_refresh(
                SHEET_FPS,
                f"F{row_index}:I{row_index}",
                [[
                    "taken",
                    state["fp_for_whom"],
                    who_took_text,
                    today
                ]]
            )

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
            f"Ссылка: {fp_link}\n"
            f"Для кого: {state['fp_for_whom']}\n"
            f"Кто взял в боте: {who_took_text}"
        )

        tg_send_message(chat_id, f"Ссылка:\n{fp_link}")
        send_accounts_main_menu(chat_id, "Меню Accounts:")

    except Exception as e:
        logging.exception("confirm_fp_issue crashed")
        tg_send_message(chat_id, "Ошибка выдачи ФП. Попробуй ещё раз.")
        send_accounts_main_menu(chat_id, "Меню Accounts:")

def issue_pixels_bulk(chat_id, user_id, username, count_needed):
    try:
        state = get_state(user_id)

        if state.get("mode") != "awaiting_pixel_count":
            send_pixels_menu(chat_id, "Сначала начни выдачу Пикселей заново.")
            return

        pixel_for_whom = state.get("pixel_for_whom")
        if not pixel_for_whom:
            clear_state(user_id)
            send_pixels_menu(chat_id, "Не найдено для кого выдавать Пиксели. Начни заново.")
            return

        found_pixels = find_free_pixels(count_needed)

        if len(found_pixels) < count_needed:
            clear_state(user_id)
            send_pixels_menu(chat_id, f"Недостаточно свободных Пикселей. Доступно: {len(found_pixels)}")
            return

        today = datetime.now().strftime("%d/%m/%Y")
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
                if len(row) < 8:
                    row = row + [''] * (8 - len(row))

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

                pixel_name = extract_pixel_name_from_data(row[7])

                issue_rows.append([
                    pixel_name,                          # A
                    "PIXEL",                            # B
                    row[0],                             # C дата покупки
                    normalize_numeric_for_sheet(row[1]),# D цена
                    today,                              # E дата выдачи
                    row[2],                             # F поставщик
                    pixel_for_whom                      # G кому выдали
                ])

                issued_messages.append({
                    "pixel_name": pixel_name,
                    "data_text": row[7]
                })

            refresh_sheet_cache(SHEET_PIXELS)

            if issue_rows:
                sheet_append_rows_and_refresh(
                    SHEET_ISSUES,
                    issue_rows,
                    value_input_option="USER_ENTERED"
                )

            invalidate_stats_cache()

        clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"Выдано Пикселей: {len(issued_messages)}\n"
            f"Для кого: {pixel_for_whom}\n"
            f"Кто взял в боте: {who_took_text}"
        )

        for i, item in enumerate(issued_messages, start=1):
            tg_send_message(
                chat_id,
                f"Пиксель {i}: {item['pixel_name']}\n\n{item['data_text']}"
            )

        send_pixels_menu(chat_id, "Выбери следующее действие:")

    except Exception:
        logging.exception("issue_pixels_bulk crashed")
        tg_send_message(chat_id, "Ошибка выдачи Пикселей. Попробуй ещё раз.")
        send_pixels_menu(chat_id, "Меню Пикселей:")

def find_pixel_in_base_by_data(pixel_query):
    rows = get_sheet_rows_cached(SHEET_PIXELS)
    target = str(pixel_query).strip().lower()

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 8:
            row = row + [''] * (8 - len(row))

        data_text = str(row[7]).strip().lower()
        if target and target in data_text:
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
        f"Для кого: {state['pixel_for_whom']}"
    )

    keyboard = [
        [{"text": BTN_PIXEL_CONFIRM}, {"text": BTN_PIXEL_NEXT}],
        [{"text": BTN_BACK_TO_MENU}]
    ]

    tg_send_message(chat_id, text, keyboard)


def return_pixel_to_ban(pixel_query):
    found = find_pixel_in_base_by_data(pixel_query)
    if not found:
        return False, "Пиксель не найден."

    row = found["row"]
    if len(row) < 8:
        row = row + [''] * (8 - len(row))

    status = str(row[3]).strip().lower()
    if status == "ban":
        return False, "Этот Пиксель уже в ban."

    sheet_update_and_refresh(
        SHEET_PIXELS,
        f"D{found['row_index']}:E{found['row_index']}",
        [["ban", "ban"]]
    )

    invalidate_stats_cache()
    return True, "Пиксель переведён в ban."

def get_free_farm_king_geos():
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

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


def send_farm_king_geo_options(chat_id):
    geos = get_free_farm_king_geos()

    if not geos:
        send_farm_kings_menu(chat_id, "Нет свободных фарм кингов ни по одному GEO.")
        return

    keyboard = []
    for geo in geos:
        keyboard.append([{"text": geo}])

    keyboard.append([{"text": MENU_CANCEL}])

    tg_send_message(chat_id, "Какое GEO нужно?", keyboard)

def send_free_farm_kings(chat_id):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    free_rows = []
    for row in rows[1:]:
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

        if str(row[4]).strip().lower() == "free":
            free_rows.append(row)

    if not free_rows:
        tg_send_message(chat_id, "Свободных фарм кингов сейчас нет.")
        return

    lines = []
    for i, row in enumerate(free_rows, start=1):
        lines.append(
            f"{i}. {row[0] or '(без названия)'}\n"
            f"Цена: {row[2]} | GEO: {row[7]}\n"
        )

    header = f"Свободные фарм кинги: {len(free_rows)}\n\n"
    current_text = header

    for line in lines:
        if len(current_text) + len(line) > 3500:
            tg_send_message(chat_id, current_text.strip())
            current_text = line + "\n"
        else:
            current_text += line + "\n"

    if current_text.strip():
        tg_send_message(chat_id, current_text.strip())

def find_free_farm_kings(count_needed, geo=None):
    rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    candidates = []
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

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
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

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
    if len(row) < 10:
        row = row + [''] * (10 - len(row))

    return (
        f"Название: {row[0]}\n"
        f"Статус: {row[4] or 'не указан'}\n"
        f"Цена: {row[2] or 'не указана'}\n"
        f"Дата взятия: {row[6] or 'не указана'}\n"
        f"Кто взял: {row[8] or 'не указано'}\n"
        f"Для кого: {row[5] or 'не указано'}\n\n"
        f"Данные:\n{row[9] or 'нет данных'}"
    )


def return_farm_king_to_ban(king_name):
    found = find_farm_king_in_base_by_name(king_name)
    if not found:
        return False, "Кинг не найден в База фарм кинги."

    row = found["row"]
    if len(row) < 10:
        row = row + [''] * (10 - len(row))

    if str(row[4]).strip().lower() == "ban":
        return False, "Этот кинг уже в ban."

    sheet_update_and_refresh(
        SHEET_FARM_KINGS,
        f"E{found['row_index']}:F{found['row_index']}",
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


def issue_farm_kings(chat_id, user_id, username, king_names):
    state = get_state(user_id)
    selected_rows = state.get("farm_king_rows", [])

    if not selected_rows or len(selected_rows) != len(king_names):
        clear_state(user_id)
        send_farm_kings_menu(chat_id, "Ошибка выдачи фарм кингов. Начни заново.")
        return

    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
    who_took_text = f"@{username}" if username else "без username"

    issue_rows = []
    messages = []

    with issue_lock:
        current_rows = get_sheet_rows_cached(SHEET_FARM_KINGS, force=True)

        for item, king_name in zip(selected_rows, king_names):
            row_index = item["row_index"]

            if row_index - 1 >= len(current_rows):
                clear_state(user_id)
                send_farm_kings_menu(chat_id, "Ошибка: один из кингов пропал из таблицы.")
                return

            row = current_rows[row_index - 1]
            if len(row) < 10:
                row = row + [''] * (10 - len(row))

            if str(row[4]).strip().lower() != "free":
                clear_state(user_id)
                send_farm_kings_menu(chat_id, f"Кинг '{row[0] or king_name}' уже не свободен.")
                return

        for item, king_name in zip(selected_rows, king_names):
            row_index = item["row_index"]
            row = current_rows[row_index - 1]
            if len(row) < 10:
                row = row + [''] * (10 - len(row))

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

            issue_rows.append([
                king_name,
                "KING",
                row[1],
                normalize_numeric_for_sheet(row[2]),
                today,
                row[3],
                "farm"
            ])

            messages.append(
                f"{king_name}\n\n{row[9] if len(row) > 9 and row[9] else 'нет данных'}"
            )

        refresh_sheet_cache(SHEET_FARM_KINGS)

        if issue_rows:
            sheet_append_rows_and_refresh(
                SHEET_ISSUES,
                issue_rows,
                value_input_option="USER_ENTERED"
            )

        invalidate_stats_cache()

    clear_state(user_id)

    tg_send_message(chat_id, f"Готово ✅\n\nВыдано кингов: {len(king_names)}")
    for msg_text in messages:
        tg_send_message(chat_id, msg_text)

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


def issue_farm_bm(chat_id, user_id, username):
    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
    who_took_text = f"@{username}" if username else "без username"

    with issue_lock:
        found = find_free_farm_bm()

        if not found:
            send_farm_bms_menu(chat_id, "Свободных фарм BMов сейчас нет.")
            return

        row_index = found["row_index"]
        rows = get_sheet_rows_cached(SHEET_FARM_BMS, force=True)

        if row_index - 1 >= len(rows):
            send_farm_bms_menu(chat_id, "BM не найден в таблице.")
            return

        row = rows[row_index - 1]
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

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

        next_row = get_next_empty_row_in_issues()
        sheet_update_and_refresh(
            SHEET_ISSUES,
            f"A{next_row}:G{next_row}",
            [[
                row[0],
                "БМ",
                row[1],
                normalize_numeric_for_sheet(row[2]),
                today,
                row[3],
                "farm"
            ]]
        )

        invalidate_stats_cache()

    tg_send_message(
        chat_id,
        f"Готово ✅\n\nBM выдан.\nID BM: {row[0]}\nКому передали: farm"
    )

    if len(row) > 8 and row[8]:
        tg_send_message(chat_id, row[8])
    else:
        tg_send_message(chat_id, "Данные BM не найдены.")

    send_farm_bms_menu(chat_id, "Выбери следующее действие:")


def find_free_farm_fps(count_needed):
    rows = get_sheet_rows_cached(SHEET_FARM_FPS)

    candidates = []
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 9:
            row = row + [''] * (9 - len(row))

        if str(row[5]).strip().lower() != "free":
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


def issue_farm_fps(chat_id, user_id, username, count_needed):
    found = find_free_farm_fps(count_needed)

    if len(found) < count_needed:
        send_farm_fps_menu(chat_id, f"Недостаточно свободных FP. Доступно: {len(found)}")
        return

    today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
    who_took_text = f"@{username}" if username else "без username"

    issue_rows = []
    messages = []

    with issue_lock:
        current_rows = get_sheet_rows_cached(SHEET_FARM_FPS, force=True)

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

            messages.append(
                f"Ссылка: {row[0]}\nСклад: {row[4]}"
            )

        refresh_sheet_cache(SHEET_FARM_FPS)

        if issue_rows:
            sheet_append_rows_and_refresh(
                SHEET_ISSUES,
                issue_rows,
                value_input_option="USER_ENTERED"
            )

        invalidate_stats_cache()

    clear_state(user_id)

    tg_send_message(chat_id, f"Готово ✅\n\nВыдано FP: {len(messages)}")
    for msg_text in messages:
        tg_send_message(chat_id, msg_text)

    send_farm_fps_menu(chat_id, "Выбери следующее действие:")
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

def parse_limit_number(value):
    if value is None:
        return None

    text = str(value).strip().lower()
    if not text:
        return None

    if text in ["no limit", "unlim", "unlimited"]:
        return "unlim"

    text = text.replace(" ", "")

    if "," in text:
        integer_part = text.split(",")[0]
    elif "." in text:
        integer_part = text.split(".")[0]
    else:
        integer_part = text

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
        return parsed != "unlim" and 801 <= parsed <= 1200

    if selected_filter == FREE_LIMIT_1500:
        return parsed != "unlim" and 1201 <= parsed <= 1500

    if selected_filter == FREE_LIMIT_UNLIM:
        if parsed == "unlim":
            return True
        return parsed != "unlim" and parsed >= 1501

    return False

def normalize_numeric_for_sheet(value):
    num = parse_price(value)
    if num is None:
        return value

    if float(num).is_integer():
        return int(num)

    return float(num)

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
        if len(row) < 10:
            row = row + [''] * (10 - len(row))
        if str(row[8]).strip().lower() != target_username:
            continue
        transfer_date = parse_sheet_date(str(row[6]).strip())
        if transfer_date and start_date <= transfer_date < end_date:
            kings_count += 1

    crypto_kings_rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)
    for row in crypto_kings_rows[1:]:
        if len(row) < 10:
            row = row + [''] * (10 - len(row))
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

    return (
        f"Статистика accounts {target_username}\n"
        f"Период: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}\n\n"
        f"Кинги: {kings_count}\n"
        f"Лички: {accounts_count}\n"
        f"БМы: {bms_count}\n"
        f"ФП: {fps_count}"
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
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

        who_took = str(row[8]).strip().lower()
        transfer_date = parse_sheet_date(row[6])

        if who_took == target_username and transfer_date and start_date <= transfer_date < end_date:
            kings_lines.append(
                f"{row[0]} | {transfer_date.strftime('%d/%m/%Y')} | {row[5]}"
            )

    crypto_kings_rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)
    for row in crypto_kings_rows[1:]:
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

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
        if len(row) < 10:
            row = row + [''] * (10 - len(row))
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
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

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
        state = user_states.get(str(user_id))

        if not state:
            return {}

        state_time = state.get("_time", 0)

        if time.time() - state_time > STATE_TTL:
            user_states.pop(str(user_id), None)
            return {}

        return dict(state)

def cleanup_states():
    now = time.time()
    to_delete = []

    with state_lock:
        for uid, state in user_states.items():
            if now - state.get("_time", 0) > STATE_TTL:
                to_delete.append(uid)

        for uid in to_delete:
            user_states.pop(uid, None)

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
    data["_time"] = time.time()
    with state_lock:
        user_states[str(user_id)] = data


def clear_state(user_id):
    with state_lock:
        user_states.pop(str(user_id), None)

def set_last_accounts_section(user_id, section_name):
    state = get_state(user_id)
    state["last_accounts_section"] = section_name
    set_state(user_id, state)

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

        values_en = [[
            normalize_numeric_for_sheet(limit_value),      # E
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
    next_row = get_next_empty_row_in_issues()

    sheet_update_and_refresh(
        SHEET_ISSUES,
        f"A{next_row}:G{next_row}",
        [[
            account_number,
            "РК",
            purchase_date,
            normalize_numeric_for_sheet(price),
            transfer_date,
            supplier,
            for_whom
        ]]
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
        rows = get_sheet_rows_cached(SHEET_ACCOUNTS, force=True)

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
        rows = get_sheet_rows_cached(SHEET_ACCOUNTS, force=True)

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

        result = issue_accounts_bulk(
            account_numbers=[account_number],
            for_whom=state["for_whom"],
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
                "for_whom": state["for_whom"]
            })

            tg_send_message(
                chat_id,
                f"Готово ✅\n\n"
                f"Выдана личка: {item['account_number']}\n"
                f"Кому передали: {state['for_whom']}\n"
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
            f"Кому передали: {state['for_whom']}\n"
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
            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
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

            next_row = get_next_empty_row_in_issues()

            sheet_update_and_refresh(
                SHEET_ISSUES,
                f"A{next_row}:G{next_row}",
                [[
                    bm_id,
                    "БМ",
                    purchase_date,
                    normalize_numeric_for_sheet(price),
                    today,
                    supplier,
                    state["bm_for_whom"]
                ]]
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

        send_accounts_main_menu(chat_id, "Меню Accounts:")

    except Exception as e:
        logging.exception("confirm_bm_issue crashed")
        tg_send_message(chat_id, "Ошибка выдачи БМа. Попробуй ещё раз.")
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

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
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

        send_accounts_main_menu(chat_id, "Меню Accounts:")

    except Exception as e:
        logging.error(f"confirm_king_issue error: {e}")
        tg_send_message(chat_id, "Ошибка выдачи кинга. Попробуй ещё раз.")
        send_accounts_main_menu(chat_id, "Меню Accounts:")

def confirm_crypto_king_issue(chat_id, user_id, username):
    try:
        with issue_lock:
            state = get_state(user_id)

            if state.get("mode") != "crypto_king_found":
                send_kings_menu(chat_id, "Сначала выбери crypto king заново.")
                return

            row_index = state.get("king_row")
            if not row_index:
                send_kings_menu(chat_id, "Не найден выбранный crypto king. Начни заново.")
                return

            rows = get_sheet_rows_cached(SHEET_CRYPTO_KINGS)

            if row_index - 1 >= len(rows):
                clear_state(user_id)
                send_kings_menu(chat_id, "Crypto king не найден в таблице. Начни заново.")
                return

            row = rows[row_index - 1]

            if len(row) < 10:
                row = row + [''] * (10 - len(row))

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

            king_name = state["king_name"].strip()

            current_name_in_row = str(row[0]).strip()
            if not current_name_in_row and crypto_king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое название.")
                state["mode"] = "awaiting_crypto_king_name"
                set_state(user_id, state)
                return

            today = datetime.now(MOSCOW_TZ).strftime("%d/%m/%Y")
            who_took_text = f"@{username}" if username else "без username"

            sheet_update_and_refresh(
                SHEET_CRYPTO_KINGS,
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

            data_text = row[9] if len(row) > 9 else ""
            invalidate_stats_cache()
            clear_state(user_id)

        tg_send_message(
            chat_id,
            f"Готово ✅\n\n"
            f"Crypto king выдан.\n"
            f"Название: {king_name}\n"
            f"Для кого: {state['king_for_whom']}\n"
            f"Гео: {row[7]}"
        )

        if data_text:
            tg_send_message(chat_id, data_text)
        else:
            tg_send_message(chat_id, "Данные crypto king не найдены.")

        send_kings_menu(chat_id, "Выбери следующее действие:")

    except Exception as e:
        logging.error(f"confirm_crypto_king_issue error: {e}")
        tg_send_message(chat_id, "Ошибка выдачи crypto king. Попробуй ещё раз.")
        send_kings_menu(chat_id, "Меню кингов:")

def append_king_to_issues_sheet(king_name, purchase_date, price, transfer_date, supplier, for_whom):
    next_row = get_next_empty_row_in_issues()

    sheet_update_and_refresh(
        SHEET_ISSUES,
        f"A{next_row}:G{next_row}",
        [[
            king_name,
            "KING",
            purchase_date,
            normalize_numeric_for_sheet(price),
            transfer_date,
            supplier,
            for_whom
        ]]
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
    kings_free = 0
    kings_taken = 0
    kings_ban = 0
    kings_geo_stats = {}

    for source_sheet in [SHEET_KINGS, SHEET_CRYPTO_KINGS]:
        kings_rows = get_sheet_rows_cached(source_sheet)

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

    # ---------- FARM KINGS ----------
    farm_kings_rows = get_sheet_rows_cached(SHEET_FARM_KINGS)

    farm_kings_free = 0
    farm_kings_taken = 0
    farm_kings_ban = 0
    farm_kings_geo_stats = {}

    for row in farm_kings_rows[1:]:
        if len(row) < 10:
            row = row + [''] * (10 - len(row))

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
            crypto_kings = main_spreadsheet.worksheet(SHEET_CRYPTO_KINGS)
            issues = main_spreadsheet.worksheet(SHEET_ISSUES)
            bms = main_spreadsheet.worksheet(SHEET_BMS)
            fps = main_spreadsheet.worksheet(SHEET_FPS)
            pixels = main_spreadsheet.worksheet(SHEET_PIXELS)
            farm_kings = main_spreadsheet.worksheet(SHEET_FARM_KINGS)
            farm_bms = main_spreadsheet.worksheet(SHEET_FARM_BMS)
            farm_fps = main_spreadsheet.worksheet(SHEET_FARM_FPS)

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

            accounts_data = accounts.get_all_values()
            kings_data = kings.get_all_values()
            crypto_kings_data = crypto_kings.get_all_values()
            issues_data = issues.get_all_values()
            bms_data = bms.get_all_values()
            fps_data = fps.get_all_values()
            pixels_data = pixels.get_all_values()
            farm_kings_data = farm_kings.get_all_values()
            farm_bms_data = farm_bms.get_all_values()
            farm_fps_data = farm_fps.get_all_values()

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

            if accounts_data:
                backup_accounts.append_rows(accounts_data)

            if kings_data:
                backup_kings.append_rows(kings_data)

            if issues_data:
                backup_issues.append_rows(issues_data)

            if bms_data:
                backup_bms.append_rows(bms_data)

            if fps_data:
                backup_fps.append_rows(fps_data)

            if pixels_data:
                backup_pixels.append_rows(pixels_data)

            if farm_kings_data:
                backup_farm_kings.append_rows(farm_kings_data)

            if farm_bms_data:
                backup_farm_bms.append_rows(farm_bms_data)

            if farm_fps_data:
                backup_farm_fps.append_rows(farm_fps_data)

            if crypto_kings_data:
                backup_crypto_kings.append_rows(crypto_kings_data)

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

    # 1. Проверка токена и Telegram API
    try:
        if BOT_TOKEN and BASE_URL.startswith("https://api.telegram.org/bot"):
            add_result("Подключение к Telegram настроено", True)
        else:
            add_result("Подключение к Telegram настроено", False, "BOT_TOKEN или BASE_URL заполнены неправильно")
    except Exception as e:
        add_result("Подключение к Telegram настроено", False, str(e))

    # 2. Проверка Google credentials
    try:
        client = get_gspread_client()
        add_result("Авторизация в Google Sheets", client is not None)
    except Exception as e:
        add_result("Авторизация в Google Sheets", False, str(e))

    # 3. Проверка основной таблицы
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        add_result("Основная таблица открывается", True, f"Название: {spreadsheet.title}")
    except Exception as e:
        add_result("Основная таблица открывается", False, str(e))

    # 4. Проверка всех рабочих листов
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

    # 5. Проверка backup таблицы
    try:
        client = get_gspread_client()
        backup_spreadsheet = client.open_by_key(BACKUP_SPREADSHEET_ID)
        add_result("Бэкап таблица открывается", True, f"Название: {backup_spreadsheet.title}")
    except Exception as e:
        add_result("Бэкап таблица открывается", False, str(e))

    # 6. Проверка обновления кеша
    try:
        refresh_sheet_cache(SHEET_ACCOUNTS)
        refresh_sheet_cache(SHEET_ISSUES)
        add_result("Кеш таблиц обновляется", True)
    except Exception as e:
        add_result("Кеш таблиц обновляется", False, str(e))

    # 7. Проверка вспомогательных функций
    try:
        parse_date("15/02/2026")
        parse_price("123.45")
        normalize_numeric_for_sheet("100")
        add_result("Базовые функции работают", True)
    except Exception as e:
        add_result("Базовые функции работают", False, str(e))

    # 8. Проверка общей статистики
    try:
        build_stats_text()
        add_result("Общая статистика собирается", True)
    except Exception as e:
        add_result("Общая статистика собирается", False, str(e))

    # 9. Проверка статистики accounts
    try:
        build_manager_stats_text("test_user")
        add_result("Статистика accounts собирается", True)
    except Exception as e:
        add_result("Статистика accounts собирается", False, str(e))

    # 10. Проверка статистики farmers
    try:
        build_farmer_stats_text("test_user")
        add_result("Статистика farmers собирается", True)
    except Exception as e:
        add_result("Статистика farmers собирается", False, str(e))

    summary = (
        "Проверка бота завершена\n\n"
        f"Успешно: {ok_count}\n"
        f"Ошибок: {fail_count}\n\n"
        "Что именно проверено:\n"
        + "\n".join(report)
    )

    return summary

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

        now = time.time()
        with user_action_lock:
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
            send_accounts_main_menu(chat_id, "Меню Accounts:")
            return

        if text == MENU_FARMER_STATS:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к этой статистике.")
                return

            farmer_stats_text = build_farmer_stats_text(username)
            tg_send_message(chat_id, farmer_stats_text)
            send_farmers_menu(chat_id, "Меню Farmers:")
            return

        if text == MENU_ADMIN:
            if not is_admin(user_id):
                tg_send_message(chat_id, "У вас нет доступа к меню Admin.")
                return

            clear_state(user_id)
            send_admin_menu(chat_id)
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

            tg_send_message(chat_id, "Запускаю полную проверку бота...")
            result = run_bot_diagnostics()
            tg_send_long_message(chat_id, result)
            send_admin_menu(chat_id, "Меню Admin:")
            return

        if text == BTN_BACK_FROM_ADMIN:
            clear_state(user_id)
            send_main_menu(chat_id, user_id=user_id)
            return

        if text == BTN_BACK_FROM_ACCOUNTANTS:
            clear_state(user_id)
            send_admin_menu(chat_id)
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
            last_section = state.get("last_accounts_section", "")
            clear_state(user_id)

            if last_section == "kings":
                set_state(user_id, {"last_accounts_section": "kings"})
                send_kings_menu(chat_id, "Меню кингов:")
                return

            if last_section == "bms":
                set_state(user_id, {"last_accounts_section": "bms"})
                send_bms_menu(chat_id, "Меню БМов:")
                return

            if last_section == "fps":
                set_state(user_id, {"last_accounts_section": "fps"})
                send_fps_menu(chat_id, "Меню ФП:")
                return

            if last_section == "pixels":
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
            send_farm_kings_menu(chat_id)
            return

        if text == FARM_MENU_BM:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к разделу Farmers.")
                return

            clear_state(user_id)
            send_farm_bms_menu(chat_id)
            return

        if text == FARM_MENU_FP:
            if not (is_admin(user_id) or is_farmers_user(user_id)):
                tg_send_message(chat_id, "У вас нет доступа к разделу Farmers.")
                return

            clear_state(user_id)
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
            set_state(user_id, {"mode": "awaiting_free_accounts_limit"})
            send_free_accounts_limit_menu(chat_id, "Выбери лимит для показа свободных личек:")
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

        if text == BTN_ISSUE_MORE:
            if state.get("mode") != "quick_issue_continue":
                send_accounts_menu(chat_id, "Сначала начни быструю выдачу заново.")
                return

            result = issue_next_quick_account_for_person(
                for_whom=state["for_whom"],
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
            if state.get("mode") != "awaiting_return_confirm":
                send_accounts_menu(chat_id, "Сначала выбери действие заново.")
                return

            account_number = state.get("return_account_number", "")
            ok, message = return_account_to_ban(account_number)
            clear_state(user_id)
            send_accounts_main_menu(chat_id, message)
            return

        # ========= КИНГИ =========
        if text == SUBMENU_FREE_KINGS:
            send_free_kings(chat_id)
            send_accounts_main_menu(chat_id, "Меню Accounts:")
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

        if text == SUBMENU_CRYPTO_KINGS:
            clear_state(user_id)
            set_state(user_id, {"mode": "awaiting_crypto_king_geo"})
            send_crypto_king_geo_options(chat_id)
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

            if state.get("mode") == "crypto_king_found" or state.get("mode") == "awaiting_crypto_king_name":
                if not state.get("king_geo"):
                    send_kings_menu(chat_id, "Начни заново.")
                    return

                found = find_free_crypto_king_by_geo(
                    state["king_geo"],
                    exclude_row=state.get("king_row")
                )

                if not found:
                    clear_state(user_id)
                    send_kings_menu(chat_id, "Свободных crypto king с таким GEO больше нет.")
                    return

                show_found_crypto_king(chat_id, user_id, found)
                return

            if not state.get("king_geo"):
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
            send_accounts_main_menu(chat_id, message)
            return

        # ========= БМы =========
        if text == SUBMENU_FREE_BMS:
            free_count = count_free_bms()
            tg_send_message(chat_id, f"Свободных БМов: {free_count}")
            send_accounts_main_menu(chat_id, "Меню Accounts:")
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

        # ========= фп =========
        if text == SUBMENU_SEARCH_FP:
            set_state(user_id, {"mode": "awaiting_search_fp"})
            tg_send_message(chat_id, "Впиши ссылку ФП для поиска.")
            return

        if text == SUBMENU_GET_FP:
            clear_state(user_id)
            set_state(user_id, {"mode": "awaiting_fp_department"})
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
            set_state(user_id, {"mode": "awaiting_search_pixel"})
            tg_send_message(chat_id, "Впиши часть данных Пикселя для поиска.")
            return

        if text == SUBMENU_RETURN_PIXEL:
            set_state(user_id, {"mode": "awaiting_return_pixel"})
            tg_send_message(chat_id, "Впиши часть данных Пикселя, который нужно перевести в ban.")
            return

        if text == SUBMENU_GET_PIXELS:
            clear_state(user_id)
            set_state(user_id, {"mode": "awaiting_pixel_department"})
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

            pixel_query = state.get("return_pixel_query", "")
            ok, message = return_pixel_to_ban(pixel_query)
            clear_state(user_id)
            send_pixels_menu(chat_id, message)
            return
        
        # ========= FARMERS =========
        if text == FARM_SUBMENU_FREE_KINGS:
            send_free_farm_kings(chat_id)
            send_farm_kings_menu(chat_id, "Выбери следующее действие:")
            return

        if text == FARM_SUBMENU_GET_KINGS:
            set_state(user_id, {"mode": "awaiting_farm_king_geo"})
            send_farm_king_geo_options(chat_id)
            return

        if text == FARM_SUBMENU_RETURN_KING:
            set_state(user_id, {"mode": "awaiting_farm_return_king_name"})
            tg_send_message(chat_id, "Впиши название кинга.")
            return

        if text == FARM_SUBMENU_SEARCH_KING:
            set_state(user_id, {"mode": "awaiting_farm_search_king_name"})
            tg_send_message(chat_id, "Впиши название кинга.")
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

        if text == FARM_SUBMENU_GET_FP:
            set_state(user_id, {"mode": "awaiting_farm_fp_count"})
            tg_send_message(chat_id, "Сколько FP нужно?")
            return

        if text == FARM_SUBMENU_SEARCH_FP:
            set_state(user_id, {"mode": "awaiting_farm_search_fp"})
            tg_send_message(chat_id, "Впиши ссылку FP.")
            return

        if text == BTN_FARM_KINGS_PARTIAL_CONFIRM:
            if state.get("mode") != "awaiting_farm_kings_partial_confirm":
                send_farm_kings_menu(chat_id, "Начни заново.")
                return

            state["mode"] = "awaiting_farm_king_names"
            set_state(user_id, state)

            tg_send_message(
                chat_id,
                f"Пришли {state['farm_kings_count']} названий для кингов.\nКаждое название с новой строки."
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

        if state.get("mode") == "awaiting_fps_add":
            result = add_fps_from_text(text)
            clear_state(user_id)
            tg_send_message(chat_id, result)

            if is_admin(user_id):
                send_admin_menu(chat_id, "Выбери следующее действие:")
            else:
                send_main_menu(chat_id, "Готово. Выбери следующее действие:", user_id=user_id)
            return

        if state.get("mode") == "awaiting_farm_fps_add":
            result = add_fps_from_text(text, target_sheet=SHEET_FARM_FPS)
            clear_state(user_id)
            tg_send_message(chat_id, result)

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

            tg_send_message(chat_id, "Теперь напиши номер лички или несколько номеров, каждый с новой строки.")
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
        if state.get("mode") == "awaiting_crypto_king_geo":
            geos = get_free_crypto_king_geos()

            if text not in geos:
                send_crypto_king_geo_options(chat_id)
                return

            set_state(user_id, {
                "mode": "awaiting_crypto_king_department",
                "king_geo": text
            })

            keyboard = [
                [{"text": DEPT_CRYPTO}],
                [{"text": MENU_CANCEL}]
            ]
            tg_send_message(chat_id, "Выбери для кого crypto king:", keyboard)
            return
        

        if state.get("mode") == "awaiting_crypto_king_department":
            if text != DEPT_CRYPTO:
                keyboard = [
                    [{"text": DEPT_CRYPTO}],
                    [{"text": MENU_CANCEL}]
                ]
                tg_send_message(chat_id, "Нужно выбрать Крипта кнопкой:", keyboard)
                return

            state["mode"] = "awaiting_crypto_king_for_whom"
            state["king_department"] = text
            set_state(user_id, state)

            send_person_menu(chat_id, DEPT_CRYPTO)
            return


        if state.get("mode") == "awaiting_crypto_king_for_whom":
            if text not in CRYPTO_NAMES:
                send_person_menu(chat_id, DEPT_CRYPTO)
                return

            state["mode"] = "awaiting_crypto_king_name"
            state["king_for_whom"] = text
            set_state(user_id, state)

            tg_send_message(chat_id, "Какое название будет у crypto king?")
            return


        if state.get("mode") == "awaiting_crypto_king_name":
            king_name = text.strip()

            if not king_name:
                tg_send_message(chat_id, "Напиши название crypto king.")
                return

            if crypto_king_name_exists(king_name):
                tg_send_message(chat_id, f"Название '{king_name}' уже существует. Напиши другое.")
                return

            state["mode"] = "crypto_king_found"
            state["king_name"] = king_name
            set_state(user_id, state)

            found = find_free_crypto_king_by_geo(state["king_geo"])

            if not found:
                clear_state(user_id)
                send_kings_menu(chat_id, "Свободных crypto king с таким GEO нет.")
                return

            show_found_crypto_king(chat_id, user_id, found)
            return
        
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
            send_accounts_main_menu(chat_id, "Меню Accounts:")
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
            send_accounts_main_menu(chat_id, "Меню Accounts:")
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
                tg_send_message(chat_id, "Впиши данные Пикселя.")
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
                tg_send_message(chat_id, "Впиши данные Пикселя.")
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

            set_state(user_id, {
                "mode": "awaiting_pixel_count",
                "pixel_department": state.get("pixel_department"),
                "pixel_for_whom": text
            })

            tg_send_message(chat_id, "Сколько Пикселей нужно?")
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
            fp_link = text.strip()

            if not fp_link:
                tg_send_message(chat_id, "Впиши ссылку ФП.")
                return

            result = build_fp_search_text(fp_link)
            clear_state(user_id)

            if not result:
                send_fps_menu(chat_id, "ФП не найдено.")
                return

            tg_send_message(chat_id, result)
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

            state["fp_for_whom"] = text
            set_state(user_id, state)

            found = find_free_fp()

            if not found:
                clear_state(user_id)
                send_fps_menu(chat_id, "Свободных ФП сейчас нет.")
                return

            show_found_fp(chat_id, user_id, found)
            return

        # ========= СОСТОЯНИЯ: FARM KING =========
        if state.get("mode") == "awaiting_farm_king_geo":
            geos = get_free_farm_king_geos()

            if text not in geos:
                send_farm_king_geo_options(chat_id)
                return

            set_state(user_id, {
                "mode": "awaiting_farm_kings_count",
                "farm_king_geo": text
            })

            tg_send_message(chat_id, f"Сколько кингов нужно для GEO {text}?")
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

            tg_send_message(
                chat_id,
                f"Пришли {count_needed} названий для кингов GEO {selected_geo}.\nКаждое название с новой строки."
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
                tg_send_message(chat_id, "Впиши название кинга.")
                return

            result = build_farm_king_search_text(king_name)
            clear_state(user_id)

            if not result:
                send_farm_kings_menu(chat_id, "Кинг не найден.")
                return

            tg_send_message(chat_id, result)
            send_farm_kings_menu(chat_id, "Выбери следующее действие:")
            return

        if state.get("mode") == "awaiting_farm_return_king_name":
            king_name = text.strip()

            if not king_name:
                tg_send_message(chat_id, "Впиши название кинга.")
                return

            ok, message = return_farm_king_to_ban(king_name)
            clear_state(user_id)
            send_farm_kings_menu(chat_id, message)
            return

        # ========= СОСТОЯНИЯ: FARM BM =========
        if state.get("mode") == "awaiting_farm_search_bm":
            bm_id = text.strip()

            if not bm_id:
                tg_send_message(chat_id, "Впиши ID BM.")
                return

            result = build_farm_bm_search_text(bm_id)
            clear_state(user_id)

            if not result:
                send_farm_bms_menu(chat_id, "BM не найден.")
                return

            tg_send_message(chat_id, result)
            send_farm_bms_menu(chat_id, "Выбери следующее действие:")
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
                tg_send_message(chat_id, "Впиши ссылку FP.")
                return

            result = build_farm_fp_search_text(fp_link)
            clear_state(user_id)

            if not result:
                send_farm_fps_menu(chat_id, "FP не найдено.")
                return

            tg_send_message(chat_id, result)
            send_farm_fps_menu(chat_id, "Выбери следующее действие:")
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

def handle_callback_query(callback_query):
    try:
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
            full_text = build_manager_stats_text(username)

            tg_answer_callback_query(callback_id)
            safe_replace_stats_message(
                chat_id=chat_id,
                message_id=message_id,
                full_text=full_text,
                back_callback_data=f"backstats_accounts:{username}"
            )
            return

        if data.startswith("fullstats_farmers:"):
            username = data.split(":", 1)[1]
            full_text = build_farmer_stats_text(username)

            tg_answer_callback_query(callback_id)
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

        tg_answer_callback_query(callback_id, "Неизвестная команда")

    except Exception as e:
        logging.exception("handle_callback_query crashed")
        try:
            tg_answer_callback_query(callback_query["id"], "Ошибка")
        except Exception:
            pass

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
    return "healthy", 200


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
        return jsonify({"ok": True})

@app.route("/fastadscheck-import", methods=["POST"])
def fastadscheck_import():
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

        updated = 0
        not_found = []
        skipped = []

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

            ok, _ = update_account_from_fastadscheck(
                account_number=account_number,
                limit_value=limit_value,
                threshold_value=threshold_value,
                gmt_value=gmt_value,
                currency_value=currency_value,
                account_url=account_url
            )

            if ok:
                updated += 1
            else:
                not_found.append(account_number)

        refresh_sheet_cache(SHEET_ACCOUNTS)
        invalidate_stats_cache()

        return jsonify({
            "ok": True,
            "updated": updated,
            "not_found": not_found[:100],
            "skipped": skipped[:100],
            "received": len(rows)
        }), 200

    except Exception as e:
        logging.exception("fastadscheck_import crashed")
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500

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

                to_append.append([
                    account_id,
                    "",
                    "",
                    "",
                    normalize_numeric_for_sheet(limit_value) if limit_value is not None else "",
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
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500
        
if __name__ == "__main__":
    # cache_thread = threading.Thread(target=cache_warmer_loop, daemon=True)
    # cache_thread.start()

    backup_thread = threading.Thread(target=backup_scheduler_loop, daemon=True)
    backup_thread.start()

    watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
    watchdog_thread.start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
