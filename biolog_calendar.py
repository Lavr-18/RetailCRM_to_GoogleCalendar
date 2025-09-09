import os
import requests
import openai
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Для загрузки переменных окружения из .env
from dotenv import load_dotenv

# Для работы с Google Calendar API
import google.auth
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os.path
import pickle

# Загружаем переменные окружения из файла .env
load_dotenv()

# --- Конфигурация RetailCRM ---
RETAILCRM_BASE_URL = os.getenv('RETAILCRM_BASE_URL')
RETAILCRM_API_KEY = os.getenv('RETAILCRM_API_KEY')
RETAILCRM_SITE_CODE = os.getenv('RETAILCRM_SITE_CODE')

if not RETAILCRM_BASE_URL or not RETAILCRM_API_KEY or not RETAILCRM_SITE_CODE:
    print(
        "Ошибка: Не все переменные RetailCRM (RETAILCRM_BASE_URL, RETAILCRM_API_KEY, RETAILCRM_SITE_CODE) найдены в файле .env. Пожалуйста, проверьте настройки.")
    exit()

# Настройки Telegram (для уведомлений об ошибках, если нужны)
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print("Внимание: Токен Telegram бота или Chat ID не найдены в .env. Уведомления об ошибках отправляться не будут.")

# Настройки Google Calendar API
SCOPES = ['https://www.googleapis.com/auth/calendar']
CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID')  # ID вашего календаря

# Период для поиска заказов (только за сегодняшний день)
today = datetime.now()
REPORT_START_DATE = datetime(today.year, today.month, today.day)
REPORT_END_DATE = datetime(today.year, today.month, today.day, 23, 59, 59)

# # Временно устанавливаем конкретную дату для поиска
# test_date = datetime(2025, 9, 3)
# REPORT_START_DATE = datetime(test_date.year, test_date.month, test_date.day)
# REPORT_END_DATE = datetime(test_date.year, test_date.month, test_date.day, 23, 59, 59)

# Артикулы услуг "Выезд биолога"
BIOLOGIST_SERVICE_KEYWORDS = [
    "28063", "acfvbkQRh1vbMh95fh9lo0", "k-RDynuFhLNdo8rsWOqGo2",
    "26483", "2tZjx-wpg65Ie5vRryvdt0", "YXJrVu5tja9fE-BBl2V-j0",
    "26481", "suODkHGjgaMvqLkJGUR4F2", "IpYqw1O8jVX21Fa2Ie30O2",
    "kyp58frLhC9GqlEh2e4RR1"
]
BIOLOGIST_SERVICE_USE_SKU = True

REQUEST_TIMEOUT = 120


# --- Вспомогательные функции ---

def send_telegram_message(message: str):
    """Отправляет сообщение (например, об ошибке или информацию) в Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при отправке сообщения в Telegram: {e}")


def get_google_calendar_service():
    """Аутентифицируется и возвращает сервис Google Calendar API."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('calendar', 'v3', credentials=creds)


def get_or_create_calendar(service, calendar_name="Выезд Биолога"):
    """
    Ищет календарь по названию. Если не находит, создает новый.
    Возвращает ID календаря.
    """
    global CALENDAR_ID
    if CALENDAR_ID:
        return CALENDAR_ID

    calendar_list = service.calendarList().list().execute()
    for calendar_item in calendar_list.get('items', []):
        if calendar_item.get('summary') == calendar_name:
            print(f"Найден существующий календарь: '{calendar_name}' с ID: {calendar_item['id']}")
            CALENDAR_ID = calendar_item['id']
            return CALENDAR_ID

    # Календарь не найден, создаем новый
    new_calendar = {
        'summary': calendar_name,
        'timeZone': 'Europe/Moscow'  # Время по Москве
    }
    created_calendar = service.calendars().insert(body=new_calendar).execute()
    print(f"Создан новый календарь: '{created_calendar['summary']}' с ID: {created_calendar['id']}")
    CALENDAR_ID = created_calendar['id']
    send_telegram_message(
        f"✅ *Google Календарь создан:* Создан новый календарь '{created_calendar['summary']}'. ID: `{created_calendar['id']}`. Пожалуйста, сохраните этот ID в вашем файле .env в переменной GOOGLE_CALENDAR_ID, чтобы не создавать его снова."
    )
    return CALENDAR_ID


def create_calendar_event(service, calendar_id, order_data):
    """
    Создает событие в Google Календаре на основе данных заказа.
    """
    try:
        # Извлекаем данные из заказа
        order_internal_id = order_data.get('id', 'N/A')
        order_external_id = order_data.get('externalId', 'N/A')
        client_name = order_data.get('firstName', '')
        client_phone = order_data.get('phone', 'Не указан')
        manager_comment = order_data.get('managerComment', 'Без комментария')

        # Дата и время выезда из кастомного поля
        departure_datetime_str = order_data.get('customFields', {}).get('data_vyezda')

        if not departure_datetime_str:
            print(f"Пропускаю заказ {order_internal_id}: не найдена дата выезда в поле 'data_vyezda'.")
            return

        # Преобразуем строку даты-времени в нужный формат для Google Calendar API
        try:
            start_datetime = datetime.strptime(departure_datetime_str, '%Y-%m-%d %H:%M:%S')
            end_datetime = start_datetime + timedelta(hours=2)  # Длительность события 2 часа
        except ValueError:
            print(f"Ошибка формата даты/времени '{departure_datetime_str}' для заказа {order_internal_id}. Пропускаю.")
            return

        # Извлекаем имя биолога и форматируем его
        biologist_name_raw = order_data.get('customFields', {}).get('biolog', '')
        biologist_name = biologist_name_raw.capitalize() if biologist_name_raw else 'Не назначен'

        # Создаем заголовок события
        event_summary = f"Выезд биолога: {biologist_name}"

        # Создаем описание события
        event_description = (
            f"Заказ CRM ID: {order_internal_id}\n"
            f"Заказ External ID: {order_external_id}\n"
            f"Клиент: {client_name}\n"
            f"Телефон: {client_phone}\n"
            f"Менеджер: {order_data.get('manager', {}).get('firstName', '')}\n"
            f"Комментарий: {manager_comment}\n"
            f"Ссылка на заказ: {RETAILCRM_BASE_URL}/orders/{order_internal_id}/edit"
        )

        # Создаем тело события
        event = {
            'summary': event_summary,
            'description': event_description,
            'start': {
                'dateTime': start_datetime.isoformat(),
                'timeZone': 'Europe/Moscow',
            },
            'end': {
                'dateTime': end_datetime.isoformat(),
                'timeZone': 'Europe/Moscow',
            }
        }

        # Отправляем событие в Google Calendar
        created_event = service.events().insert(calendarId=calendar_id, body=event).execute()
        print(f"Событие создано для заказа {order_internal_id}. Ссылка: {created_event.get('htmlLink')}")

    except Exception as e:
        print(f"Ошибка при создании события в Google Calendar для заказа {order_internal_id}: {e}")
        send_telegram_message(
            f"‼️ *Ошибка Google Calendar API:* Не удалось создать событие для заказа {order_internal_id}. Ошибка: {e}")


def fetch_data_from_retailcrm(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Универсальная функция для получения данных из RetailCRM API."""
    url = f"{RETAILCRM_BASE_URL}/api/v5/{endpoint}"
    if params is None:
        params = {}
    params["apiKey"] = RETAILCRM_API_KEY
    params['site'] = RETAILCRM_SITE_CODE

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_msg = f"Ошибка при запросе к RetailCRM API (endpoint: {endpoint}): {e}"
        print(error_msg)
        if 'response' in locals() and response.text:
            print(f"Ответ API: {response.text}")
            error_msg += f"\nОтвет API: {response.text}"
        send_telegram_message(f"‼️ *Ошибка RetailCRM API:* {error_msg}")
        return {}
    except ValueError as e:
        error_msg = f"Ошибка при парсинге JSON ответа RetailCRM (endpoint: {endpoint}): {e}"
        print(error_msg)
        if 'response' in locals() and response.text:
            print(f"Ответ API: {response.text}")
            error_msg += f"\nОтвет API: {response.text}"
        send_telegram_message(f"‼️ *Ошибка парсинга JSON:* {error_msg}")
        return {}


def get_orders_for_period(start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    """Получает заказы из RetailCRM за указанный период времени."""
    url_suffix = "orders"
    params = {
        'filter[createdAtFrom]': start_date.isoformat(),
        'filter[createdAtTo]': end_date.isoformat(),
        'limit': 100
    }
    all_orders = []
    page = 1
    while True:
        params['page'] = page
        data = fetch_data_from_retailcrm(url_suffix, params=params)
        if 'orders' in data and data['orders']:
            all_orders.extend(data['orders'])
            if len(data['orders']) < params['limit']:
                break
            page += 1
        else:
            break
    return all_orders


def generate_biologist_calendar_report():
    """
    Получает заказы с услугами биолога, извлекает дату выезда,
    выводит отчет и создает события в Google Календаре.
    """
    print(
        f"Ищу заказы с услугами биолога за период с {REPORT_START_DATE.strftime('%d.%m.%Y %H:%M')} по {REPORT_END_DATE.strftime('%d.%m.%Y %H:%M')}..."
    )
    orders = get_orders_for_period(REPORT_START_DATE, REPORT_END_DATE)
    if not orders:
        print("Заказы за указанный период не найдены или произошла ошибка получения данных.")
        return

    # Инициализация Google Calendar API
    calendar_service = get_google_calendar_service()
    biologist_calendar_id = get_or_create_calendar(calendar_service)

    if not biologist_calendar_id:
        print("Не удалось получить или создать Google Календарь. Операция прервана.")
        return

    print("\n--- Найденные заказы с услугами биолога ---")
    print("{:<22} {:<15} {:<12} {:<50} {:<30} {:<15} {:<12}".format(
        "Время создания заказа", "External ID", "CRM ID", "Ссылка на заказ", "Ответственный", "Сумма услуги",
        "Дата выезда"
    ))
    print("-" * 157)
    found_count = 0
    for order_data in orders:
        order_external_id = order_data.get('externalId', 'N/A')
        order_internal_id = order_data.get('id', 'N/A')
        order_created_at_str = order_data.get('createdAt')
        order_manager = order_data.get('manager', {}).get('firstName', '') + ' ' + order_data.get('manager', {}).get(
            'lastName', '') or f"ID: {order_data.get('managerId', 'N/A')}"
        order_link = f"{RETAILCRM_BASE_URL}/orders/{order_internal_id}/edit"
        biologist_service_price = 0.0
        found_biologist_service_in_order = False
        items = order_data.get('items', [])
        for item in items:
            item_offer = item.get('offer', {})
            item_xml_id = str(item_offer.get('xmlId', ''))
            item_id = str(item_offer.get('id', ''))
            if (BIOLOGIST_SERVICE_USE_SKU and item_xml_id.lower() in [k.lower() for k in BIOLOGIST_SERVICE_KEYWORDS]) or \
                    (not BIOLOGIST_SERVICE_USE_SKU and item_id.lower() in [k.lower() for k in
                                                                           BIOLOGIST_SERVICE_KEYWORDS]):
                item_price = float(item.get('initialPrice', 0)) * int(item.get('quantity', 1))
                biologist_service_price += item_price
                found_biologist_service_in_order = True

        if found_biologist_service_in_order:
            departure_datetime_str = order_data.get('customFields', {}).get('data_vyezda')

            created_at_dt = None
            if order_created_at_str:
                try:
                    created_at_dt = datetime.fromisoformat(order_created_at_str.replace('Z', '+00:00'))
                except ValueError:
                    try:
                        created_at_dt = datetime.strptime(order_created_at_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        pass
            timestamp_for_display = created_at_dt.strftime('%d.%m.%Y %H:%M:%S') if created_at_dt else "Неизвестно"

            print("{:<22} {:<15} {:<12} {:<50} {:<30} {:<15.2f} {:<12}".format(
                timestamp_for_display,
                order_external_id,
                order_internal_id,
                order_link,
                order_manager,
                biologist_service_price,
                departure_datetime_str if departure_datetime_str else "N/A"
            ))

            # Создаем событие в календаре
            if departure_datetime_str:
                create_calendar_event(calendar_service, biologist_calendar_id, order_data)

            found_count += 1

    if found_count == 0:
        print("За указанный период не найдено ни одного заказа, содержащего услуги 'Выезд биолога'.")
        send_telegram_message(
            f"ℹ️ *Информация:* За период с {REPORT_START_DATE.strftime('%d.%m')} по {REPORT_END_DATE.strftime('%d.%m')} не найдено заказов с услугами биолога."
        )
    else:
        print("-" * 157)
        print(f"Всего найдено заказов с услугами биолога: {found_count}")
        send_telegram_message(
            f"✅ *Отчет сгенерирован:* За период с {REPORT_START_DATE.strftime('%d.%m')} по {REPORT_END_DATE.strftime('%d.%m')} найдено {found_count} заказов с услугами биолога. Подробности в консоли."
        )


# --- Запуск скрипта ---
if __name__ == "__main__":
    print("Начинаю поиск заказов с услугами биолога...")
    generate_biologist_calendar_report()
    print("Поиск завершен.")