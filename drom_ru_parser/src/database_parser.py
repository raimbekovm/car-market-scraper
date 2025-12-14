"""
ПАРСЕР DROM.RU
Скрапит объявления автомобилей с сайта auto.drom.ru
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import time
from typing import List, Dict, Any, Set, Optional
import random
import os
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Проверяем наличие brotli
try:
    import brotli
    print("✅ Brotli установлен")
except ImportError:
    print("❌ ОШИБКА: Библиотека brotli не установлена!")
    print("   Установите её командой: pip install brotli")
    exit(1)

# Получаем директорию, где находится скрипт
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# НАСТРОЙКА: порог для "малого" количества объявлений
SMALL_BRAND_THRESHOLD = 20


# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С ПРОГРЕССОМ ==========

def load_progress():
    """Загружает прогресс из файла"""
    progress_file = os.path.join(SCRIPT_DIR, 'drom_scraped_data_progress.json')
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()

                if not content:
                    print(f"⚠️  Файл прогресса пустой, начинаем сначала")
                    return None

                data = json.loads(content)

                if 'last_index' not in data or 'statistics' not in data:
                    print(f"⚠️  Файл прогресса поврежден")
                    return None

                print(f"\n📂 НАЙДЕН ФАЙЛ ПРОГРЕССА")
                print(f"   Последняя обработанная строка: {data.get('last_index', -1) + 1}")
                print(f"   Успешно: {data['statistics'].get('successful', 0)}")
                print(f"   Ошибок: {data['statistics'].get('failed', 0)}")
                print(f"   Без результатов: {data['statistics'].get('no_results', 0)}")
                return data
        except json.JSONDecodeError as e:
            print(f"⚠️  Ошибка парсинга JSON: {e}")
            return None
        except Exception as e:
            print(f"⚠️  Ошибка загрузки прогресса: {e}")
            return None
    return None


# Загружаем данные из файла
excel_file = os.path.join(SCRIPT_DIR, 'недостающие модели и поколения_updated2.xlsx')
df = pd.read_excel(excel_file)

# Загружаем прогресс
progress_data = load_progress()

if progress_data:
    start_index = progress_data.get('last_index', -1) + 1
    results = progress_data.get('results', [])
    statistics = progress_data.get('statistics', {
        'total_rows': len(df),
        'successful': 0,
        'failed': 0,
        'no_results': 0,
        'total_ads_found': 0,
        'skipped_brands': 0,
        'errors': []
    })
    brand_cache = progress_data.get('brand_cache', {})
    request_count = progress_data.get('request_count', 0)

    print(f"🔄 ПРОДОЛЖАЕМ С СТРОКИ {start_index + 1} из {len(df)}")
    print(f"   Прогресс: {start_index}/{len(df)} ({start_index / len(df) * 100:.1f}%)")
    print(f"   Выполнено запросов: {request_count}")
    print(f"{'=' * 70}\n")
else:
    start_index = 0
    results = []
    statistics = {
        'total_rows': len(df),
        'successful': 0,
        'failed': 0,
        'no_results': 0,
        'total_ads_found': 0,
        'skipped_brands': 0,
        'errors': []
    }
    brand_cache = {}
    request_count = 0

    print(f"🆕 НАЧИНАЕМ С НАЧАЛА")
    print(f"   Всего строк: {len(df)}")
    print(f"{'=' * 70}\n")

# User-Agent список
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]


def create_session():
    """Создает сессию с повторными попытками"""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_headers():
    """Генерирует случайные заголовки"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }


def decode_response(response):
    """Декодирует ответ с учетом Content-Encoding и charset"""
    content_encoding = response.headers.get('Content-Encoding', '').lower()
    content_type = response.headers.get('Content-Type', '')

    # Определяем кодировку из Content-Type (обычно windows-1251 для drom.ru)
    encoding = 'windows-1251'
    if 'charset=' in content_type:
        encoding = content_type.split('charset=')[-1].strip()

    # Читаем сырые данные
    raw_content = response.raw.read()

    try:
        if content_encoding == 'br':
            return brotli.decompress(raw_content).decode(encoding)
        elif content_encoding == 'gzip':
            import gzip
            return gzip.decompress(raw_content).decode(encoding)
        else:
            return raw_content.decode(encoding)
    except UnicodeDecodeError:
        # Fallback на windows-1251
        if content_encoding == 'br':
            return brotli.decompress(raw_content).decode('windows-1251')
        elif content_encoding == 'gzip':
            import gzip
            return gzip.decompress(raw_content).decode('windows-1251')
        else:
            return raw_content.decode('windows-1251')


def adaptive_delay(request_count, base_min=0.5, base_max=1):
    """Адаптивная пауза - минимальные значения для drom.ru"""
    multiplier = 1 + (request_count // 50) * 0.1
    min_delay = min(base_min * multiplier, 3)
    max_delay = min(base_max * multiplier, 5)
    return random.uniform(min_delay, max_delay)


def long_break_check(request_count, break_every=200, break_duration=30):
    """Длинная пауза каждые N запросов - редко и коротко"""
    if request_count > 0 and request_count % break_every == 0:
        print(f"\n    ⏸️  ДЛИННАЯ ПАУЗА после {request_count} запросов")
        print(f"    ⏸️  Ждём {break_duration} секунд...")
        time.sleep(break_duration)
        return True
    return False


def save_progress(current_index):
    """Сохраняет текущий прогресс"""
    progress_file = os.path.join(SCRIPT_DIR, 'drom_scraped_data_progress.json')

    # Резервная копия
    if os.path.exists(progress_file):
        backup_file = os.path.join(SCRIPT_DIR, 'drom_scraped_data_progress.json.backup')
        try:
            import shutil
            shutil.copy2(progress_file, backup_file)
        except:
            pass

    # Сохраняем прогресс
    progress_data = {
        'last_index': current_index,
        'statistics': statistics,
        'results': results,
        'brand_cache': brand_cache,
        'request_count': request_count
    }

    try:
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"    ❌ ОШИБКА сохранения прогресса: {e}")
        return

    if results:
        excel_data = []
        for result in results:
            if result['total_ads'] == 0:
                excel_data.append({
                    'brand': result['brand'],
                    'model': result['model'],
                    'start_year': result['start_year'],
                    'finish_year': result['finish_year'],
                    'search_url': result['search_url'],
                    'status': result['error'] or 'Нет объявлений',
                    'car_name': '',
                    'year': '',
                    'price': '',
                    'currency': '',
                    'url': '',
                    'mileage': '',
                    'vin': '',
                    'image_url': ''
                })
            else:
                for listing in result['listings']:
                    excel_data.append({
                        'brand': result['brand'],
                        'model': result['model'],
                        'start_year': result['start_year'],
                        'finish_year': result['finish_year'],
                        'search_url': result['search_url'],
                        'status': 'Найдено',
                        'car_name': listing.get('name', ''),
                        'year': listing.get('year', ''),
                        'price': listing.get('price', ''),
                        'currency': listing.get('currency', 'RUB'),
                        'url': listing.get('url', ''),
                        'mileage': listing.get('mileage', ''),
                        'vin': listing.get('vin', ''),
                        'image_url': listing.get('image', '')
                    })

        results_file = os.path.join(SCRIPT_DIR, 'drom_scraped_data_progress.xlsx')
        try:
            pd.DataFrame(excel_data).to_excel(results_file, index=False)
        except Exception as e:
            print(f"    ❌ ОШИБКА сохранения Excel: {e}")
            return

    print("    💾 Прогресс сохранен")


def parse_json_ld_listings(html: str) -> List[Dict[str, Any]]:
    """Парсит JSON-LD объявления из HTML"""
    soup = BeautifulSoup(html, 'html.parser')
    listings = []

    json_ld_scripts = soup.find_all('script', {'type': 'application/ld+json'})

    for script in json_ld_scripts:
        if not script.string:
            continue

        try:
            data = json.loads(script.string)

            if isinstance(data, dict) and data.get('@type') == 'Car':
                # Извлекаем нужные поля
                listing = {
                    'name': data.get('name', ''),
                    'brand': data.get('brand', {}).get('name', ''),
                    'model': data.get('model', ''),
                    'year': data.get('vehicleModelDate', ''),
                    'price': data.get('offers', {}).get('price'),
                    'currency': data.get('offers', {}).get('priceCurrency', 'RUB'),
                    'url': data.get('offers', {}).get('url', ''),
                    'image': data.get('image', {}).get('url', ''),
                    'mileage': data.get('mileageFromOdometer', {}).get('value'),
                    'vin': data.get('vehicleIdentificationNumber', ''),
                }
                listings.append(listing)
        except:
            pass

    return listings


def fetch_brand_listings(brand: str, session, max_pages: int = 3) -> Dict[str, Any]:
    """Получает объявления для бренда"""
    global request_count

    url = f"https://auto.drom.ru/{brand}/"
    all_listings = []
    seen_urls = set()

    print(f"    🔍 Проверка бренда: {url}")

    for page in range(1, max_pages + 1):
        try:
            if page > 1:
                current_url = f"{url}page{page}/"
            else:
                current_url = url

            response = session.get(current_url, headers=get_headers(), timeout=45, stream=True)
            request_count += 1

            if response.status_code == 429:
                print(f"    ⚠️ Код 429, пауза 5 минут...")
                time.sleep(300)
                continue

            if response.status_code != 200:
                print(f"    ⚠ Статус {response.status_code}")
                break

            html = decode_response(response)
            page_listings = parse_json_ld_listings(html)

            if not page_listings:
                if page == 1:
                    print(f"    ⚠ Объявления не найдены")
                break

            # Дедупликация по URL
            new_count = 0
            for listing in page_listings:
                url_key = listing.get('url', '')
                if url_key and url_key not in seen_urls:
                    seen_urls.add(url_key)
                    all_listings.append(listing)
                    new_count += 1

            print(f"    Стр.{page}: найдено {len(page_listings)} (новых: {new_count})")

            if page < max_pages and new_count > 0:
                time.sleep(random.uniform(0.3, 0.5))

        except Exception as e:
            print(f"    ⚠ Ошибка на странице {page}: {e}")
            break

    return {
        'count': len(all_listings),
        'listings': all_listings
    }


def filter_listings_by_model(listings: List[Dict], model: str, start_year: Any = None, finish_year: Any = None) -> List[Dict]:
    """Фильтрует объявления по модели и годам"""
    model_variants = [
        model.lower(),
        model.replace('-', ' ').lower(),
        model.replace('-', '').lower(),
        model.replace(' ', '-').lower(),
    ]

    filtered = []
    for listing in listings:
        # Проверка модели
        name = listing.get('name', '').lower()
        listing_model = listing.get('model', '').lower()

        model_match = any(variant in name or variant in listing_model for variant in model_variants)

        if not model_match:
            continue

        # Проверка годов
        if pd.notna(start_year) and pd.notna(finish_year):
            listing_year = listing.get('year', '')
            if listing_year:
                try:
                    year = int(listing_year)
                    if not (int(start_year) <= year <= int(finish_year)):
                        continue
                except:
                    pass

        filtered.append(listing)

    return filtered


def scrape_brand_model(brand: str, model: str, start_year: Any, finish_year: Any, session) -> Dict[str, Any]:
    """Скрапит данные для конкретной модели"""
    global request_count

    base_url = f"https://auto.drom.ru/{brand}/{model}/"

    if pd.notna(start_year) and pd.notna(finish_year):
        search_url = f"{base_url}?minyear={int(start_year)}&maxyear={int(finish_year)}"
    else:
        search_url = base_url

    result = {
        'brand': brand,
        'model': model,
        'start_year': start_year,
        'finish_year': finish_year,
        'search_url': search_url,
        'total_ads': 0,
        'listings': [],
        'error': None
    }

    page = 1
    consecutive_failures = 0
    seen_urls: Set[str] = set()

    print(f"    🔗 {search_url}")

    while True:  # Парсим пока есть объявления
        try:
            if page > 1:
                if pd.notna(start_year) and pd.notna(finish_year):
                    current_url = f"{base_url}page{page}/?minyear={int(start_year)}&maxyear={int(finish_year)}"
                else:
                    current_url = f"{base_url}page{page}/"
            else:
                current_url = search_url

            response = session.get(current_url, headers=get_headers(), timeout=45, stream=True)
            request_count += 1

            if response.status_code == 429:
                print(f"    ⚠️ Код 429")
                time.sleep(300)
                consecutive_failures += 1
                continue

            if response.status_code != 200:
                result['error'] = f"HTTP {response.status_code}"
                break

            html = decode_response(response)
            page_listings = parse_json_ld_listings(html)

            if not page_listings:
                print(f"    Стр.{page}: объявлений не найдено (конец)")
                break

            new_listings = []
            for listing in page_listings:
                url_key = listing.get('url', '')
                if url_key and url_key not in seen_urls:
                    seen_urls.add(url_key)
                    new_listings.append(listing)

            if not new_listings:
                print(f"    Стр.{page}: все дубликаты (конец)")
                break

            result['listings'].extend(new_listings)
            result['total_ads'] += len(new_listings)

            duplicates = len(page_listings) - len(new_listings)
            if duplicates > 0:
                print(f"    Стр.{page}: {len(new_listings)} новых + {duplicates} дубликатов")
            else:
                print(f"    Стр.{page}: {len(new_listings)} объявлений")

            # ОПТИМИЗАЦИЯ: если на первой странице меньше 20 объявлений, значит это последняя
            if page == 1 and len(new_listings) < 20:
                print(f"    💡 На первой странице меньше 20 объявлений - это последняя страница")
                break

            consecutive_failures = 0
            page += 1

            # Минимальная пауза между страницами
            page_delay = random.uniform(0.3, 0.5)
            time.sleep(page_delay)

        except Exception as e:
            consecutive_failures += 1
            print(f"    ⚠ Ошибка на стр.{page}: {e}")
            if consecutive_failures >= 5:
                result['error'] = str(e)
                break
            time.sleep(random.uniform(10, 20))

    return result


# ========== ОСНОВНОЙ ЦИКЛ ==========

session = create_session()

try:
    current_brand = None
    brand_models_list = []
    skip_to_index = None
    idx = start_index

    while idx < len(df):
        # Пропуск после обработки малого бренда
        if skip_to_index is not None:
            idx = skip_to_index
            skip_to_index = None
            current_brand = None
            continue

        row = df.iloc[idx]

        brand = row['brand']
        model = row['model']
        start_year = row.get('start_year', None)
        finish_year = row.get('finish_year', None)

        long_break_check(request_count, break_every=200, break_duration=30)

        # Смена бренда
        if brand != current_brand:
            current_brand = brand

            brand_models_list = df[df['brand'] == brand].to_dict('records')

            print(f"\n{'=' * 70}")
            print(f"🔍 НОВЫЙ БРЕНД: {brand.upper()}")
            print(f"   Моделей для обработки: {len(brand_models_list)}")
            print('=' * 70)

            if brand not in brand_cache:
                brand_data = fetch_brand_listings(brand, session, max_pages=3)
                brand_cache[brand] = brand_data

                print(f"    📊 Найдено объявлений бренда: {brand_data['count']}")
                time.sleep(random.uniform(0.5, 1))
            else:
                brand_data = brand_cache[brand]
                print(f"    💾 Используем кеш: {brand_data['count']} объявлений")

            # Бренд без объявлений
            if brand_data['count'] == 0:
                print(f"    ⊗ У бренда {brand.upper()} нет объявлений")
                print(f"    ⊗ Пропускаем все {len(brand_models_list)} моделей...")

                for model_row in brand_models_list:
                    results.append({
                        'brand': brand,
                        'model': model_row['model'],
                        'start_year': model_row.get('start_year'),
                        'finish_year': model_row.get('finish_year'),
                        'search_url': f"https://auto.drom.ru/{brand}/{model_row['model']}/",
                        'total_ads': 0,
                        'listings': [],
                        'error': f"Бренд {brand} - нет объявлений"
                    })
                    statistics['no_results'] += 1

                statistics['skipped_brands'] += 1
                last_brand_idx = idx + len(brand_models_list) - 1
                save_progress(last_brand_idx)

                skip_to_index = last_brand_idx + 1
                continue

            # Малый бренд
            elif brand_data['count'] <= SMALL_BRAND_THRESHOLD:
                print(f"    💡 МАЛЫЙ БРЕНД ({brand_data['count']} объявлений)")
                print(f"    💡 Используем фильтрацию из общего списка")

                for model_row in brand_models_list:
                    model_name = model_row['model']
                    model_start = model_row.get('start_year')
                    model_finish = model_row.get('finish_year')

                    print(f"\n    ➜ Модель: {model_name}")

                    filtered = filter_listings_by_model(
                        brand_data['listings'],
                        model_name,
                        model_start,
                        model_finish
                    )

                    if filtered:
                        print(f"      ✓ Найдено: {len(filtered)} объявлений")
                        results.append({
                            'brand': brand,
                            'model': model_name,
                            'start_year': model_start,
                            'finish_year': model_finish,
                            'search_url': f"https://auto.drom.ru/{brand}/{model_name}/",
                            'total_ads': len(filtered),
                            'listings': filtered,
                            'error': None
                        })
                        statistics['successful'] += 1
                        statistics['total_ads_found'] += len(filtered)
                    else:
                        print(f"      ○ Не найдено")
                        results.append({
                            'brand': brand,
                            'model': model_name,
                            'start_year': model_start,
                            'finish_year': model_finish,
                            'search_url': f"https://auto.drom.ru/{brand}/{model_name}/",
                            'total_ads': 0,
                            'listings': [],
                            'error': None
                        })
                        statistics['no_results'] += 1

                last_brand_idx = idx + len(brand_models_list) - 1
                save_progress(last_brand_idx)

                print(f"\n    ✅ Обработано {len(brand_models_list)} моделей бренда {brand.upper()}")

                skip_to_index = last_brand_idx + 1

                if skip_to_index < len(df):
                    delay = adaptive_delay(request_count, base_min=0.5, base_max=1)
                    print(f"    ⏳ Пауза перед новым брендом {delay:.1f}с...")
                    time.sleep(delay)

                continue

            else:
                print(f"    ✓ БОЛЬШОЙ БРЕНД ({brand_data['count']} объявлений)")
                print(f"    ✓ Используем стандартную стратегию")

        # Стандартная обработка
        print(f"\n{'=' * 70}")
        print(f"[{idx + 1}/{len(df)}] {brand.upper()} {model.upper()} ({start_year}-{finish_year})")
        print(f"📊 Выполнено запросов: {request_count}")
        print('=' * 70)

        result = scrape_brand_model(brand, model, start_year, finish_year, session)
        results.append(result)

        if result['error']:
            statistics['failed'] += 1
            statistics['errors'].append({
                'brand': brand,
                'model': model,
                'url': result['search_url'],
                'error': result['error']
            })
            print(f"    ✗ ОШИБКА: {result['error']}")
        elif result['total_ads'] == 0:
            statistics['no_results'] += 1
            print(f"    ○ Объявлений не найдено")
        else:
            statistics['successful'] += 1
            statistics['total_ads_found'] += result['total_ads']
            print(f"    ✓ УСПЕШНО: {result['total_ads']} объявлений")

        save_progress(idx)

        if idx < len(df) - 1:
            delay = adaptive_delay(request_count, base_min=0.5, base_max=1)
            print(f"    ⏳ Адаптивная пауза {delay:.1f}с... (запросов: {request_count})")
            time.sleep(delay)

        idx += 1

except KeyboardInterrupt:
    print("\n\n⚠ ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ")
    if 'idx' in locals():
        save_progress(idx)
except Exception as e:
    print(f"\n\n⚠ КРИТИЧЕСКАЯ ОШИБКА: {e}")
    import traceback
    traceback.print_exc()
    if 'idx' in locals():
        save_progress(idx)
    raise
finally:
    session.close()

if 'idx' in locals():
    save_progress(idx)
else:
    save_progress(len(df) - 1)

print(f"\n{'=' * 70}")
print("ИТОГОВАЯ СТАТИСТИКА")
print('=' * 70)
print(f"Всего обработано строк:    {statistics['total_rows']}")
print(f"Всего запросов:            {request_count}")
print(f"✓ Успешно найдены:         {statistics['successful']}")
print(f"  Всего объявлений:        {statistics['total_ads_found']}")
print(f"○ Без результатов:         {statistics['no_results']}")
print(f"⊗ Пропущено брендов:       {statistics['skipped_brands']}")
print(f"✗ Ошибки:                  {statistics['failed']}")

if statistics['errors']:
    print(f"\n{'-' * 70}")
    print("СПИСОК ОШИБОК:")
    print('-' * 70)
    for error in statistics['errors'][:10]:
        print(f"  • {error['brand']} {error['model']}")
        print(f"    {error['url']}")
        print(f"    Ошибка: {error['error']}")
    if len(statistics['errors']) > 10:
        print(f"  ... и еще {len(statistics['errors']) - 10} ошибок")

print(f"\n{'=' * 70}")
print("Файлы сохранены:")
print("  • drom_scraped_data_progress.json")
print("  • drom_scraped_data_progress.xlsx")
print('=' * 70)