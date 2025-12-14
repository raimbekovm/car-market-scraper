"""
УЛУЧШЕННЫЙ АСИНХРОННЫЙ ПАРСЕР С АВТО-ПОВТОРОМ ОШИБОК
- Сначала добивает до конца основной парсинг
- Потом автоматически повторяет все ошибки
- Максимум 3 попытки на каждую строку
"""

import pandas as pd
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import random
import re
import time
import os
import json
from typing import Dict, Any, Optional, List, Tuple, Set

# Проверяем brotli
try:
    import brotli
    print("✅ Brotli установлен")
except ImportError:
    print("❌ ОШИБКА: Библиотека brotli не установлена!")
    exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
]

# Настройки
CONCURRENT_REQUESTS = 7  # Количество параллельных запросов
SAVE_BATCH_SIZE = 50  # Сохранять каждые N успешных записей
CHUNK_SIZE = 50000  # Размер файла (50,000 записей)
START_INDEX = 36578  # Начинаем с этой записи
MAX_RETRY_ATTEMPTS = 3  # Максимум попыток для каждой ошибки


def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }


def parse_specifications_table(soup: BeautifulSoup) -> Dict[str, str]:
    """Парсит таблицу характеристик"""
    specs = {}
    table = soup.find('table', {'class': 'i2nf564', 'data-ftid': 'bulletin-specifications'})

    if not table:
        return specs

    rows = table.find_all('tr')
    for row in rows:
        property_cell = row.find('th', {'data-ftid': 'property'})
        value_cell = row.find('td', {'data-ftid': 'value'})

        if property_cell and value_cell:
            property_name = property_cell.get_text(strip=True)

            # Удаляем кнопку "налог"
            button = value_cell.find('button')
            if button:
                button.decompose()

            # Заменяем ссылки на текст
            for link in value_cell.find_all('a'):
                link.replace_with(link.get_text(strip=True))

            value_text = value_cell.get_text(strip=True)
            value_text = re.sub(r'\s+', ' ', value_text)

            specs[property_name] = value_text

    return specs


def parse_vin_report(soup: BeautifulSoup) -> Dict[str, Any]:
    """Парсит блок отчета по VIN"""
    vin_info = {
        'vin_full': None,
        'report_items': []
    }

    vin_block = soup.find('div', {'data-ga-stats-name': 'gibdd_report'})

    if not vin_block:
        return vin_info

    # VIN номер
    vin_div = vin_block.find('div', class_='css-o8yr01')
    if vin_div:
        vin_info['vin_full'] = vin_div.get_text(strip=True)

    # Пункты отчета
    report_items_divs = vin_block.find_all('div', class_=re.compile(r'css-13qo6o5|css-z05wok'))

    for item_div in report_items_divs:
        button = item_div.find('button')
        if button:
            text = button.get_text(strip=True)
        else:
            text = item_div.get_text(strip=True)

        if text and len(text) > 3:
            vin_info['report_items'].append(text)

    return vin_info


def parse_description(soup: BeautifulSoup) -> Dict[str, str]:
    """Парсит описание объявления"""
    description_data = {
        'full_description': '',
        'exchange_possible': '',
        'city_from_description': ''
    }

    desc_block = soup.find('div', {'data-ftid': 'bulletin-description'})

    if not desc_block:
        return description_data

    # Полное описание
    full_desc_div = desc_block.find('div', {'data-ftid': 'info-full'})
    if full_desc_div:
        value_span = full_desc_div.find('span', {'data-ftid': 'value'})
        if value_span:
            for br in value_span.find_all('br'):
                br.replace_with('\n')

            description_text = value_span.get_text(strip=False)
            description_text = re.sub(r'\n\s*\n', '\n', description_text)
            description_data['full_description'] = description_text.strip()

    # Обмен
    trade_div = desc_block.find('div', {'data-ftid': 'trade'})
    if trade_div:
        value_span = trade_div.find('span', {'data-ftid': 'value'})
        if value_span:
            description_data['exchange_possible'] = value_span.get_text(strip=True)

    # Город
    city_div = desc_block.find('div', {'data-ftid': 'city'})
    if city_div:
        value_span = city_div.find('span', {'data-ftid': 'value'})
        if value_span:
            description_data['city_from_description'] = value_span.get_text(strip=True)

    return description_data


def parse_bulletin_info(soup: BeautifulSoup) -> Dict[str, str]:
    """Парсит информацию об объявлении"""
    bulletin_info = {
        'bulletin_id': '',
        'bulletin_date': '',
        'views_count': ''
    }

    info_block = soup.find('div', {'data-ftid': 'bull-page_bull-views'})

    if not info_block:
        return bulletin_info

    # Номер и дата
    bulletin_text_div = info_block.find('div', class_='css-pxeubi')
    if bulletin_text_div:
        text = bulletin_text_div.get_text(strip=True)
        match = re.search(r'Объявление\s+(\d+)\s+от\s+([\d.]+)', text)
        if match:
            bulletin_info['bulletin_id'] = match.group(1)
            bulletin_info['bulletin_date'] = match.group(2)

    # Просмотры
    views_div = info_block.find('div', class_='css-14wh0pm')
    if views_div:
        views_text = views_div.get_text(strip=True)
        views_match = re.search(r'(\d+)', views_text)
        if views_match:
            bulletin_info['views_count'] = views_match.group(1)

    return bulletin_info


async def scrape_listing_details(session: aiohttp.ClientSession, url: str, idx: int) -> Tuple[int, Optional[Dict[str, Any]]]:
    """Асинхронно скрапит детальную информацию об объявлении"""
    try:
        async with session.get(url, headers=get_headers(), timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status != 200:
                return (idx, None)

            # aiohttp автоматически декодирует gzip/brotli
            html = await response.text(encoding='windows-1251')
            soup = BeautifulSoup(html, 'html.parser')

            specs = parse_specifications_table(soup)
            vin_report = parse_vin_report(soup)
            description = parse_description(soup)
            bulletin_info = parse_bulletin_info(soup)

            # Объединяем все в плоский словарь для Excel
            result = {
                # Характеристики
                'engine': specs.get('Двигатель', ''),
                'power': specs.get('Мощность', ''),
                'transmission': specs.get('Коробка передач', ''),
                'drive': specs.get('Привод', ''),
                'body_type': specs.get('Тип кузова', ''),
                'color': specs.get('Цвет', ''),
                'mileage_detail': specs.get('Пробег', ''),
                'owners': specs.get('Владельцы', ''),
                'wheel': specs.get('Руль', ''),
                'generation': specs.get('Поколение', ''),
                'complectation': specs.get('Комплектация', ''),

                # VIN отчет
                'vin_full': vin_report.get('vin_full', ''),
                'vin_report_items': ' | '.join(vin_report.get('report_items', [])),

                # Описание
                'full_description': description.get('full_description', ''),
                'exchange_possible': description.get('exchange_possible', ''),
                'city_from_description': description.get('city_from_description', ''),

                # Информация об объявлении
                'bulletin_id': bulletin_info.get('bulletin_id', ''),
                'bulletin_date': bulletin_info.get('bulletin_date', ''),
                'views_count': bulletin_info.get('views_count', '')
            }

            return (idx, result)

    except Exception as e:
        return (idx, None)


def get_file_number(idx: int) -> int:
    """Определяет номер файла по индексу записи"""
    return (idx // CHUNK_SIZE) + 1


def get_file_path(file_number: int) -> str:
    """Возвращает путь к файлу по номеру"""
    return os.path.join(SCRIPT_DIR, f'drom_full_scraper_{file_number}.xlsx')


def load_or_create_chunk_file(file_number: int, source_df: pd.DataFrame) -> pd.DataFrame:
    """Загружает существующий файл или создает новый из исходного DataFrame"""
    file_path = get_file_path(file_number)

    # Определяем диапазон индексов для этого файла
    start_idx = (file_number - 1) * CHUNK_SIZE
    end_idx = min(file_number * CHUNK_SIZE, len(source_df))

    if os.path.exists(file_path):
        try:
            return pd.read_excel(file_path)
        except Exception as e:
            print(f"   ⚠️ Ошибка чтения файла: {e}, создаем новый")

    # Создаем новый файл из нужного диапазона
    chunk_df = source_df.iloc[start_idx:end_idx].copy()

    # Добавляем новые колонки
    new_columns = [
        'engine', 'power', 'transmission', 'drive', 'body_type', 'color',
        'mileage_detail', 'owners', 'wheel', 'generation', 'complectation',
        'vin_full', 'vin_report_items', 'full_description', 'exchange_possible',
        'city_from_description', 'bulletin_id', 'bulletin_date', 'views_count'
    ]

    for col in new_columns:
        if col not in chunk_df.columns:
            chunk_df[col] = ''

    return chunk_df


def load_progress():
    """Загружает прогресс"""
    progress_file = os.path.join(SCRIPT_DIR, 'drom_full_scraper_progress.json')
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return None


def save_progress(last_index, successful, failed, skipped, failed_indices: Set[int], retry_attempt: int = 1):
    """Сохраняет прогресс"""
    progress_file = os.path.join(SCRIPT_DIR, 'drom_full_scraper_progress.json')
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump({
            'last_index': last_index,
            'successful': successful,
            'failed': failed,
            'skipped': skipped,
            'failed_indices': list(failed_indices),
            'retry_attempt': retry_attempt
        }, f, ensure_ascii=False, indent=2)


async def process_batch(session: aiohttp.ClientSession, tasks: List[Tuple[int, str]]) -> List[Tuple[int, Optional[Dict]]]:
    """Обрабатывает батч запросов параллельно"""
    coroutines = [scrape_listing_details(session, url, idx) for idx, url in tasks]
    return await asyncio.gather(*coroutines)


async def main():
    start_time = time.time()

    print(f"\n{'=' * 80}")
    print("УЛУЧШЕННЫЙ ПАРСЕР С АВТО-ПОВТОРОМ ОШИБОК")
    print('=' * 80)
    print(f"⚡ Параллельных запросов: {CONCURRENT_REQUESTS}")
    print(f"📦 Размер файла: {CHUNK_SIZE:,} записей")
    print(f"💾 Сохранение каждые: {SAVE_BATCH_SIZE} успешных записей")
    print(f"🔄 Макс. попыток для ошибок: {MAX_RETRY_ATTEMPTS}")

    # Загружаем исходный файл
    input_file = os.path.join(SCRIPT_DIR, 'drom_scraped_data_progress.xlsx')

    if not os.path.exists(input_file):
        print(f"\n❌ ОШИБКА: Файл {input_file} не найден!")
        return

    print(f"\n📂 Загружаем исходный файл: drom_scraped_data_progress.xlsx")
    source_df = pd.read_excel(input_file)
    print(f"   Всего строк: {len(source_df):,}")

    # Подсчитываем сколько нужно обработать
    rows_to_process = source_df[source_df['status'] == 'Найдено']
    print(f"   Со статусом 'Найдено': {len(rows_to_process):,}")

    # Загружаем прогресс
    progress_data = load_progress()

    if progress_data:
        start_index = max(START_INDEX, progress_data.get('last_index', -1) + 1)
        successful = progress_data.get('successful', 0)
        failed = progress_data.get('failed', 0)
        skipped = progress_data.get('skipped', 0)
        failed_indices = set(progress_data.get('failed_indices', []))
        retry_attempt = progress_data.get('retry_attempt', 1)
        print(f"\n🔄 ПРОДОЛЖАЕМ С СТРОКИ {start_index:,}")
        print(f"   Ошибок в базе: {len(failed_indices):,}")
        print(f"   Попытка: {retry_attempt}")
    else:
        start_index = START_INDEX
        successful = 0
        failed = 0
        skipped = 0
        failed_indices = set()
        retry_attempt = 1
        print(f"\n🆕 НАЧИНАЕМ СО СТРОКИ {start_index:,}")

    print(f"{'=' * 80}\n")

    # Кэш загруженных файлов
    current_chunk_number = None
    current_chunk_df = None

    # Асинхронная сессия
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            # ===== ЭТАП 1: ОСНОВНОЙ ПАРСИНГ ДО КОНЦА =====
            print(f"{'='*80}")
            print("ЭТАП 1: ОСНОВНОЙ ПАРСИНГ ДО КОНЦА")
            print(f"{'='*80}\n")

            batch_tasks = []
            idx = start_index

            while idx < len(source_df):
                row = source_df.iloc[idx]
                status = row.get('status', '')
                url = row.get('url', '')

                # Пропускаем не "Найдено"
                if status != 'Найдено':
                    skipped += 1
                    idx += 1
                    continue

                if not url:
                    skipped += 1
                    idx += 1
                    continue

                # Добавляем в батч
                batch_tasks.append((idx, url))

                # Когда батч заполнен - обрабатываем
                if len(batch_tasks) >= CONCURRENT_REQUESTS:
                    results = await process_batch(session, batch_tasks)

                    # Обрабатываем результаты
                    for result_idx, details in results:
                        # Определяем файл для этой записи
                        file_number = get_file_number(result_idx)

                        # Загружаем chunk если нужно
                        if file_number != current_chunk_number:
                            # Сохраняем предыдущий chunk
                            if current_chunk_df is not None and current_chunk_number is not None:
                                file_path = get_file_path(current_chunk_number)
                                current_chunk_df.to_excel(file_path, index=False)

                            # Загружаем новый chunk
                            current_chunk_number = file_number
                            current_chunk_df = load_or_create_chunk_file(file_number, source_df)

                        # Записываем результат
                        chunk_idx = result_idx % CHUNK_SIZE

                        if details:
                            for key, value in details.items():
                                current_chunk_df.at[chunk_idx, key] = value
                            successful += 1

                            # Убираем из списка ошибок если была там
                            if result_idx in failed_indices:
                                failed_indices.remove(result_idx)
                                failed -= 1

                            # Улучшенный вывод прогресса
                            total_processed = successful + failed
                            progress_pct = (result_idx / len(source_df)) * 100
                            success_rate = (successful / total_processed * 100) if total_processed > 0 else 0

                            car_name = source_df.iloc[result_idx].get('car_name', 'N/A')
                            vin = details.get('vin_full', 'N/A')[:8] if details.get('vin_full') else 'N/A'
                            views = details.get('views_count', 'N/A')

                            print(f"[{result_idx + 1:,}/{len(source_df):,}] ({progress_pct:.1f}%) ✓ {car_name} | VIN: {vin}... | Просмотры: {views} | Успешно: {successful:,} | Ошибок: {failed:,} | Success Rate: {success_rate:.1f}%")
                        else:
                            failed += 1
                            failed_indices.add(result_idx)

                            total_processed = successful + failed
                            progress_pct = (result_idx / len(source_df)) * 100
                            success_rate = (successful / total_processed * 100) if total_processed > 0 else 0

                            car_name = source_df.iloc[result_idx].get('car_name', 'N/A')
                            print(f"[{result_idx + 1:,}/{len(source_df):,}] ({progress_pct:.1f}%) ✗ {car_name} | Успешно: {successful:,} | Ошибок: {failed:,} | Success Rate: {success_rate:.1f}%")

                        # Сохраняем прогресс
                        if successful % SAVE_BATCH_SIZE == 0:
                            save_progress(result_idx, successful, failed, skipped, failed_indices, retry_attempt)
                            if current_chunk_df is not None and current_chunk_number is not None:
                                file_path = get_file_path(current_chunk_number)
                                current_chunk_df.to_excel(file_path, index=False)

                                # Расчет ETA
                                elapsed = time.time() - start_time
                                items_per_sec = successful / elapsed if elapsed > 0 else 0
                                remaining_items = len(rows_to_process) - successful
                                eta_seconds = remaining_items / items_per_sec if items_per_sec > 0 else 0
                                eta_hours = eta_seconds / 3600

                                print(f"    💾 Прогресс сохранен | Успешно: {successful:,} | Ошибок: {failed:,} | Скорость: {items_per_sec:.1f} items/sec | ETA: {eta_hours:.1f}ч")

                    batch_tasks = []

                    # Небольшая пауза между батчами
                    await asyncio.sleep(random.uniform(0.3, 0.7))

                idx += 1

            # Обрабатываем оставшиеся задачи
            if batch_tasks:
                results = await process_batch(session, batch_tasks)

                for result_idx, details in results:
                    file_number = get_file_number(result_idx)

                    if file_number != current_chunk_number:
                        if current_chunk_df is not None and current_chunk_number is not None:
                            file_path = get_file_path(current_chunk_number)
                            current_chunk_df.to_excel(file_path, index=False)

                        current_chunk_number = file_number
                        current_chunk_df = load_or_create_chunk_file(file_number, source_df)

                    chunk_idx = result_idx % CHUNK_SIZE

                    if details:
                        for key, value in details.items():
                            current_chunk_df.at[chunk_idx, key] = value
                        successful += 1

                        if result_idx in failed_indices:
                            failed_indices.remove(result_idx)
                            failed -= 1

                        total_processed = successful + failed
                        progress_pct = (result_idx / len(source_df)) * 100
                        success_rate = (successful / total_processed * 100) if total_processed > 0 else 0

                        car_name = source_df.iloc[result_idx].get('car_name', 'N/A')
                        vin = details.get('vin_full', 'N/A')[:8] if details.get('vin_full') else 'N/A'
                        views = details.get('views_count', 'N/A')

                        print(f"[{result_idx + 1:,}/{len(source_df):,}] ({progress_pct:.1f}%) ✓ {car_name} | VIN: {vin}... | Просмотры: {views} | Успешно: {successful:,} | Ошибок: {failed:,} | Success Rate: {success_rate:.1f}%")
                    else:
                        failed += 1
                        failed_indices.add(result_idx)

                        total_processed = successful + failed
                        progress_pct = (result_idx / len(source_df)) * 100
                        success_rate = (successful / total_processed * 100) if total_processed > 0 else 0

                        car_name = source_df.iloc[result_idx].get('car_name', 'N/A')
                        print(f"[{result_idx + 1:,}/{len(source_df):,}] ({progress_pct:.1f}%) ✗ {car_name} | Успешно: {successful:,} | Ошибок: {failed:,} | Success Rate: {success_rate:.1f}%")

            # Сохраняем после основного парсинга
            if current_chunk_df is not None and current_chunk_number is not None:
                file_path = get_file_path(current_chunk_number)
                current_chunk_df.to_excel(file_path, index=False)

            save_progress(len(source_df) - 1, successful, failed, skipped, failed_indices, retry_attempt)

            print(f"\n{'='*80}")
            print("ЭТАП 1 ЗАВЕРШЕН!")
            print(f"{'='*80}")
            print(f"Успешно: {successful:,} | Ошибок: {len(failed_indices):,}")

            # ===== ЭТАП 2: ПОВТОРНАЯ ОБРАБОТКА ОШИБОК =====
            while len(failed_indices) > 0 and retry_attempt < MAX_RETRY_ATTEMPTS:
                retry_attempt += 1

                print(f"\n{'='*80}")
                print(f"ЭТАП 2: ПОВТОРНАЯ ОБРАБОТКА ОШИБОК (Попытка {retry_attempt}/{MAX_RETRY_ATTEMPTS})")
                print(f"{'='*80}")
                print(f"Ошибок для обработки: {len(failed_indices):,}")
                print(f"{'='*80}\n")

                # Преобразуем в список для итерации
                failed_list = sorted(list(failed_indices))
                batch_tasks = []
                processed_count = 0

                for idx in failed_list:
                    row = source_df.iloc[idx]
                    url = row.get('url', '')

                    if not url:
                        continue

                    # Добавляем в батч
                    batch_tasks.append((idx, url))

                    # Когда батч заполнен - обрабатываем
                    if len(batch_tasks) >= CONCURRENT_REQUESTS:
                        results = await process_batch(session, batch_tasks)

                        # Обрабатываем результаты
                        for result_idx, details in results:
                            processed_count += 1

                            file_number = get_file_number(result_idx)

                            if file_number != current_chunk_number:
                                if current_chunk_df is not None and current_chunk_number is not None:
                                    file_path = get_file_path(current_chunk_number)
                                    current_chunk_df.to_excel(file_path, index=False)

                                current_chunk_number = file_number
                                current_chunk_df = load_or_create_chunk_file(file_number, source_df)

                            chunk_idx = result_idx % CHUNK_SIZE

                            if details:
                                for key, value in details.items():
                                    current_chunk_df.at[chunk_idx, key] = value
                                successful += 1
                                failed -= 1
                                failed_indices.remove(result_idx)

                                car_name = source_df.iloc[result_idx].get('car_name', 'N/A')
                                vin = details.get('vin_full', 'N/A')[:8] if details.get('vin_full') else 'N/A'
                                views = details.get('views_count', 'N/A')

                                print(f"[Retry {processed_count:,}/{len(failed_list):,}] ✓ {car_name} | VIN: {vin}... | Осталось ошибок: {len(failed_indices):,}")
                            else:
                                car_name = source_df.iloc[result_idx].get('car_name', 'N/A')
                                print(f"[Retry {processed_count:,}/{len(failed_list):,}] ✗ {car_name} | Осталось ошибок: {len(failed_indices):,}")

                            # Сохраняем прогресс
                            if processed_count % SAVE_BATCH_SIZE == 0:
                                save_progress(result_idx, successful, failed, skipped, failed_indices, retry_attempt)
                                if current_chunk_df is not None and current_chunk_number is not None:
                                    file_path = get_file_path(current_chunk_number)
                                    current_chunk_df.to_excel(file_path, index=False)
                                    print(f"    💾 Прогресс сохранен | Успешно: {successful:,} | Ошибок: {len(failed_indices):,}")

                        batch_tasks = []
                        await asyncio.sleep(random.uniform(0.5, 1.0))  # Немного больше пауза при ретраях

                # Обрабатываем оставшиеся
                if batch_tasks:
                    results = await process_batch(session, batch_tasks)

                    for result_idx, details in results:
                        processed_count += 1

                        file_number = get_file_number(result_idx)

                        if file_number != current_chunk_number:
                            if current_chunk_df is not None and current_chunk_number is not None:
                                file_path = get_file_path(current_chunk_number)
                                current_chunk_df.to_excel(file_path, index=False)

                            current_chunk_number = file_number
                            current_chunk_df = load_or_create_chunk_file(file_number, source_df)

                        chunk_idx = result_idx % CHUNK_SIZE

                        if details:
                            for key, value in details.items():
                                current_chunk_df.at[chunk_idx, key] = value
                            successful += 1
                            failed -= 1
                            failed_indices.remove(result_idx)

                            car_name = source_df.iloc[result_idx].get('car_name', 'N/A')
                            vin = details.get('vin_full', 'N/A')[:8] if details.get('vin_full') else 'N/A'
                            views = details.get('views_count', 'N/A')

                            print(f"[Retry {processed_count:,}/{len(failed_list):,}] ✓ {car_name} | VIN: {vin}... | Осталось ошибок: {len(failed_indices):,}")
                        else:
                            car_name = source_df.iloc[result_idx].get('car_name', 'N/A')
                            print(f"[Retry {processed_count:,}/{len(failed_list):,}] ✗ {car_name} | Осталось ошибок: {len(failed_indices):,}")

                # Сохраняем после каждого прохода по ошибкам
                if current_chunk_df is not None and current_chunk_number is not None:
                    file_path = get_file_path(current_chunk_number)
                    current_chunk_df.to_excel(file_path, index=False)

                save_progress(len(source_df) - 1, successful, failed, skipped, failed_indices, retry_attempt)

                print(f"\n{'='*80}")
                print(f"ПОПЫТКА {retry_attempt} ЗАВЕРШЕНА")
                print(f"{'='*80}")
                print(f"Успешно обработано в этой попытке: {len(failed_list) - len(failed_indices):,}")
                print(f"Осталось ошибок: {len(failed_indices):,}")

        except KeyboardInterrupt:
            print("\n\n⚠ ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ")

        finally:
            # Финальное сохранение
            if current_chunk_df is not None and current_chunk_number is not None:
                file_path = get_file_path(current_chunk_number)
                current_chunk_df.to_excel(file_path, index=False)

            save_progress(len(source_df) - 1, successful, failed, skipped, failed_indices, retry_attempt)

            # Финальная статистика
            elapsed_time = time.time() - start_time
            elapsed_hours = elapsed_time / 3600
            items_per_hour = successful / elapsed_hours if elapsed_hours > 0 else 0

            print(f"\n{'=' * 80}")
            print("ИТОГОВАЯ СТАТИСТИКА")
            print('=' * 80)
            print(f"Всего строк:           {len(source_df):,}")
            print(f"✓ Успешно обработано:  {successful:,}")
            print(f"✗ Финальных ошибок:    {len(failed_indices):,}")
            print(f"○ Пропущено:           {skipped:,}")
            print(f"\n⏱️  Время выполнения:    {elapsed_hours:.2f} часов")
            print(f"⚡ Скорость:             {items_per_hour:.0f} items/час")
            print(f"🔄 Попыток сделано:     {retry_attempt}")
            print(f"\nФайлы сохранены в: {SCRIPT_DIR}")
            print('=' * 80)


if __name__ == '__main__':
    asyncio.run(main())
