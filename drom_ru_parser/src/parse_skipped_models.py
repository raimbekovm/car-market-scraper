"""
Скрипт для обработки пропущенных моделей из skipped_models.xlsx
1. Исправляет search_url
2. Парсит объявления в drom_scraped_data_progress_2.xlsx
3. Парсит детальную информацию в drom_full_scraper_5.xlsx
"""

import pandas as pd
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import random
import re
import time
import os
from typing import Dict, Any, Optional, List, Tuple
import urllib.parse

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

CONCURRENT_REQUESTS = 5
SAVE_BATCH_SIZE = 50


def create_search_url(brand: str, model: str, start_year: float, finish_year: float) -> str:
    """Создает правильный search_url из исправленных brand и model"""
    # Берем brand и model как есть, только приводим к нижнему регистру
    brand_clean = str(brand).lower().strip()
    model_clean = str(model).lower().strip()

    # Формируем URL - используем значения как есть!
    url = f"https://auto.drom.ru/{brand_clean}/{model_clean}/"

    # Добавляем года если они есть
    if pd.notna(start_year) and pd.notna(finish_year):
        start_year_int = int(start_year)
        finish_year_int = int(finish_year)
        url += f"?minyear={start_year_int}&maxyear={finish_year_int}"

    return url


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


async def scrape_listing_page(session: aiohttp.ClientSession, url: str, brand: str, model: str, start_year: float, finish_year: float) -> List[Dict[str, Any]]:
    """Парсит страницу с объявлениями"""
    try:
        async with session.get(url, headers=get_headers(), timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status == 404:
                return []

            if response.status != 200:
                return []

            html = await response.text(encoding='windows-1251')
            soup = BeautifulSoup(html, 'html.parser')

            # Ищем объявления
            listings = []

            # Основной контейнер с объявлениями - теперь это DIV, а не A!
            cards = soup.find_all('div', {'data-ftid': 'bulls-list_bull'})

            for card in cards:
                try:
                    # Ищем ссылку внутри div
                    link_elem = card.find('a', href=True)
                    if not link_elem:
                        continue

                    listing_url = link_elem.get('href', '')
                    if not listing_url.startswith('http'):
                        listing_url = 'https://auto.drom.ru' + listing_url

                    # Название машины - теперь это <a>, а не <span>!
                    title_elem = card.find('a', {'data-ftid': 'bull_title'})
                    car_name = title_elem.get_text(strip=True) if title_elem else ''

                    # Цена
                    price_elem = card.find('span', {'data-ftid': 'bull_price'})
                    price_text = price_elem.get_text(strip=True) if price_elem else ''

                    # Парсим цену
                    price = ''
                    currency = ''
                    if price_text:
                        price_match = re.search(r'([\d\s]+)', price_text.replace('\xa0', ' '))
                        if price_match:
                            price = price_match.group(1).replace(' ', '')

                        if '₽' in price_text:
                            currency = 'RUB'
                        elif '$' in price_text:
                            currency = 'USD'
                        elif '€' in price_text:
                            currency = 'EUR'

                    # Год
                    year_match = re.search(r'(\d{4})', car_name)
                    year = year_match.group(1) if year_match else ''

                    # Пробег
                    mileage_elem = card.find('span', string=re.compile(r'км', re.IGNORECASE))
                    mileage = mileage_elem.get_text(strip=True) if mileage_elem else ''

                    # Фото
                    img_elem = card.find('img', {'data-ftid': 'bull_img'})
                    image_url = img_elem.get('src', '') if img_elem else ''

                    listing = {
                        'brand': brand,
                        'model': model,
                        'start_year': start_year,
                        'finish_year': finish_year,
                        'search_url': url,
                        'status': 'Найдено',
                        'car_name': car_name,
                        'year': year,
                        'price': price,
                        'currency': currency,
                        'url': listing_url,
                        'mileage': mileage,
                        'vin': '',
                        'image_url': image_url
                    }

                    listings.append(listing)

                except Exception as e:
                    continue

            if len(listings) == 0:
                # Если не нашли объявлений
                return [{
                    'brand': brand,
                    'model': model,
                    'start_year': start_year,
                    'finish_year': finish_year,
                    'search_url': url,
                    'status': 'Нет объявлений',
                    'car_name': '',
                    'year': '',
                    'price': '',
                    'currency': '',
                    'url': '',
                    'mileage': '',
                    'vin': '',
                    'image_url': ''
                }]

            return listings

    except Exception as e:
        return [{
            'brand': brand,
            'model': model,
            'start_year': start_year,
            'finish_year': finish_year,
            'search_url': url,
            'status': f'Ошибка: {str(e)[:50]}',
            'car_name': '',
            'year': '',
            'price': '',
            'currency': '',
            'url': '',
            'mileage': '',
            'vin': '',
            'image_url': ''
        }]


async def scrape_listing_details(session: aiohttp.ClientSession, url: str, idx: int) -> Tuple[int, Optional[Dict[str, Any]]]:
    """Парсит детальную информацию об объявлении"""
    try:
        async with session.get(url, headers=get_headers(), timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status != 200:
                return (idx, None)

            html = await response.text(encoding='windows-1251')
            soup = BeautifulSoup(html, 'html.parser')

            # Парсим таблицу характеристик
            specs = {}
            table = soup.find('table', {'class': 'i2nf564', 'data-ftid': 'bulletin-specifications'})

            if table:
                rows = table.find_all('tr')
                for row in rows:
                    property_cell = row.find('th', {'data-ftid': 'property'})
                    value_cell = row.find('td', {'data-ftid': 'value'})

                    if property_cell and value_cell:
                        property_name = property_cell.get_text(strip=True)

                        button = value_cell.find('button')
                        if button:
                            button.decompose()

                        for link in value_cell.find_all('a'):
                            link.replace_with(link.get_text(strip=True))

                        value_text = value_cell.get_text(strip=True)
                        value_text = re.sub(r'\s+', ' ', value_text)
                        specs[property_name] = value_text

            # VIN отчет
            vin_full = ''
            vin_report_items = []

            vin_block = soup.find('div', {'data-ga-stats-name': 'gibdd_report'})
            if vin_block:
                vin_div = vin_block.find('div', class_='css-o8yr01')
                if vin_div:
                    vin_full = vin_div.get_text(strip=True)

                report_items_divs = vin_block.find_all('div', class_=re.compile(r'css-13qo6o5|css-z05wok'))
                for item_div in report_items_divs:
                    button = item_div.find('button')
                    if button:
                        text = button.get_text(strip=True)
                    else:
                        text = item_div.get_text(strip=True)

                    if text and len(text) > 3:
                        vin_report_items.append(text)

            # Описание
            full_description = ''
            exchange_possible = ''
            city_from_description = ''

            desc_block = soup.find('div', {'data-ftid': 'bulletin-description'})
            if desc_block:
                full_desc_div = desc_block.find('div', {'data-ftid': 'info-full'})
                if full_desc_div:
                    value_span = full_desc_div.find('span', {'data-ftid': 'value'})
                    if value_span:
                        for br in value_span.find_all('br'):
                            br.replace_with('\n')

                        description_text = value_span.get_text(strip=False)
                        description_text = re.sub(r'\n\s*\n', '\n', description_text)
                        full_description = description_text.strip()

                trade_div = desc_block.find('div', {'data-ftid': 'trade'})
                if trade_div:
                    value_span = trade_div.find('span', {'data-ftid': 'value'})
                    if value_span:
                        exchange_possible = value_span.get_text(strip=True)

                city_div = desc_block.find('div', {'data-ftid': 'city'})
                if city_div:
                    value_span = city_div.find('span', {'data-ftid': 'value'})
                    if value_span:
                        city_from_description = value_span.get_text(strip=True)

            # Информация об объявлении
            bulletin_id = ''
            bulletin_date = ''
            views_count = ''

            info_block = soup.find('div', {'data-ftid': 'bull-page_bull-views'})
            if info_block:
                bulletin_text_div = info_block.find('div', class_='css-pxeubi')
                if bulletin_text_div:
                    text = bulletin_text_div.get_text(strip=True)
                    match = re.search(r'Объявление\s+(\d+)\s+от\s+([\d.]+)', text)
                    if match:
                        bulletin_id = match.group(1)
                        bulletin_date = match.group(2)

                views_div = info_block.find('div', class_='css-14wh0pm')
                if views_div:
                    views_text = views_div.get_text(strip=True)
                    views_match = re.search(r'(\d+)', views_text)
                    if views_match:
                        views_count = views_match.group(1)

            result = {
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
                'vin_full': vin_full,
                'vin_report_items': ' | '.join(vin_report_items),
                'full_description': full_description,
                'exchange_possible': exchange_possible,
                'city_from_description': city_from_description,
                'bulletin_id': bulletin_id,
                'bulletin_date': bulletin_date,
                'views_count': views_count
            }

            return (idx, result)

    except Exception as e:
        # Логируем ошибку для debugging
        import sys
        print(f"DEBUG: Error parsing {url}: {type(e).__name__}: {str(e)[:100]}", file=sys.stderr)
        return (idx, None)


async def main():
    start_time = time.time()

    print(f"\n{'='*80}")
    print("ПАРСЕР ПРОПУЩЕННЫХ МОДЕЛЕЙ")
    print('='*80)
    print(f"⚡ Параллельных запросов: {CONCURRENT_REQUESTS}")

    # Загружаем skipped_models.xlsx
    input_file = os.path.join(SCRIPT_DIR, 'skipped_models.xlsx')

    if not os.path.exists(input_file):
        print(f"\n❌ ОШИБКА: Файл {input_file} не найден!")
        return

    print(f"\n📂 Загружаем файл: skipped_models.xlsx")
    df_skipped = pd.read_excel(input_file)
    print(f"   Всего моделей: {len(df_skipped):,}")

    # Исправляем search_url
    print(f"\n🔧 Исправляем search_url...")
    df_skipped['search_url'] = df_skipped.apply(
        lambda row: create_search_url(row['brand'], row['model'], row['start_year'], row['finish_year']),
        axis=1
    )

    print("   ✓ URL исправлены")

    # ЭТАП 1: Парсинг списков объявлений
    print(f"\n{'='*80}")
    print("ЭТАП 1: ПАРСИНГ ОБЪЯВЛЕНИЙ")
    print(f"{'='*80}\n")

    all_listings = []
    processed = 0
    found = 0
    not_found = 0
    errors = 0

    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for idx, row in df_skipped.iterrows():
            brand = row['brand']
            model = row['model']
            start_year = row['start_year']
            finish_year = row['finish_year']
            url = row['search_url']

            print(f"[{idx + 1}/{len(df_skipped)}] {brand} {model} - {url}")

            listings = await scrape_listing_page(session, url, brand, model, start_year, finish_year)

            for listing in listings:
                all_listings.append(listing)

                if listing['status'] == 'Найдено':
                    found += 1
                elif listing['status'] == 'Нет объявлений':
                    not_found += 1
                else:
                    errors += 1

            processed += 1

            if len(listings) > 0 and listings[0]['status'] == 'Найдено':
                print(f"   ✓ Найдено {len(listings)} объявлений")
            elif len(listings) > 0 and listings[0]['status'] == 'Нет объявлений':
                print(f"   ○ Нет объявлений")
            else:
                print(f"   ✗ Ошибка")

            await asyncio.sleep(random.uniform(0.5, 1.0))

    # Сохраняем drom_scraped_data_progress_2.xlsx
    progress_2_file = os.path.join(SCRIPT_DIR, 'drom_scraped_data_progress_2.xlsx')
    df_progress_2 = pd.DataFrame(all_listings)
    df_progress_2.to_excel(progress_2_file, index=False)

    print(f"\n{'='*80}")
    print("ЭТАП 1 ЗАВЕРШЕН")
    print(f"{'='*80}")
    print(f"Обработано моделей: {processed}")
    print(f"✓ Найдено объявлений: {found}")
    print(f"○ Нет объявлений: {not_found}")
    print(f"✗ Ошибок: {errors}")
    print(f"\n💾 Сохранено в: {progress_2_file}")
    print(f"   Всего строк: {len(df_progress_2):,}")

    # ЭТАП 2: Парсинг детальной информации
    print(f"\n{'='*80}")
    print("ЭТАП 2: ПАРСИНГ ДЕТАЛЬНОЙ ИНФОРМАЦИИ")
    print(f"{'='*80}\n")

    # Берем только объявления со статусом "Найдено"
    df_to_parse = df_progress_2[df_progress_2['status'] == 'Найдено'].copy()
    print(f"Объявлений для детального парсинга: {len(df_to_parse):,}")

    if len(df_to_parse) == 0:
        print("\n⚠️  Нет объявлений для детального парсинга")
        return

    # Добавляем колонки для детальной информации
    detail_columns = [
        'engine', 'power', 'transmission', 'drive', 'body_type', 'color',
        'mileage_detail', 'owners', 'wheel', 'generation', 'complectation',
        'vin_full', 'vin_report_items', 'full_description', 'exchange_possible',
        'city_from_description', 'bulletin_id', 'bulletin_date', 'views_count'
    ]

    for col in detail_columns:
        if col not in df_to_parse.columns:
            df_to_parse[col] = ''

    successful = 0
    failed = 0

    # Создаем connector и timeout для Stage 2
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        batch = []

        for idx in df_to_parse.index:
            url = df_to_parse.at[idx, 'url']
            if not url:
                continue

            batch.append((idx, url))

            if len(batch) >= CONCURRENT_REQUESTS:
                # Обрабатываем батч
                tasks = [scrape_listing_details(session, url, i) for i, url in batch]
                results = await asyncio.gather(*tasks)

                for result_idx, details in results:
                    if details:
                        for key, value in details.items():
                            df_to_parse.at[result_idx, key] = value
                        successful += 1

                        car_name = df_to_parse.at[result_idx, 'car_name']
                        vin = details.get('vin_full', '')[:8] if details.get('vin_full') else 'N/A'
                        print(f"[{successful + failed}/{len(df_to_parse)}] ✓ {car_name} | VIN: {vin}...")
                    else:
                        failed += 1
                        car_name = df_to_parse.at[result_idx, 'car_name']
                        print(f"[{successful + failed}/{len(df_to_parse)}] ✗ {car_name}")

                batch = []
                await asyncio.sleep(random.uniform(0.5, 1.0))

        # Обрабатываем остаток
        if batch:
            tasks = [scrape_listing_details(session, url, i) for i, url in batch]
            results = await asyncio.gather(*tasks)

            for result_idx, details in results:
                if details:
                    for key, value in details.items():
                        df_to_parse.at[result_idx, key] = value
                    successful += 1

                    car_name = df_to_parse.at[result_idx, 'car_name']
                    vin = details.get('vin_full', '')[:8] if details.get('vin_full') else 'N/A'
                    print(f"[{successful + failed}/{len(df_to_parse)}] ✓ {car_name} | VIN: {vin}...")
                else:
                    failed += 1
                    car_name = df_to_parse.at[result_idx, 'car_name']
                    print(f"[{successful + failed}/{len(df_to_parse)}] ✗ {car_name}")

    # Сохраняем drom_full_scraper_5.xlsx
    scraper_5_file = os.path.join(SCRIPT_DIR, 'drom_full_scraper_5.xlsx')
    df_to_parse.to_excel(scraper_5_file, index=False)

    elapsed_time = time.time() - start_time
    elapsed_hours = elapsed_time / 3600

    print(f"\n{'='*80}")
    print("ЭТАП 2 ЗАВЕРШЕН")
    print(f"{'='*80}")
    print(f"✓ Успешно обработано: {successful:,}")
    print(f"✗ Ошибок: {failed:,}")
    print(f"Success Rate: {(successful / (successful + failed) * 100) if (successful + failed) > 0 else 0:.1f}%")
    print(f"\n💾 Сохранено в: {scraper_5_file}")
    print(f"   Всего строк: {len(df_to_parse):,}")
    print(f"\n⏱️  Общее время: {elapsed_hours:.2f} часов")
    print('='*80)


if __name__ == '__main__':
    asyncio.run(main())
