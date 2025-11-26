import json
import requests
import re
import time
from pathlib import Path


def fix_photo_url(url):
    """Исправляет URL фото для получения оригинала"""
    # Убираем gen240_, gen300_ и т.д. префиксы
    url = re.sub(r'/gen\d+x?\d*_', '/', url)
    # Исправляем экранирование
    url = url.replace('\\/', '/')
    return url


def parse_generations(model_url, model_name):
    """Парсит поколения для конкретной модели, возвращая массив"""
    try:
        print(f"  Загружаем {model_name}...")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Referer': 'https://auto.drom.ru/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }

        response = requests.get(model_url, headers=headers, timeout=15)
        response.raise_for_status()

        html = response.text

        # Ищем generationsByModels
        pattern = r'"generationsByModels":\[(.*?)\],"filledParams"'
        match = re.search(pattern, html, re.DOTALL)

        if not match:
            print(f"    ⚠ Поколения не найдены")
            return None

        json_str = '[' + match.group(1) + ']'
        generations_data = json.loads(json_str)

        if not generations_data or 'generations' not in generations_data[0]:
            return None

        result = []  # теперь массив, а не объект

        for gen_group in generations_data[0]['generations']:
            generation_num = gen_group.get('generation')

            for item in gen_group.get('items', []):
                restyling = item.get('restyling', 0)
                year_start = item.get('yearStart')
                year_end = item.get('yearEnd')
                frames = item.get('frames', [])

                # Получаем фото
                photos = item.get('photos', {})
                photo_url = None

                if 'm' in photos and 'x2' in photos['m']:
                    photo_url = fix_photo_url(photos['m']['x2'])
                elif 'm' in photos and 'x1' in photos['m']:
                    photo_url = fix_photo_url(photos['m']['x1'])
                elif 's' in photos and 'x2' in photos['s']:
                    photo_url = fix_photo_url(photos['s']['x2'])
                elif 's' in photos and 'x1' in photos['s']:
                    photo_url = fix_photo_url(photos['s']['x1'])

                # Добавляем объект поколения в список
                result.append({
                    "generation": generation_num,
                    "restyling": restyling,
                    "frames": frames,
                    "year_start": year_start,
                    "year_end": year_end,
                    "photo": photo_url
                })

        print(f"    ✓ Найдено {len(result)} поколений")
        return result

    except requests.exceptions.Timeout:
        print(f"    ✗ Таймаут")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    ✗ Ошибка сети: {e}")
        return None
    except Exception as e:
        print(f"    ✗ Ошибка: {e}")
        return None


def main():
    # Загружаем существующий JSON
    input_file = 'toyota_models.json'
    output_file = 'toyota_models_with_generations.json'

    print("Загружаем toyota_models.json...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    toyota_models = data.get('toyota', {})
    total = len(toyota_models)

    print(f"Найдено {total} моделей Toyota\n")

    # Обрабатываем каждую модель
    for i, (model_key, model_data) in enumerate(toyota_models.items(), 1):
        print(f"[{i}/{total}] {model_data['model_name']}")

        # Парсим поколения
        generations = parse_generations(model_data['url'], model_data['model_name'])

        if generations:
            model_data['generations'] = generations

        # Сохраняем прогресс после каждой модели
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Задержка между запросами (3-5 секунд)
        if i < total:
            delay = 4  # секунды
            print(f"  Ожидание {delay} сек...\n")
            time.sleep(delay)

    print("=" * 60)
    print(f"✓ Готово! Результат сохранен в {output_file}")
    print(f"✓ Обработано моделей: {total}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Прервано пользователем")
        print("✓ Прогресс сохранен в toyota_models_with_generations.json")
    except Exception as e:
        print(f"\n✗ Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()