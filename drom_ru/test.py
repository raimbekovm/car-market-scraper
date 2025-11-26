import json
from bs4 import BeautifulSoup

# Марки для тестирования
test_brands = [
    ('Abarth', 'abarth'),
    ('AC', 'ac'),
    ('Acura', 'acura')
]

result = {}

# Список возможных кодировок
encodings = ['utf-8', 'cp1251', 'windows-1251', 'iso-8859-1', 'latin-1']

for brand_name, car_name in test_brands:
    try:
        print(f"Парсим {brand_name}...")

        # Пробуем разные кодировки
        html_content = None
        used_encoding = None

        for encoding in encodings:
            try:
                with open(f'{car_name}.html', 'r', encoding=encoding) as file:
                    html_content = file.read()
                used_encoding = encoding
                print(f"  ✓ Файл прочитан с кодировкой: {encoding}")
                break
            except (UnicodeDecodeError, UnicodeError):
                continue

        if not html_content:
            print(f"  ✗ Не удалось прочитать файл ни с одной кодировкой")
            continue

        soup = BeautifulSoup(html_content, 'html.parser')

        # Ищем элемент "Любая модель"
        any_model_div = soup.find('div', string=lambda text: text and 'Любая модель' in text)

        if not any_model_div:
            # Попробуем найти по data-ftid
            any_model_div = soup.find('div', {'data-ftid': 'component_select_reset'})
            print(f"  Искали по data-ftid")

        if not any_model_div:
            print(f"  ✗ Не найден элемент 'Любая модель'")
            # Покажем первые 10 div для отладки
            print(f"  Первые div элементы:")
            for div in soup.find_all('div')[:10]:
                text = div.get_text(strip=True)[:50]
                if text:
                    print(f"    {text}")
            continue

        # Получаем id
        base_id_full = any_model_div.get('id')
        if not base_id_full:
            print(f"  ✗ У элемента 'Любая модель' нет id")
            continue

        # Извлекаем базовую часть
        base_id = base_id_full.rsplit('-', 1)[0]
        print(f"  Base ID: {base_id}")

        # Парсим модели начиная с -2
        models = {}
        counter = 2

        while True:
            model_id = f"{base_id}-{counter}"
            model_div = soup.find('div', {'id': model_id})

            if not model_div:
                break  # Больше нет моделей

            # Извлекаем текст модели
            model_text = model_div.get_text(strip=True)

            if not model_text:
                counter += 1
                continue

            # Убираем количество в скобках
            if '(' in model_text:
                model_name = model_text.split('(')[0].strip()
            else:
                model_name = model_text

            # Создаем slug для модели
            model_slug = model_name.lower().replace(' ', '-')

            models[model_slug] = {
                "model_name": model_name
            }

            counter += 1

        if models:
            result[car_name] = models
            print(f"  ✓ Найдено {len(models)} моделей")
            # Показываем первые 5
            for i, (slug, data) in enumerate(list(models.items())[:5]):
                print(f"    - {slug}: {data['model_name']}")
        else:
            print(f"  ✗ Модели не найдены")

    except FileNotFoundError:
        print(f"  ✗ Файл {car_name}.html не найден. Сохраните страницу вручную!")
    except Exception as e:
        print(f"  ✗ Ошибка: {e}")
        import traceback

        traceback.print_exc()

print("\n" + "=" * 50)
print("РЕЗУЛЬТАТ:")
print("=" * 50)
print(json.dumps(result, ensure_ascii=False, indent=2))

# Сохраняем
with open('cars_test.json', 'w', encoding='utf-8') as file:
    json.dump(result, file, ensure_ascii=False, indent=2)

print(f"\n✓ Результат сохранен в cars_test.json")