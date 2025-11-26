import requests


def try_all_urls(original_url):
    """Пробуем ВСЕ варианты URL и сохраняем каждый"""

    variants = [
        # Вариант 1: Оригинальный URL с /c/
        ("Original with /c/", original_url),

        # Вариант 2: Без /c/
        ("Without /c/", original_url.replace('/c/photos/', '/photos/')),

        # Вариант 3: Заменить fullsize на big
        ("With /big/", original_url.replace('/fullsize/', '/big/')),

        # Вариант 4: Без /c/ и с /big/
        ("Without /c/ + /big/", original_url.replace('/c/photos/', '/photos/').replace('/fullsize/', '/big/')),

        # Вариант 5: Изменить поддомен на s6
        ("Subdomain s6", original_url.replace('s.auto.drom.ru', 's6.auto.drom.ru')),

        # Вариант 6: s6 без /c/
        ("s6 without /c/", original_url.replace('s.auto.drom.ru', 's6.auto.drom.ru').replace('/c/photos/', '/photos/')),

        # Вариант 7: Использовать photo вместо photos
        ("photo instead of photos", original_url.replace('/photos/', '/photo/')),

        # Вариант 8: Без /c/ с photo
        ("photo without /c/", original_url.replace('/c/photos/', '/photo/')),

        # Вариант 9: original вместо gen240
        ("original prefix", original_url.replace('gen240_', 'original_')),

        # Вариант 10: Без префикса вообще
        ("no prefix", original_url.replace('gen240_', '')),
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Referer': 'https://auto.drom.ru/',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8'
    }

    results = []

    for i, (description, url) in enumerate(variants, 1):
        try:
            print(f"\n{i}. {description}")
            print(f"   URL: {url}")

            response = requests.get(url, headers=headers, timeout=10)

            size = len(response.content)

            # Проверяем, что это не 404 изображение
            if response.status_code == 200 and size > 1000:  # > 1KB
                filename = f'test_image_{i}_{description.replace(" ", "_").replace("/", "")}.jpg'
                with open(filename, 'wb') as f:
                    f.write(response.content)

                print(f"   ✓ УСПЕХ! Размер: {size:,} байт ({size / 1024:.1f} KB)")
                print(f"   ✓ Сохранено как: {filename}")
                results.append((description, url, size, filename))
            else:
                print(f"   ✗ Не подходит (код: {response.status_code}, размер: {size} байт)")

        except Exception as e:
            print(f"   ✗ Ошибка: {e}")

    # Итоги
    print("\n" + "=" * 80)
    print("ИТОГИ:")
    print("=" * 80)

    if results:
        # Сортируем по размеру (больше = лучше)
        results.sort(key=lambda x: x[2], reverse=True)

        for i, (desc, url, size, filename) in enumerate(results, 1):
            print(f"\n{i}. {desc}")
            print(f"   Размер: {size:,} байт ({size / 1024:.1f} KB)")
            print(f"   Файл: {filename}")
            print(f"   URL: {url}")

        print(f"\n✓ Лучшее качество (самый большой файл): {results[0][3]}")
    else:
        print("\n✗ Ни один вариант не сработал")

    return results


# Тестируем
original = "https://s.auto.drom.ru/i24292/c/photos/fullsize/toyota/4runner/gen240_toyota_4runner_1196956.jpg"
results = try_all_urls(original)