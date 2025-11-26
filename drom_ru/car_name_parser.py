from bs4 import BeautifulSoup
import json

# Читаем локальный HTML файл
with open('drom.html', 'r', encoding='utf-8') as file:
    html_content = file.read()

# Парсим HTML
soup = BeautifulSoup(html_content, 'html.parser')

# Находим все ссылки
links = soup.find_all('a', href=True)

# Создаем словарь для результата
result = {}

for link in links:
    href = link.get('href')
    # Извлекаем model_name из URL (например, из "https://auto.drom.ru/abarth/" получаем "abarth")
    model_name = href.rstrip('/').split('/')[-1]
    # Извлекаем название марки из текста ссылки
    brand_name = link.get_text(strip=True)

    result[brand_name] = {
        "car_name": model_name
    }

# Сохраняем в JSON
with open('cars.json', 'w', encoding='utf-8') as json_file:
    json.dump(result, json_file, ensure_ascii=False, indent=2)

print(f"Готово! Обработано {len(result)} марок")