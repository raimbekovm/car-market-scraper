import pandas as pd

# Читаем исходный файл
input_file = '/drom_ru_parser/drom_scraped_data_progress.xlsx'
output_file = '/drom_ru_parser/skipped_models.xlsx'

# Загружаем данные
df = pd.read_excel(input_file)

# Фильтруем строки со статусом HTTP 404
http_404_models = df[df['status'] == 'HTTP 404']

print(f"Всего записей в файле: {len(df)}")
print(f"Найдено моделей с HTTP 404: {len(http_404_models)}")

# Сохраняем в новый файл
http_404_models.to_excel(output_file, index=False)

print(f"\nФайл сохранен: {output_file}")
print("\nПервые несколько записей:")
print(http_404_models.head())
