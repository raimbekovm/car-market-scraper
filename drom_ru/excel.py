import json
import pandas as pd

# ============================
# 1. Загружаем JSON
# ============================
input_file = "toyota_models_with_generations.json"
output_file = "toyota_models_with_generations.xlsx"

with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

toyota_models = data.get("toyota", {})

rows = []

# ============================
# 2. Преобразуем в плоскую таблицу
# ============================
for model_key, model_data in toyota_models.items():
    model_name = model_data.get("model_name")
    generations = model_data.get("generations", [])

    for gen in generations:
        rows.append({
            "brand": "Toyota",
            "model_key": model_key,
            "model_name": model_name,
            "generation": gen.get("generation"),
            "restyling": gen.get("restyling"),
            "frames": ", ".join(gen.get("frames", [])),
            "year_start": gen.get("year_start"),
            "year_end": gen.get("year_end"),
            "photo": gen.get("photo"),
        })

# ============================
# 3. Формируем DataFrame
# ============================
df = pd.DataFrame(rows)

# Сортировка по модели и поколению
df = df.sort_values(by=["model_name", "generation", "restyling"])

# ============================
# 4. Сохраняем в Excel
# ============================
df.to_excel(output_file, index=False)

print("✓ Файл успешно создан:", output_file)
