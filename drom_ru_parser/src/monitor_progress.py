"""
Мониторинг прогресса парсера
"""
import json
import os
import time
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
progress_file = os.path.join(SCRIPT_DIR, 'drom_full_scraper_progress.json')

def format_time(seconds):
    """Форматирует секунды в читаемый вид"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}ч {minutes}м {secs}с"

def get_file_size_mb(filepath):
    """Возвращает размер файла в MB"""
    if os.path.exists(filepath):
        size_bytes = os.path.getsize(filepath)
        return size_bytes / (1024 * 1024)
    return 0

print("=" * 80)
print("МОНИТОРИНГ ПРОГРЕССА ПАРСЕРА")
print("=" * 80)

if not os.path.exists(progress_file):
    print("\n❌ Файл прогресса не найден! Парсер еще не запущен или не сохранил прогресс.")
    exit(1)

# Читаем начальное состояние
with open(progress_file, 'r', encoding='utf-8') as f:
    initial_data = json.load(f)

initial_successful = initial_data.get('successful', 0)
initial_failed = initial_data.get('failed', 0)
initial_index = initial_data.get('last_index', 0)

start_time = time.time()

print(f"\nНачальное состояние:")
print(f"  Последняя строка: {initial_index + 1}")
print(f"  Успешно: {initial_successful:,}")
print(f"  Ошибок: {initial_failed:,}")

print(f"\n{'=' * 80}")
print("Нажмите Ctrl+C для выхода из мониторинга")
print("=" * 80)

try:
    while True:
        time.sleep(5)  # Обновление каждые 5 секунд

        if not os.path.exists(progress_file):
            print("\n⚠️ Файл прогресса исчез! Парсер остановлен?")
            break

        with open(progress_file, 'r', encoding='utf-8') as f:
            current_data = json.load(f)

        current_successful = current_data.get('successful', 0)
        current_failed = current_data.get('failed', 0)
        current_index = current_data.get('last_index', 0)
        current_skipped = current_data.get('skipped', 0)

        # Вычисляем прогресс
        total_processed = current_index - 36578  # Начали с 36578
        new_successful = current_successful - initial_successful
        new_failed = current_failed - initial_failed

        elapsed = time.time() - start_time

        # Вычисляем скорость
        if elapsed > 0:
            speed = new_successful / elapsed
        else:
            speed = 0

        # Оставшееся время
        remaining = 196056 - current_index
        if speed > 0:
            eta_seconds = remaining / speed
            eta = format_time(eta_seconds)
        else:
            eta = "рассчитывается..."

        # Проверяем размеры файлов
        file1_size = get_file_size_mb(os.path.join(SCRIPT_DIR, 'drom_full_scraper_1.xlsx'))
        file2_size = get_file_size_mb(os.path.join(SCRIPT_DIR, 'drom_full_scraper_2.xlsx'))
        file3_size = get_file_size_mb(os.path.join(SCRIPT_DIR, 'drom_full_scraper_3.xlsx'))
        file4_size = get_file_size_mb(os.path.join(SCRIPT_DIR, 'drom_full_scraper_4.xlsx'))

        # Прогресс-бар
        progress_percent = (current_index / 196056) * 100
        bar_length = 50
        filled_length = int(bar_length * current_index // 196056)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)

        # Очищаем экран и выводим статус
        os.system('clear' if os.name == 'posix' else 'cls')

        print("=" * 80)
        print("МОНИТОРИНГ ПРОГРЕССА ПАРСЕРА")
        print("=" * 80)

        print(f"\n📊 ОБЩИЙ ПРОГРЕСС:")
        print(f"  [{bar}] {progress_percent:.2f}%")
        print(f"  Строка: {current_index + 1:,} / 196,056")
        print(f"  Осталось: {remaining:,} строк")

        print(f"\n✅ СТАТИСТИКА:")
        print(f"  Успешно обработано: {current_successful:,} (+{new_successful:,} за сессию)")
        print(f"  Ошибок: {current_failed:,} (+{new_failed:,} за сессию)")
        print(f"  Пропущено: {current_skipped:,}")
        print(f"  Процент успеха: {(current_successful / (current_successful + current_failed) * 100):.1f}%")

        print(f"\n⚡ ПРОИЗВОДИТЕЛЬНОСТЬ:")
        print(f"  Скорость: {speed:.2f} записей/сек")
        print(f"  Время работы: {format_time(elapsed)}")
        print(f"  Оставшееся время: {eta}")

        print(f"\n📁 РАЗМЕРЫ ФАЙЛОВ:")
        if file1_size > 0:
            print(f"  drom_full_scraper_1.xlsx: {file1_size:.2f} MB")
        if file2_size > 0:
            print(f"  drom_full_scraper_2.xlsx: {file2_size:.2f} MB")
        if file3_size > 0:
            print(f"  drom_full_scraper_3.xlsx: {file3_size:.2f} MB")
        if file4_size > 0:
            print(f"  drom_full_scraper_4.xlsx: {file4_size:.2f} MB")

        print(f"\n⏰ Последнее обновление: {datetime.now().strftime('%H:%M:%S')}")
        print(f"\n{'=' * 80}")
        print("Нажмите Ctrl+C для выхода из мониторинга (парсер продолжит работу)")

except KeyboardInterrupt:
    print("\n\n✅ Мониторинг остановлен. Парсер продолжает работу в фоне.")
