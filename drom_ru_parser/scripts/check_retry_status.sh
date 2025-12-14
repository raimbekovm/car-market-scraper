#!/bin/bash

echo "================================================================================"
echo "СТАТУС УЛУЧШЕННОГО ПАРСЕРА С АВТО-ПОВТОРОМ ОШИБОК"
echo "================================================================================"
echo ""

# Проверяем запущен ли процесс
if ps aux | grep -v grep | grep "full_parser_with_retry.py" > /dev/null; then
    echo "✅ ПАРСЕР РАБОТАЕТ"
    echo ""
    ps aux | grep -v grep | grep "full_parser_with_retry.py" | awk '{print "   PID: "$2"\n   CPU: "$3"%\n   Memory: "$4"%\n   Время работы: "$10}'
else
    echo "⚠️  Парсер НЕ РАБОТАЕТ (возможно завершен)"
fi

echo ""
echo "================================================================================"
echo "ТЕКУЩИЙ ПРОГРЕСС"
echo "================================================================================"
echo ""

# Читаем файл прогресса
if [ -f "drom_full_scraper_progress.json" ]; then
    python3 << 'EOF'
import json
import os

try:
    with open('drom_full_scraper_progress.json', 'r') as f:
        progress = json.load(f)

    total_rows = 196056
    last_index = progress.get('last_index', 0)
    successful = progress.get('successful', 0)
    failed = progress.get('failed', 0)
    skipped = progress.get('skipped', 0)
    failed_indices = progress.get('failed_indices', [])
    retry_attempt = progress.get('retry_attempt', 1)

    total_processed = successful + failed
    progress_pct = (last_index / total_rows) * 100 if total_rows > 0 else 0
    success_rate = (successful / total_processed * 100) if total_processed > 0 else 0

    print(f"Последняя обработанная строка: {last_index + 1:,} из {total_rows:,}")
    print(f"Прогресс:                      {progress_pct:.1f}%")
    print(f"")
    print(f"✓ Успешно обработано:          {successful:,}")
    print(f"✗ Текущих ошибок:              {len(failed_indices):,}")
    print(f"○ Пропущено:                   {skipped:,}")
    print(f"Success Rate:                  {success_rate:.1f}%")
    print(f"")
    print(f"🔄 Попытка обработки:           {retry_attempt}/3")

    if retry_attempt > 1:
        print(f"")
        print(f"📊 РЕЖИМ ПОВТОРНОЙ ОБРАБОТКИ ОШИБОК АКТИВЕН")
        print(f"   Обрабатывается попытка #{retry_attempt}")
        print(f"   Осталось ошибок для обработки: {len(failed_indices):,}")

except Exception as e:
    print(f"Ошибка чтения прогресса: {e}")
EOF
else
    echo "Файл прогресса не найден"
fi

echo ""
echo "================================================================================"
echo "ПОСЛЕДНИЕ 40 СТРОК ЛОГА"
echo "================================================================================"
echo ""

if [ -f "parser_retry_output.log" ]; then
    tail -40 parser_retry_output.log
else
    echo "Лог файл не найден"
fi

echo ""
echo "================================================================================"
echo "РАЗМЕР ФАЙЛОВ"
echo "================================================================================"
echo ""

ls -lh drom_full_scraper_*.xlsx 2>/dev/null | grep -v backup | grep -v copy

echo ""
echo "================================================================================"
echo "КОМАНДЫ:"
echo "================================================================================"
echo "Полный лог:              cat parser_retry_output.log"
echo "Лог в реальном времени:  tail -f parser_retry_output.log"
echo "Остановить парсер:       kill \$(ps aux | grep 'full_parser_with_retry.py' | grep -v grep | awk '{print \$2}')"
echo "================================================================================"
