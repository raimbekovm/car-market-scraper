#!/bin/bash

echo "================================================================================"
echo "СТАТУС ПАРСЕРА ПРОПУЩЕННЫХ МОДЕЛЕЙ"
echo "================================================================================"
echo ""

# Проверяем запущен ли процесс
if ps aux | grep -v grep | grep "parse_skipped_models.py" > /dev/null; then
    echo "✅ ПАРСЕР РАБОТАЕТ"
    echo ""
    ps aux | grep -v grep | grep "parse_skipped_models.py" | awk '{print "   PID: "$2"\n   CPU: "$3"%\n   Memory: "$4"%\n   Время работы: "$10}'
else
    echo "⚠️  Парсер НЕ РАБОТАЕТ (возможно завершен)"
fi

echo ""
echo "================================================================================"
echo "ПРОГРЕСС"
echo "================================================================================"
echo ""

if [ -f "parse_skipped_output.log" ]; then
    # Подсчитываем обработанные модели
    total_models=707
    processed=$(grep -c "^\[" parse_skipped_output.log)
    found=$(grep -c "✓ Найдено" parse_skipped_output.log)
    not_found=$(grep -c "○ Нет объявлений" parse_skipped_output.log)
    errors=$(grep -c "✗ Ошибка" parse_skipped_output.log)

    percent=$((processed * 100 / total_models))

    echo "Всего моделей:        $total_models"
    echo "Обработано:           $processed ($percent%)"
    echo "✓ Найдено объявлений: $found"
    echo "○ Нет объявлений:     $not_found"
    echo "✗ Ошибки:             $errors"
else
    echo "Лог файл не найден"
fi

echo ""
echo "================================================================================"
echo "ПОСЛЕДНИЕ 30 СТРОК ЛОГА"
echo "================================================================================"
echo ""

if [ -f "parse_skipped_output.log" ]; then
    tail -30 parse_skipped_output.log
else
    echo "Лог файл не найден"
fi

echo ""
echo "================================================================================"
echo "СОЗДАННЫЕ ФАЙЛЫ"
echo "================================================================================"
echo ""

ls -lh drom_scraped_data_progress_2.xlsx drom_full_scraper_5.xlsx 2>/dev/null || echo "Файлы еще не созданы"

echo ""
echo "================================================================================"
echo "КОМАНДЫ:"
echo "================================================================================"
echo "Полный лог:              cat parse_skipped_output.log"
echo "Лог в реальном времени:  tail -f parse_skipped_output.log"
echo "Остановить парсер:       kill \$(ps aux | grep 'parse_skipped_models.py' | grep -v grep | awk '{print \$2}')"
echo "================================================================================"
