#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Очистка старой тестовой коллекции..."
docker exec -it nosql-mongos-1 mongosh --eval "db = db.getSiblingDB('university'); db.ycsb_test.drop()" 2>/dev/null

rm -f "$SCRIPT_DIR/results_load.txt"
rm -f "$SCRIPT_DIR/results_run.txt"

YCSB_DIR="$SCRIPT_DIR/YCSB/ycsb-0.17.0"

if [ ! -d "$YCSB_DIR" ]; then
    YCSB_DIR="$HOME/Desktop/NoSQL/YCSB/ycsb-0.17.0"
fi

if [ ! -d "$YCSB_DIR" ]; then
    YCSB_DIR="$SCRIPT_DIR"
fi

if [ ! -d "$YCSB_DIR" ]; then
    echo "Ошибка: Не удалось найти YCSB"
    echo "Пожалуйста, укажите путь к YCSB вручную:"
    echo "cd /путь/к/ycsb-0.17.0 && ./bin/ycsb.sh ..."
    exit 1
fi

echo "Используется YCSB в: $YCSB_DIR"
cd "$YCSB_DIR"

echo "Загрузка данных..."
./bin/ycsb.sh load mongodb -s -P workloads/workloada \
  -p mongodb.url=mongodb://localhost:27017 \
  -p mongodb.database=university \
  -p mongodb.collection=ycsb_test \
  -p recordcount=10000 > "$SCRIPT_DIR/results_load.txt"

echo "Запуск теста..."
./bin/ycsb.sh run mongodb -s -P workloads/workloada \
  -p mongodb.url=mongodb://localhost:27017 \
  -p mongodb.database=university \
  -p mongodb.collection=ycsb_test \
  -p operationcount=5000 \
  -p threadcount=10 > "$SCRIPT_DIR/results_run.txt"

echo "Результаты сохранены в $SCRIPT_DIR/results_run.txt"