# db-homework-join

[Задание](./TASK.md)


Реализация на C++, для сборки CMake, также в репозитории приложено небольшое количество юнит-тестов на python - [test_join.py](./test_join.py).

### Как собрать утилиту

```bash
cmake . && make

./join ...
```

### Как запускать
```bash
./join data1.csv 3 data2.csv 2 left
```

Так как код ничего не знает о входных данных (распределение, статистику запросов, etc) - выбор целевого алгоритма построен достаточно просто и опирается на размер входных файлов

При желании можно задать целевой алгоритм работы join'а, например:

```bash
bash -c "time  ./join trim_taxi_data.csv 2 trim_taxi_data.csv 2 inner hash > res.csv"
```

Возможные варианты: `nested, hash, sort-merge`


### Как запускать юнит-тесты

Перед запуском необходимо собрать утилиту, стоит учитывать что тесты проверяют корректность работы, поэтому тестовые данные достаточно малы

```bash
python3 -m venv env && source ./env/bin/activate # опционально

pip install -r requirements.txt
pytest
```