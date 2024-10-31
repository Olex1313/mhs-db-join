import uuid
import random


def gen_uniq_seq(name, n_records, n_extra_cols=0):
    """
    Порождает файл с уникальными ключами в первом поле
    Можно заказать дополнительные колонки
    """
    with open(name, "wt") as f:
        for i in range(n_records):
            if i % 1_000_000 == 0:
                print(i)
            print(uuid.uuid4(), file=f, end="")
            for j in range(n_extra_cols):
                print(f",{uuid.uuid4()}", file=f, end="")
            print(file=f)


def gen_grouped_seq(name, pattern, *, n_extra_cols=0, to_shuffle=False):
    """
    Порождает файл с заданным шаблоном распределением повторяемости ключей в первом поле

    Шаблон - список пар положительных целых

    Первое число - сколько групп записей с заданной численностью хочется  породить
    Второй число - численность

    Проще объяснить на примерах:

    [(1, 1)] - хотим породить одну запись
    [(1, 100)] - хотим породить 100 записей с одним ключом
    [(100, 1)] - хотим породить 100 записей с разными ключами (100 групп записей численностью 1 каждая)
    [(15, 10)] - хотим породить 10 записей с одним ключом, 10 другим - и так 15 раз
    [(1000, 1), (2, 400)] - хотим породить 1000 записей с уникальными ключами, 400 записей с другим и еще 400 с каким-то еще

    Можно заказать дополнительные колонки

    По умолчанию ключи порождаются группами по порядку перебора элементов шаблона

    Если хочется перемешать, можно указать `to_shuffle=True`

    Но такое перемешивание предполагает накопление данных в памяти. И если хочется породить очень большой
    набор и перемешать записи - это не взлетит.

    На такой случай придумана функция `random_merge`. Создайте несколько перемешанных кусков в файлах, а потом смешайте
    """

    def gen():
        num = 0
        for n_keys, n_records in pattern:
            for i1 in range(n_keys):
                body = f"{i1 + num}:{uuid.uuid4()}"
                for i2 in range(n_records):
                    for j in range(n_extra_cols):
                        body += f",{uuid.uuid4()}"
                    yield body
            num += n_keys

    if to_shuffle:
        data = list(gen())
        random.shuffle(data)
        result = data
    else:
        result = gen()

    with open(name, "wt") as f:
        for v in result:
            print(v, file=f)


def random_merge(out_name, *in_names):
    """
    Случайно перемешивает заданные входные файлы.

    known issue: при входных файлах сильно разной длины случайность перемешивания сомнительна
    (если это важно, проще входные файлы подровнять по длине, чем адаптировать код под такую ситуацию)
    """

    fs = [open(fn, "rt") for fn in in_names]
    with open(out_name, "wt") as fout:
        while fs:
            f = random.choice(fs)
            s = f.readline()
            if not s:
                f.close()
                fs.remove(f)
                continue
            print(s, file=fout, end="")
