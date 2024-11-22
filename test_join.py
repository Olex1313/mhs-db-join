import subprocess
import csv

from tempfile import NamedTemporaryFile

left_file_example = [["1", "1", ""], ["2", "6", ""], ["3", "7", ""]]

right_file_example = [["1", "b", ""], ["2", "e", ""], ["3", "h", ""]]


def dump_csv(file, data: list[list[str]]):
    writer = csv.writer(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for row in data:
        writer.writerow(row)
    file.flush()


def test_join_small_files():
    with NamedTemporaryFile(mode="w") as left_file, NamedTemporaryFile(
        mode="w"
    ) as right_file:
        dump_csv(left_file, left_file_example)
        dump_csv(right_file, right_file_example)

        join_run = subprocess.run(
            ["join", left_file.name, "1", right_file.name, "1", "inner"], check=True
        )

        expected_entries = []
        for i in range(len(left_file_example)):
            expected_entries.append(left_file_example[i] + right_file_example[i])

        resulting_entries = join_run.stdout.decode("utf-8").splitlines()
        resulting_entries = [row.split(",") for row in resulting_entries]
        assert resulting_entries == expected_entries
