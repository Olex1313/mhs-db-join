import subprocess
import csv

from tempfile import NamedTemporaryFile


def dump_csv(file, data: list[list[str]]):
    writer = csv.writer(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for row in data:
        writer.writerow(row)
    file.flush()


def test_join_small_files():
    left_table = [["1", "1", ""], ["2", "6", ""], ["3", "7", ""]]
    right_table = [["1", "2", "3"], ["2", "6", ""], ["3", "7", ""]]
    expected_set = [
        ["1", "1", "", "1", "2", "3"],
        ["2", "6", "", "2", "6", ""],
        ["3", "7", "", "3", "7", ""],
    ]

    with NamedTemporaryFile(mode="w") as left_file, NamedTemporaryFile(
        mode="w"
    ) as right_file:
        dump_csv(left_file, left_table)
        dump_csv(right_file, right_table)

        join_run = subprocess.run(
            ["./join", left_file.name, "1", right_file.name, "1", "inner"],
            capture_output=True,
        )

        text_res = join_run.stdout.decode("ascii")
        resulting_entries = [row.split(",") for row in text_res.splitlines()]
        assert resulting_entries == expected_set
