import subprocess
import csv
import pytest

from tempfile import NamedTemporaryFile


def dump_csv(file, data: list[list[str]]):
    writer = csv.writer(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for row in data:
        writer.writerow(row)
    file.flush()


@pytest.mark.parametrize(
    "left_table, left_column, right_table, right_column, join_type, expected_output",
    [
        (
            [["1", "Alice"], ["2", "Bob"], ["3", "Charlie"]],
            1,
            [["1", "25"], ["2", "30"], ["4", "40"]],
            1,
            "inner",
            [["1", "Alice", "1", "25"], ["2", "Bob", "2", "30"]],
        ),
        (
            [["1", "Alice"], ["2", "Bob"], ["3", "Charlie"]],
            1,
            [["1", "25"], ["2", "30"], ["4", "40"]],
            1,
            "left",
            [
                ["1", "Alice", "1", "25"],
                ["2", "Bob", "2", "30"],
                ["3", "Charlie", "", ""],
            ],
        ),
        (
            [["1", "Alice"], ["2", "Bob"]],
            1,
            [["1", "25"], ["2", "30"], ["3", "50"]],
            1,
            "right",
            [["1", "Alice", "1", "25"], ["2", "Bob", "2", "30"], ["", "", "3", "50"]],
        ),
        (
            [["1", "Alice"], ["2", "Bob"]],
            1,
            [["2", "30"], ["3", "40"]],
            1,
            "outer",
            [["1", "Alice", "", ""], ["2", "Bob", "2", "30"], ["", "", "3", "40"]],
        ),
    ],
)
def test_join(
    left_table,
    left_column,
    right_table,
    right_column,
    join_type,
    expected_output,
):

    with NamedTemporaryFile(mode="w") as left_file, NamedTemporaryFile(
        mode="w"
    ) as right_file:
        dump_csv(left_file, left_table)
        dump_csv(right_file, right_table)

        join_run = subprocess.run(
            [
                "./join",
                left_file.name,
                str(left_column),
                right_file.name,
                str(right_column),
                join_type,
            ],
            capture_output=True,
        )

        assert join_run.returncode == 0, f"Error: {join_run.stderr}"
        text_res = (
            join_run.stdout.decode("utf-8").replace("\r\n", "\n").replace("\r", "")
        )
        resulting_entries = [row.split(",") for row in text_res.splitlines()]

        assert (
            resulting_entries == expected_output
        ), f"Unexpected output: {resulting_entries}"
