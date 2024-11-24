import subprocess
import csv
import pytest

from tempfile import NamedTemporaryFile
from dataclasses import dataclass


def dump_csv(file, data: list[list[str]]):
    writer = csv.writer(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for row in data:
        writer.writerow(row)
    file.flush()


@dataclass
class JoinTestcase:
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str
    expected_out: list[list[str]]


@pytest.mark.parametrize("algorithm", ["nested", "hash"])
@pytest.mark.parametrize(
    "testcase",
    [
        JoinTestcase(
            [["1", "Alice"], ["2", "Bob"], ["3", "Charlie"]],
            "1",
            [["1", "25"], ["2", "30"], ["4", "40"]],
            "1",
            "inner",
            [["1", "Alice", "1", "25"], ["2", "Bob", "2", "30"]],
        ),
        JoinTestcase(
            [["1", "Alice"], ["2", "Bob"], ["3", "Charlie"]],
            "1",
            [["1", "25"], ["2", "30"], ["4", "40"]],
            "1",
            "left",
            [
                ["1", "Alice", "1", "25"],
                ["2", "Bob", "2", "30"],
                ["3", "Charlie", "", ""],
            ],
        ),
        JoinTestcase(
            [["1", "Alice"], ["2", "Bob"]],
            "1",
            [["1", "25"], ["2", "30"], ["3", "50"]],
            "1",
            "right",
            [["1", "Alice", "1", "25"], ["2", "Bob", "2", "30"], ["", "", "3", "50"]],
        ),
        JoinTestcase(
            [["1", "Alice"], ["2", "Bob"]],
            "1",
            [["2", "30"], ["3", "40"]],
            "1",
            "outer",
            [["1", "Alice", "", ""], ["2", "Bob", "2", "30"], ["", "", "3", "40"]],
        ),
    ],
)
def test_join(algorithm: str, testcase: JoinTestcase):

    with NamedTemporaryFile(mode="w") as left_file, NamedTemporaryFile(
        mode="w"
    ) as right_file:
        dump_csv(left_file, testcase.left_table)
        dump_csv(right_file, testcase.right_table)

        join_run = subprocess.run(
            [
                "./join",
                left_file.name,
                testcase.left_column,
                right_file.name,
                testcase.right_column,
                testcase.join_type,
                algorithm,
            ],
            capture_output=True,
        )

        assert join_run.returncode == 0, f"Error: {join_run.stderr}"
        text_res = (
            join_run.stdout.decode("utf-8").replace("\r\n", "\n").replace("\r", "")
        )
        resulting_entries = [row.split(",") for row in text_res.splitlines()]

        assert (
            resulting_entries == testcase.expected_out
        ), f"Unexpected output: {resulting_entries}"
