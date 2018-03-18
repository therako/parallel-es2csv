import os
from es2csv_cli.utils.csv_util import add_rows


def test_add_rows():
    rows = [['a', 'b'], ['c', 'd']]
    filename = 'test_add_rows.csv'
    add_rows(rows, filename)
    output_data = open(filename).read()
    os.remove(filename)
    assert output_data == '"a","b"\n"c","d"\n'
