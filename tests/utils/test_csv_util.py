import csv
import unittest

from es2csv_cli.utils.csv_util import write_dicts_to_csv
from es2csv_cli.utils.file import purge


def csv_to_json(csv_path):
    csv_file = csv.DictReader(open(csv_path, 'r'))
    json_list = []
    for row in csv_file:
        json_list.append(row)
    return json_list


class TestCsvUtil(unittest.TestCase):
    def test_write_dicts_to_csv(self):
        dicts = [{
            'col1': 'value1',
            'col2': 2,
        }, {
            'col1': 'value3',
            'col2': 4,
        }]
        filename = 'tests/tmp_space/test_write_dicts_to_csv.csv'
        purge(filename)
        write_dicts_to_csv(dicts, filename)
        output_data = csv_to_json(filename)
        purge(filename)
        assert output_data[0]['col1'] == 'value1'
        assert output_data[0]['col2'] == '2'
        assert output_data[1]['col1'] == 'value3'
        assert output_data[1]['col2'] == '4'

    def test_write_dicts_to_csv_newlines(self):
        dicts = [{
            'col1': 'line1\nline2',
            'col2': 2,
        }, {
            'col1': 'value3',
            'col2': 4,
        }]
        filename = 'tests/tmp_space/test_write_dicts_to_csv_newlines.csv'
        purge(filename)
        write_dicts_to_csv(dicts, filename)
        output_data = csv_to_json(filename)
        purge(filename)
        assert output_data[0]['col1'] == 'line1\nline2'
        assert output_data[0]['col2'] == '2'
        assert output_data[1]['col1'] == 'value3'
        assert output_data[1]['col2'] == '4'
