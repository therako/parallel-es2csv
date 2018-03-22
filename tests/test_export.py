import mock
import unittest

from es2csv_cli.export import extract_to_csv


class TestFileUtil(unittest.TestCase):

    def test_extract_to_csv(self):
        data = [
            {
                "_score": 1.0,
                "_type": "index1",
                "_id": "1",
                "_source": {
                    "name": "n1",
                    "value": 2
                },
                "_index": "index1"
            }, {
                "_score": 1.0,
                "_type": "index1",
                "_id": "2",
                "_source": {
                    "name": "n2",
                    "value": 2
                },
                "_index": "index1"
            }
        ]
        _fieldnames = {
            'index1': ['name', 'value']
        }
        _output_folder = '_output_folder'
        csv_util = mock.MagicMock()
        with mock.patch('es2csv_cli.utils.csv_util.write_dicts_to_csv',
                        csv_util):
            extract_to_csv(data, write_headers=True, fieldnames=_fieldnames,
                           output_folder=_output_folder, scroll_id=0)
            _expected_rows = [
                {'name': 'n1', 'value': 2}, {'name': 'n2', 'value': 2}]
            _expected_output_folder = _output_folder + '/index1_0.csv'
            csv_util.assert_called_with(
                _expected_rows, _expected_output_folder, write_headers=True,
                fieldnames=_fieldnames['index1'])
