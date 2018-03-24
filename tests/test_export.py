import tqdm
import mock
import unittest

from _pytest.monkeypatch import MonkeyPatch
from es2csv_cli.export import (
    _extract_to_csv, scroll_and_extract_data, get_fieldnames_for,
    _add_slice_if_needed, _init_progressbar
)


class TestFileUtil(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()

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
        _fieldnames = {'index1': ['name', 'value']}
        _output_folder = '_output_folder'
        _csv_util = mock.MagicMock()
        with mock.patch('es2csv_cli.utils.csv_util.write_dicts_to_csv',
                        _csv_util):
            _extract_to_csv(data, write_headers=True, fieldnames=_fieldnames,
                            output_folder=_output_folder, worker_id=0)
            _expected_rows = [
                {'name': 'n1', 'value': 2}, {'name': 'n2', 'value': 2}]
            _expected_output_folder = _output_folder + '/index1_0.csv'
            _csv_util.assert_called_with(
                _expected_rows, _expected_output_folder, write_headers=True,
                fieldnames=_fieldnames['index1'])

    def test_multiple_indices_extracts(self):
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
                "_type": "index2",
                "_id": "2",
                "_source": {
                    "name": "n2",
                    "value": 2
                },
                "_index": "index2"
            }
        ]
        _fieldnames = {
            'index1': ['name', 'value'], 'index2': ['name', 'value']}
        _output_folder = '_output_folder'
        _csv_util = mock.MagicMock()
        with mock.patch('es2csv_cli.utils.csv_util.write_dicts_to_csv',
                        _csv_util):
            _extract_to_csv(data, write_headers=True, fieldnames=_fieldnames,
                            output_folder=_output_folder, worker_id=0)
            _index1_rows = [{'name': 'n1', 'value': 2}]
            _index2_rows = [{'name': 'n2', 'value': 2}]
            assert _csv_util.mock_calls == [
                mock.call(_index1_rows, _output_folder + '/index1_0.csv',
                          write_headers=True,
                          fieldnames=_fieldnames['index1']),
                mock.call(_index2_rows, _output_folder + '/index2_0.csv',
                          write_headers=True,
                          fieldnames=_fieldnames['index2'])
            ]

    def test_scroll_and_extract_data(self):
        _scroll_id = 0
        _total_worker_count = 2
        _es_hosts = 'es_hosts'
        _timeout = 1
        _search_args = dict(body={})
        _fieldnames = {'index1': ['name', 'value']}
        _output_folder = '_output_folder'
        _data = [
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
        _page = {'hits': {'hits': _data, 'total': 2}, '_scroll_id': 10}
        _es_mock = mock.MagicMock()
        _es_mock.search.return_value = _page
        _empty_page = {'hits': {'hits': None, 'total': 2}, '_scroll_id': 10}
        _es_mock.scroll.side_effect = [_page, _empty_page]
        _es_connect_mock = mock.MagicMock(return_value=_es_mock)
        _csv_util = mock.MagicMock()
        self.monkeypatch.setattr(tqdm, 'tqdm', mock.MagicMock())
        with mock.patch('elasticsearch.Elasticsearch', _es_connect_mock):
            with mock.patch('es2csv_cli.export._extract_to_csv', _csv_util):
                scroll_and_extract_data(
                    _scroll_id, _total_worker_count, _es_hosts, _timeout,
                    _search_args, _fieldnames, _output_folder)
                _es_connect_mock.assert_called_with(_es_hosts)
                _es_mock.search.assert_called_with(
                    body={'slice': {'max': 2, 'id': 0}})
                _es_mock.scroll.assert_called_with(worker_id=10, scroll='1m')

    def test_get_fieldnames_for(self):
        _es_hosts = 'es_hosts'
        _indices = ['index1']
        _mappings = {
            'index1':
                {'mappings': {
                    'index1': {'properties': {'name': 'string'}}}}}
        _es_mock = mock.MagicMock()
        _es_mock.indices.get_mapping.return_value = _mappings
        _es_connect_mock = mock.MagicMock(return_value=_es_mock)
        with mock.patch('elasticsearch.Elasticsearch', _es_connect_mock):
            fieldnames = get_fieldnames_for(_es_hosts, _indices)
            assert fieldnames['index1'] == ['name']

    def test_no_slice_for_single_worker(self):
        _search_args = dict(body={})
        _res = _add_slice_if_needed(1, _search_args, 0)
        assert _res['body'] == {}

    def test_add_slice_config_for_multiple_worker(self):
        _search_args = dict(body={})
        _res = _add_slice_if_needed(2, _search_args, 0)
        assert _res['body'] == {'slice': {'id': 0, 'max': 2}}
        _res = _add_slice_if_needed(2, _search_args, 1)
        assert _res['body'] == {'slice': {'id': 1, 'max': 2}}

    def test_init_progressbar(self):
        _tqdm = mock.MagicMock()
        with mock.patch('tqdm.tqdm', _tqdm):
            _init_progressbar(100, 0)
            _tqdm.assert_called_with(
                position=0, total=100, ascii=True, desc='slice-0')
