import mock
import pytest
import unittest

from elasticsearch import Elasticsearch
from es2csv_cli.export import (
    extract_to_csv, scroll_and_extract_data, get_fieldnames_for
)


@pytest.fixture
def es_mocks():
    _es = mock.MagicMock(spec=Elasticsearch)
    return _es


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
        _fieldnames = {'index1': ['name', 'value']}
        _output_folder = '_output_folder'
        _csv_util = mock.MagicMock()
        with mock.patch('es2csv_cli.utils.csv_util.write_dicts_to_csv',
                        _csv_util):
            extract_to_csv(data, write_headers=True, fieldnames=_fieldnames,
                           output_folder=_output_folder, scroll_id=0)
            _expected_rows = [
                {'name': 'n1', 'value': 2}, {'name': 'n2', 'value': 2}]
            _expected_output_folder = _output_folder + '/index1_0.csv'
            _csv_util.assert_called_with(
                _expected_rows, _expected_output_folder, write_headers=True,
                fieldnames=_fieldnames['index1'])

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
        with mock.patch('elasticsearch.Elasticsearch', _es_connect_mock):
            with mock.patch('es2csv_cli.export.extract_to_csv', _csv_util):
                scroll_and_extract_data(
                    _scroll_id, _total_worker_count, _es_hosts, _timeout,
                    _search_args, _fieldnames, _output_folder,
                    progress_bar=False)
                _es_connect_mock.assert_called_with(_es_hosts)
                _es_mock.search.assert_called_with(
                    body={'slice': {'max': 2, 'id': 0}})
                _es_mock.scroll.assert_called_with(scroll_id=10, scroll='1m')

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
            self.assertEquals(fieldnames['index1'], ['name'])
