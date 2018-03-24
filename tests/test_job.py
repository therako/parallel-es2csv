import pytest

from mock import patch, MagicMock, call
from es2csv_cli.utils import async_worker
from es2csv_cli.job import Es2CsvJob


@pytest.fixture
def simple_options():
    class SimpleOptions:
        @property
        def no_of_workers(self):
            return 2

        @property
        def url(self):
            return 'es_url'

        @property
        def indices(self):
            return ['index1', 'index2']

        @property
        def timeout(self):
            return 10

        @property
        def output_folder(self):
            return 'output_folder'

        @property
        def scroll_size(self):
            return 100

        @property
        def fields(self):
            return ['field1', 'field2']
    return SimpleOptions()


@pytest.fixture
def single_worker_options():
    class SingleWorkerOptions:
        @property
        def no_of_workers(self):
            return 1

        @property
        def url(self):
            return 'es_url'

        @property
        def indices(self):
            return ['index1', 'index2']

        @property
        def timeout(self):
            return 10

        @property
        def output_folder(self):
            return 'output_folder'

        @property
        def scroll_size(self):
            return 100

        @property
        def fields(self):
            return ['field1', 'field2']
    return SingleWorkerOptions()


def test_init_async_worker(simple_options):
    async_worker_mock = MagicMock(spec=async_worker.AsyncWorker)
    with patch('es2csv_cli.utils.async_worker.AsyncWorker', async_worker_mock):
        Es2CsvJob(simple_options)
        async_worker_mock.assert_called_with(2)


def test__wait_till_complete(simple_options):
    async_worker_mock = MagicMock(spec=async_worker.AsyncWorker)
    with patch('es2csv_cli.utils.async_worker.AsyncWorker', async_worker_mock):
        _es2CsvJob = Es2CsvJob(simple_options)
        _es2CsvJob._wait_till_complete()
        async_worker_mock.get_job_results.assert_called()


def test_get_fieldnames(simple_options):
    export_mock = MagicMock()
    with patch('es2csv_cli.export.get_fieldnames_for', export_mock):
        _es2CsvJob = Es2CsvJob(simple_options)
        _es2CsvJob._get_fieldnames()
        export_mock.assert_called_with('es_url', ['index1', 'index2'])


def test_slice_and_scroll(simple_options):
    async_worker_mock = MagicMock()
    worker_mock = MagicMock()
    async_worker_mock.return_value = worker_mock
    scroll_mock = MagicMock()
    _search_args = {'body': {'query': {
        'match_all': {}}}, 'index': 'index1,index2',
        '_source_include': 'field1,field2', 'scroll': '10s', 'size': 100}
    with patch('es2csv_cli.utils.async_worker.AsyncWorker', async_worker_mock):
        with patch('es2csv_cli.export.scroll_and_extract_data', scroll_mock):
            _es2CsvJob = Es2CsvJob(simple_options)
            fieldnames = 'some_fieldnames'
            _es2CsvJob._slice_and_scroll(fieldnames)
            assert worker_mock.send_data_to_worker.call_count == 2
            assert worker_mock.send_data_to_worker.mock_calls[0] == call(
                scroll_mock,
                search_args=_search_args,
                scroll_id=0, total_worker_count=2, es_timeout=10,
                output_folder='output_folder', es_hosts='es_url',
                fieldnames='some_fieldnames')
            assert worker_mock.send_data_to_worker.mock_calls[1] == call(
                scroll_mock,
                search_args=_search_args,
                scroll_id=1, total_worker_count=2, es_timeout=10,
                output_folder='output_folder', es_hosts='es_url',
                fieldnames='some_fieldnames')


def test_slice_and_scroll_with_single_worker(single_worker_options):
    async_worker_mock = MagicMock()
    worker_mock = MagicMock()
    async_worker_mock.return_value = worker_mock
    scroll_mock = MagicMock()
    _search_args = {'body': {'query': {
        'match_all': {}}}, 'index': 'index1,index2',
        '_source_include': 'field1,field2', 'scroll': '10s', 'size': 100}
    with patch('es2csv_cli.utils.async_worker.AsyncWorker', async_worker_mock):
        with patch('es2csv_cli.export.scroll_and_extract_data', scroll_mock):
            _es2CsvJob = Es2CsvJob(single_worker_options)
            fieldnames = 'some_fieldnames'
            _es2CsvJob._slice_and_scroll(fieldnames)
            assert worker_mock.send_data_to_worker.call_count == 1
            assert worker_mock.send_data_to_worker.mock_calls[0] == call(
                scroll_mock,
                search_args=_search_args,
                scroll_id=0, total_worker_count=1, es_timeout=10,
                output_folder='output_folder', es_hosts='es_url',
                fieldnames='some_fieldnames')


def test_job_execute(simple_options):
    _es2CsvJob = Es2CsvJob(simple_options)
    _es2CsvJob._slice_and_scroll = MagicMock()
    _es2CsvJob._get_fieldnames = MagicMock()
    _es2CsvJob._get_fieldnames.return_value = 'some_fieldnames'
    _es2CsvJob._wait_till_complete = MagicMock()
    _es2CsvJob.execute()
    _es2CsvJob._get_fieldnames.assert_called()
    _es2CsvJob._slice_and_scroll.assert_called_with('some_fieldnames')
    _es2CsvJob._wait_till_complete.assert_called()
