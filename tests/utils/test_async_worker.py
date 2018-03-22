from time import sleep
import unittest

from es2csv_cli.utils.async_worker import AsyncWorker


def sleep_task(ms):
    sleep(ms / 1000)
    return ms


def raise_exception():
    raise Exception("Test")


class TestAsyncWorker(unittest.TestCase):
    def test_happy_path(self):
        _worker = AsyncWorker(2)
        _worker.send_data_to_worker(sleep_task, ms=100)
        _worker.send_data_to_worker(sleep_task, ms=100)
        _results = _worker.get_job_results()
        self.assertEqual(_results, [100, 100])

    def test_exception_path(self):
        _worker = AsyncWorker(2)
        _worker.send_data_to_worker(sleep_task, ms=100)
        _worker.send_data_to_worker(raise_exception)
        with self.assertRaises(Exception):
            _worker.get_job_results()
