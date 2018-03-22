import os
import unittest

from es2csv_cli.utils.file import purge


class TestFileUtil(unittest.TestCase):
    def test_file_purge(self):
        filename = 'tests/tmp_space/test_file_purge.txt'
        with open(filename, 'a+') as f:
            f.write("test_file_purge")
        purge(filename)
        self.assertFalse(os.path.exists(filename))

    def test_empty_folder_purge(self):
        folder = 'tests/tmp_space/test_empty_folder_purge'
        os.mkdir(folder)
        purge(folder)
        self.assertFalse(os.path.exists(folder))

    def test_non_empty_folder_purge(self):
        folder = 'tests/tmp_space/test_non_empty_folder_purge'
        os.mkdir(folder)
        filename = '{}/test_non_empty_folder_purge.txt'.format(folder)
        with open(filename, 'a+') as f:
            f.write("test_non_empty_folder_purge")
        purge(folder)
        self.assertFalse(os.path.exists(folder))
        self.assertFalse(os.path.exists(filename))
