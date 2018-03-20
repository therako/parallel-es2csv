#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


def version():
    import es2csv_cli
    return es2csv_cli.__version__


README = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()

setup(
    name='parallel-es2csv',
    author='Arunkumar Ramanathan',
    author_email='rako.aka.arun@gmail.com',
    license='MIT License',
    url='https://github.com/therako/parallel-es2csv',
    description='A CLI client for exporting elasticsearch data to csv',
    long_description=README,
    version=version(),
    packages=find_packages(exclude=('tests',)),
    cmdclass={'test': PyTest},
    scripts=('bin/parallel-es2csv',),
    install_requires=[
        'elasticsearch>=6.0.0,<7.0.0',
        'elasticsearch-dsl>=6.0.0,<7.0.0',
    ],
    tests_require=[
        'pytest',
        'mock==1.0.1'
    ],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Development Status :: 2 - Pre-Alpha',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
)
