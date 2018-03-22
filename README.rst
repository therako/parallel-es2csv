A command line interface for exporting data from elasticsearch to local csv files
=================================================================================

.. image:: https://travis-ci.org/therako/parallel-es2csv.svg?branch=master
   :target: https://travis-ci.org/therako/parallel-es2csv
   :alt: Build Status

.. image:: https://img.shields.io/pypi/v/parallel-es2csv.svg
   :target: https://pypi.python.org/pypi/parallel-es2csv/
   :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/parallel-es2csv.svg
   :target: https://pypi.python.org/pypi/parallel-es2csv/
   :alt: Python versions

.. image:: https://img.shields.io/pypi/status/parallel-es2csv.svg
   :target: https://pypi.python.org/pypi/parallel-es2csv/
   :alt: Package status

.. image:: https://coveralls.io/repos/github/therako/parallel-es2csv/badge.svg?branch=master
   :target: https://coveralls.io/github/therako/parallel-es2csv?branch=master
   :alt: Package coverage



Installation and usage
----------------------

**Installation**
Since this is a Python package available on PyPi you can install it like 
any other Python package.

.. code-block:: shell

    # on modern systems with Python you can install with pip
    $ pip install parallel-es2csv
    # on older systems you can install using easy_install
    $ easy_install parallel-es2csv

**Usage**
The commands should be mostly self-documenting in how they are defined,
which is made available through the ``help`` command.

.. code-block:: shell

    $ parallel-es2csv
    usage: parallel-es2csv -u <elasticsearch_url> -i <[list_of_index]> [-n <no_of_workers>] [-o <output_folder>]

