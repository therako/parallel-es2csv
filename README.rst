A command line interface for exporting data from elasticsearch to local csv files
=================================================================================

.. image:: https://travis-ci.org/therako/es2csv-cli.svg?branch=master
   :target: https://travis-ci.org/therako/es2csv-cli
   :alt: Build Status

.. image:: https://img.shields.io/pypi/v/es2csv-cli.svg
   :target: https://pypi.python.org/pypi/es2csv-cli/
   :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/es2csv-cli.svg
   :target: https://pypi.python.org/pypi/es2csv-cli/
   :alt: Python versions

.. image:: https://img.shields.io/pypi/status/es2csv-cli.svg
   :target: https://pypi.python.org/pypi/es2csv-cli/
   :alt: Package status


Installation and usage
----------------------

**Installation**
Since this is a Python package available on PyPi you can install it like 
any other Python package.

.. code-block:: shell

    # on modern systems with Python you can install with pip
    $ pip install pyes2csv
    # on older systems you can install using easy_install
    $ easy_install pyes2csv

**Usage**
The commands should be mostly self-documenting in how they are defined,
which is made available through the ``help`` command.

.. code-block:: shell

    $ pyes2csv
    usage: es2csv-cli -u <elasticsearch_url> -i <index_to_export> -o <output_file>

