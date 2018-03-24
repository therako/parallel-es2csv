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


This project is to just have a simple cli command to export data from ES using the CPU's,
and Elasticsearch's Sliced Scroll Search for fetching large datasets.
It's intended to be used in Data workflow for extracting data out.

The performance seems better when **no_of_workers == no_of_shards_for_the_index**.

Note
----

This is still early in the development and a bit rough around the edges.
Any bug reports, feature suggestions, etc are greatly appreciated. :)


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

    arguments:
      -h, --help            show this help message and exit
      -i INDICES [INDICES ...], --indices INDICES [INDICES ...]
                            ES indices to export.
      -u URL, --url URL     Elasticsearch host URL. Default is
                            http://localhost:9200.
      -a AUTH, --auth AUTH  Elasticsearch basic authentication in the form of
                            username:pwd.
      -D DOC_TYPE [DOC_TYPE ...], --doc_types DOC_TYPE [DOC_TYPE ...]
                            Document type(s).
      -o OUTPUT_FOLDER, --output_folder OUTPUT_FOLDER
                            Output folder path.
      -f FIELDS [FIELDS ...], --fields FIELDS [FIELDS ...]
                            List of selected fields in output. Default is
                            ['_all'].
      -m INTEGER, --max INTEGER
                            Maximum number of results to return. Default is 0.
      -s INTEGER, --scroll_size INTEGER
                            Scroll size for each batch of results. Default is 100.
      -t INTEGER, --timeout INTEGER
                            Timeout in seconds. Default is 60.
      -e, --meta_fields     Add meta-fields in output.
      -n NO_OF_WORKERS, --no_of_workers NO_OF_WORKERS
                            No. or parallel scroll from Elasticsearch, using
                            Multiprocess
      -v, --version         Show version and exit.
      --debug               Debug mode on.
