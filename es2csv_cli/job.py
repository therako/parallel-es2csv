import argparse
from . import __version__
from .utils import async_worker
from . import export


parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument(
    '-i', '--indices', dest='indices', type=str, required=True, nargs='+',
    help='ES indices to export.')
parser.add_argument(
    '-u', '--url', dest='url', default='http://localhost:9200', type=str,
    help='Elasticsearch host URL. Default is %(default)s.')
parser.add_argument(
    '-a', '--auth', dest='auth', type=str, required=False,
    help='Elasticsearch basic authentication in the form of username:pwd.')
parser.add_argument(
    '-D', '--doc_types', dest='doc_types', type=str, nargs='+',
    metavar='DOC_TYPE', help='Document type(s).')
parser.add_argument(
    '-o', '--output_folder', dest='output_folder', type=str, default='.',
    help='Output folder path.')
parser.add_argument(
    '-f', '--fields', dest='fields', default=['_all'], type=str, nargs='+',
    help='List of selected fields in output. Default is %(default)s.')
parser.add_argument(
    '-m', '--max', dest='max_results', default=0, type=int, metavar='INTEGER',
    help='Maximum number of results to return. Default is %(default)s.')
parser.add_argument(
    '-s', '--scroll_size', dest='scroll_size', default=100, type=int,
    metavar='INTEGER',
    help='Scroll size for each batch of results. Default is %(default)s.')
parser.add_argument(
    '-t', '--timeout', dest='timeout', default=60, type=int,
    metavar='INTEGER', help='Timeout in seconds. Default is %(default)s.')
parser.add_argument(
    '-e', '--meta_fields', dest='meta_fields', action='store_true',
    help='Add meta-fields in output.')
parser.add_argument(
    '-n', '--no_of_workers', dest='no_of_workers', type=int, default=1,
    help='No. or parallel scroll from Elasticsearch, using Multiprocess')
parser.add_argument(
    '-v', '--version', action='version',
    version='%(prog)s ' + __version__,
    help='Show version and exit.')
parser.add_argument(
    '--debug', dest='debug_mode', action='store_true', help='Debug mode on.')


class Es2CsvJob:
    def __init__(self, opts):
        self.opts = opts
        self._init_async_worker()

    def execute(self):
        self._slice_and_scroll(self._get_fieldnames())
        self._wait_till_complete()

    def _init_async_worker(self):
        self.a = async_worker.AsyncWorker(self.opts.no_of_workers)

    def _get_fieldnames(self):
        return export.get_fieldnames_for(self.opts.url, self.opts.indices)

    def _slice_and_scroll(self, fieldnames):
        for i in range(self.opts.no_of_workers):
            search_args = dict(
                index=','.join(self.opts.indices),
                scroll='{}s'.format(self.opts.timeout),
                size=self.opts.scroll_size,
                body={
                    'query': {
                        'match_all': {}
                    }
                }
            )
            if '_all' not in self.opts.fields:
                search_args['_source_include'] = ','.join(self.opts.fields)
            self.a.send_data_to_worker(
                export.scroll_and_extract_data,
                worker_id=i,
                total_worker_count=self.opts.no_of_workers,
                es_hosts=self.opts.url,
                es_timeout=self.opts.timeout,
                output_folder=self.opts.output_folder,
                search_args=search_args,
                fieldnames=fieldnames
            )

    def _wait_till_complete(self):
        print('Waiting for all jobs to be done...')
        self.a.get_job_results()
