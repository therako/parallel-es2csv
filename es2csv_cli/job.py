from .utils.async_worker import AsyncWorker, ASYNC_WORKER_SATE
from .utils.file import purge
from .export import scroll_and_extract_data, get_fieldnames_for


class Es2CsvJob:
    def __init__(self, opts):
        self.opts = opts

    def export(self):
        self._init_async_worker()
        self._clean_output_files()
        self._slice_and_scroll(self._get_fieldnames())
        self._wait_till_complete()

    def _init_async_worker(self):
        self.a = AsyncWorker(self.opts.no_of_workers)

    def _clean_output_files(self):
        purge(self.opts.output_prefix)

    def _get_fieldnames(self):
        return get_fieldnames_for(self.opts.url, self.opts.indices)

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
                scroll_and_extract_data,
                scroll_id=i,
                total_worker_count=self.opts.no_of_workers,
                es_hosts=self.opts.url,
                es_timeout=self.opts.timeout,
                output_prefix=self.opts.output_prefix,
                search_args=search_args,
                fieldnames=fieldnames
            )

    def _wait_till_complete(self):
        print('Waiting for all jobs to be done...')
        results = self.a.get_job_results()
        print('Jobs are done. checking results...')
        error_results = [x for x in results if x == ASYNC_WORKER_SATE.ERROR]
        if len(error_results) != 0:
            raise Exception('{} no of extract_jobs failed'.format(
                len(error_results)))
