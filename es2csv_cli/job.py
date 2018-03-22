from .utils.async_worker import AsyncWorker
from .export import scroll_and_extract_data, get_fieldnames_for


class Es2CsvJob:
    def __init__(self, opts):
        self.opts = opts

    def export(self):
        self._init_async_worker()
        self._slice_and_scroll(self._get_fieldnames())
        self._wait_till_complete()

    def _init_async_worker(self):
        self.a = AsyncWorker(self.opts.no_of_workers)

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
                output_folder=self.opts.output_folder,
                search_args=search_args,
                fieldnames=fieldnames
            )

    def _wait_till_complete(self):
        print('Waiting for all jobs to be done...')
        self.a.get_job_results()
