import re

from .utils.async_worker import AsyncWorker, ASYNC_WORKER_SATE
from .utils.file import purge
from .export import scroll_and_extract_data


class Es2CsvJob:
    def __init__(self, opts):
        self.opts = opts

    def export(self):
        self._init_async_worker()
        self._clean_output_files()
        self._slice_and_scroll()
        self._wait_till_complete()

    def _init_async_worker(self):
        self.a = AsyncWorker(self.opts.no_of_workers)

    def _clean_output_files(self):
        purge(self.opts.output_file)

    def _slice_and_scroll(self):
        for i in range(self.opts.no_of_workers):
            output_file = self._output_file_for(i)
            self.a.send_data_to_worker(
                scroll_and_extract_data,
                scroll_id=i,
                total_worker_count=self.opts.no_of_workers,
                es_hosts=self.opts.url,
                es_index=self.opts.index,
                es_timeout=self.opts.timeout,
                es_scroll_batch_size=self.opts.scroll_size,
                es_columns=self.opts.fields,
                output_file=output_file
            )

    def _output_file_for(self, worker_no):
        if self.opts.no_of_workers > 1:
            without_extension = re.sub(r'\.csv', '', self.opts.output_file)
            return '{}_{}.csv'.format(without_extension, worker_no)
        return self.opts.output_file

    def _wait_till_complete(self):
        print('Waiting for all jobs to be done...')
        results = self.a.get_job_results()
        print('Jobs are done. checking results...')
        error_results = [x for x in results if x == ASYNC_WORKER_SATE.ERROR]
        if len(error_results) != 0:
            raise Exception('{} no of extract_jobs failed'.format(
                len(error_results)))
