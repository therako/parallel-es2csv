from concurrent.futures import ProcessPoolExecutor, wait


class AsyncWorker(object):
    """
        Wrapper for ProcessPoolExectuor
        Params:
            num_workers - Number of parallel workers to run
        Note: other functions `send_data_to_worker` and `get_job_results`
    """
    def __init__(self, num_workers, num_jobs=None):
        self.num_workers = num_workers
        self._executor = ProcessPoolExecutor(max_workers=self.num_workers)
        self._futures = []

    def send_data_to_worker(self, worker_callback, **args):
        """
            Enqueue's a python callable to the async executor
            Params:
                worker_callback - A python callable method
                **args - args for worker_callback

        """
        self._futures.append(self._executor.submit(worker_callback, **args))

    def get_job_results(self):
        """
            Waits and gets results from all jobs
            returns:
                array of return values from each task
            Note: It checks for exceptions inside the tasks and in case of
            any exceptions, they will be printed
            and a final RuntimeError will be raisen.
        """
        _job_results = []
        raise_error = False
        all_futures = wait(self._futures).done
        for x in all_futures:
            try:
                _job_results.append((x.result()))
            except Exception as exc:
                print('A slice ended with Exception: %s' % exc)
                raise_error = True
        if raise_error:
            raise RuntimeError("One or more worker ended in Exception.")
        return _job_results
