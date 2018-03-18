import multiprocessing
import time
import logging


class ASYNC_WORKER_SATE(object):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class Consumer(multiprocessing.Process):
    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                logging.info('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            answer = next_task(proc_name)
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return


class Task(object):
    def __init__(self, worker_callback, **args):
        self.worker_callback = worker_callback
        self.args = args

    def __call__(self, proc_name):
        try:
            self.worker_callback(proc_name, **self.args)
            return ASYNC_WORKER_SATE.SUCCESS
        except Exception as e:
            print('Error in comnsumer: {0}, error: {1}'.format(proc_name, e))
            return ASYNC_WORKER_SATE.ERROR

    def __str__(self):
        return str(self.args)


def example_worker_callback(proc_name, data, id):
    '''
        Usage:
            num_workers = 100
            num_jobs = 1000
            a = AsyncWorker(num_workers, num_jobs)
            for i in range(num_jobs):
                a.send_data_to_worker(example_worker_callback,
                    data='some data', id=i)
            a.get_job_results()
    '''
    time.sleep(0.1)     # pretend to take some time to do the work
    if id == 3:
        raise Exception("hello")
    return data


class AsyncWorker(object):
    """
        AsyncWorkwer - Helper class to split workload across multiple process
        Params:
            num_workers - Number of parallel workers to run
            num_jobs - (optional) In case u'r sending more jobs than
                    no. of async workers available set this
                The jobs will be sent to workers in round round
                If set correct this is usefull while getting results back.
        Note: other functions `send_data_to_worker` and `get_job_results`
        TODO: Add support for limiting max no of jobs pending in the Queue
    """
    def __init__(self, num_workers, num_jobs=None):
        self.num_workers = num_workers
        if not num_jobs:
            self.num_jobs = num_workers
        else:
            self.num_jobs = num_jobs
        self._create_consumers()

    def _create_consumers(self):
        self.tasks = multiprocessing.JoinableQueue()
        self.results = multiprocessing.Queue()
        logging.info('Creating %d consumers' % self.num_workers)
        self.consumers = [Consumer(self.tasks, self.results)
                          for i in range(self.num_workers)]
        for w in self.consumers:
            w.start()

    def send_data_to_worker(self, worker_callback, **args):
        """
            This methods is called when u need to enqueue a job to the workers
            Params:
                worker_callback - A callback method for the job
                    See `example_worker_callback` for example
                    Also the callback function has to be thread safe
                        and not a class method
                **args - will be passed as is to the callback along
                    with proc_name

        """
        self.tasks.put(Task(worker_callback, **args))

    def _close_all_workers(self):
        # Add a poison pill for each consumer
        for i in range(self.num_workers):
            self.tasks.put(None)

    def get_job_results(self):
        """
            Sends kill to all workers waits till results are received
                from all jobs
            returns:
                array of results from the worker stating the state for each job
                    type: ASYNC_WORKER_SATE
        """
        self._close_all_workers()
        # Wait for all of the tasks to finish
        self.tasks.join()
        job_returns = []
        while self.num_jobs:
            result = self.results.get()
            job_returns.append(result)
            self.num_jobs -= 1
        return job_returns
