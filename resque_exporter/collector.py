import logging

import redis
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily

RESQUE_NAMESPACE_PREFIX = 'resque'


class ResqueCollector:
    def __init__(self, redis_url, namespace=None):
        self._r = redis.from_url(redis_url, charset="utf-8", decode_responses=True)
        self._r_namespace = namespace
        self.queues = []
        self.workers = []

    def _r_key(self, key):
        if self._r_namespace:
            return f"{RESQUE_NAMESPACE_PREFIX}:{self._r_namespace}:{key}"

        return f"{RESQUE_NAMESPACE_PREFIX}:{key}"

    def collect(self):
        logging.info("Collecting metrics from redis broker")
        self.queues = self._r.smembers(self._r_key('queues'))
        self.workers = self._r.smembers(self._r_key('workers'))

        yield self.metric_failed_jobs()
        yield self.metric_processed_jobs()
        yield self.metric_queues()
        yield self.metric_jobs_in_queue()
        yield self.metric_workers()
        yield self.metric_working_workers()
        yield self.metric_workers_per_queue()
        logging.info("Finished collecting metrics from redis broker")

    def metric_failed_jobs(self):
        failed_jobs_amount = self._r.get(self._r_key('stat:failed')) or 0
        metric = CounterMetricFamily('resque_failed_jobs', "Total number of failed jobs")
        metric.add_metric([], failed_jobs_amount)
        return metric

    def metric_processed_jobs(self):
        processed_jobs_amount = self._r.get(self._r_key('stat:processed')) or 0
        metric = CounterMetricFamily('resque_processed_jobs', "Total number of processed jobs")
        metric.add_metric([], processed_jobs_amount)
        return metric

    def metric_queues(self):
        metric = GaugeMetricFamily('resque_queues', "Number of queues")
        metric.add_metric([], len(self.queues))
        return metric

    def metric_jobs_in_queue(self):
        metric = GaugeMetricFamily('resque_jobs_in_queue',
                                   "Number of jobs in a queue",
                                   labels=['queue'])
        for queue in self.queues:
            num_jobs = self._r.llen(self._r_key(f'queue:{queue}'))
            metric.add_metric([queue, ], num_jobs)

        num_jobs_in_failed_queue = self._r.llen(self._r_key('failed'))
        metric.add_metric(['failed', ], num_jobs_in_failed_queue)
        return metric

    def metric_workers(self):
        metric = GaugeMetricFamily('resque_workers', "Number of workers")
        metric.add_metric([], len(self.workers))
        return metric

    def metric_working_workers(self):
        num_working_workers = 0
        for worker in self.workers:
            if self._r.exists(self._r_key(f'worker:{worker}')):
                num_working_workers += 1
        metric = GaugeMetricFamily('resque_working_workers', "Number of working workers")
        metric.add_metric([], num_working_workers)
        return metric

    def metric_workers_per_queue(self):
        workers_per_queue = {}

        for worker in self.workers:
            worker_details = worker.split(':')
            worker_queues = worker_details[-1].split(',')

            if '*' in worker_queues:
                worker_queues = self.queues

            for queue in worker_queues:
                workers_per_queue[queue] = workers_per_queue.get(queue, 0) + 1

        metric = GaugeMetricFamily('resque_workers_per_queue',
                                   "Number of workers handling a specific queue",
                                   labels=['queue'])
        for queue, num_of_workers in workers_per_queue.items():
            metric.add_metric([queue, ], num_of_workers)

        return metric
