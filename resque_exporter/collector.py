import logging
import re

import redis
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

RESQUE_NAMESPACE_PREFIX = 'resque'


METRIC_FAMILY_CLASS_FOR_TYPE = {
    'counter': CounterMetricFamily,
    'gauge': GaugeMetricFamily,
}


class ResqueCollector:
    def __init__(self, redis_url, namespace=None, custom_metrics=None):
        self._r = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        self._r_namespace = namespace
        self.queues = []
        self.workers = []
        self.custom_metrics = []
        if custom_metrics:
            logging.debug("Custom metrics config: %s", custom_metrics)
            for cm in custom_metrics:
                label_regex = cm['label_regex']
                if isinstance(label_regex, str):
                    label_regex = re.compile(cm['label_regex'])
                self.custom_metrics.append({
                    'metric_family_class': METRIC_FAMILY_CLASS_FOR_TYPE[cm['type']],
                    'metric_family_kwargs': {
                        'name': cm['name'],
                        'documentation': cm['documentation'],
                        'labels': label_regex.groupindex.keys(),
                    },
                    'redis_pattern': cm['redis_pattern'],
                    'label_regex': label_regex,
                })

    def _r_key(self, key):
        if self._r_namespace:
            return f"{RESQUE_NAMESPACE_PREFIX}:{self._r_namespace}:{key}"

        return f"{RESQUE_NAMESPACE_PREFIX}:{key}"

    def _remove_r_key_prefix(self, key):
        r_key_prefix = self._r_key("")
        if not key.startswith(r_key_prefix):
            raise ValueError('Cannot remove prefix from key %r' % (key,))
        return key[len(r_key_prefix):]

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
        yield from self.metric_custom_metrics()
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

    def metric_custom_metrics(self):
        for cm in self.custom_metrics:
            label_names = cm['metric_family_kwargs']['labels']
            redis_pattern = self._r_key(cm["redis_pattern"])

            collected_values = {}
            for redis_key in self._r.scan_iter(match=redis_pattern):
                value = int(self._r.get(redis_key))
                key = self._remove_r_key_prefix(redis_key)
                logging.debug('Metric %s=%r', key, value)
                label_match = cm["label_regex"].match(key)
                if label_match is None:
                    continue
                labels_dict = label_match.groupdict()
                labels_tuple = tuple(labels_dict[label] for label in label_names)
                collected_values.setdefault(labels_tuple, 0)
                collected_values[labels_tuple] += value

            if not collected_values:
                continue

            metric = cm['metric_family_class'](**cm['metric_family_kwargs'])

            for label_tuple, value in collected_values.items():
                metric.add_metric(labels=label_tuple, value=value)

            yield metric
