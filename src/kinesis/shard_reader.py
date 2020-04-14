# coding: utf-8
from __future__ import absolute_import
import multiprocessing
import time
import logging
import boto3
from botocore.exceptions import ClientError
from .exceptions import RETRY_EXCEPTIONS


log = logging.getLogger(__name__)


class ShardReader(multiprocessing.Process):
    """Read from a specific shard, passing records and errors back through queues"""
    # how long we sleep between calls to get_records
    # this follow these best practices: http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
    # this can be influeced per-reader instance via the sleep_time arg
    DEFAULT_SLEEP_TIME = 1.0

    def __init__(self, shard_id, shard_iter, record_queue, error_queue, boto3_session=None, sleep_time=None):
        self.shard_id = shard_id
        self.shard_iter = shard_iter
        self.record_queue = record_queue
        self.error_queue = error_queue
        self.boto3_session = boto3_session or boto3.Session()
        self.sleep_time = sleep_time or self.DEFAULT_SLEEP_TIME
        self.client = self.boto3_session.client('kinesis')
        self.retries = 0

        self.process = multiprocessing.Process.__init__(self)
        self.exit_event = multiprocessing.Event()

        self.start()

    def run(self):
        self.begin()
        while not self.exit_event.is_set():
            loop_status = self.loop()

            if loop_status is False:
                self.exit_event.set()
            else:
                time.sleep(loop_status or 0.05)

        log.info("Shard reader for %s stopping..", self.shard_id)
        self.end()

    def shutdown(self):
        self.exit_event.set()

    def begin(self):
        """Begin the shard reader main loop"""
        log.info("Shard reader for %s starting", self.shard_id)

    def end(self):
        """End of the main loop"""
        # self.process.terminate()
        # self.process.join(timeout=5)
        log.info("Shard reader for %s stopped.", self.shard_id)

    def loop(self):
        """Each loop iteration - returns a sleep time or False to stop the loop"""
        # by default we will sleep for our sleep_time each loop
        loop_status = self.sleep_time
        log.debug("ShardReader Loop - %s, pid is %d", self.shard_id, self.pid)
        try:
            resp = self.client.get_records(ShardIterator=self.shard_iter)
        except ClientError as exc:
            if exc.response['Error']['Code'] in RETRY_EXCEPTIONS:
                # sleep for 1 second the first loop, 1 second the next, then 2, 4, 6, 8, ..., up to a max of 30 or
                # until we complete a successful get_records call
                loop_status = min((
                    30,
                    (self.retries or 1) * 2
                ))
                log.info("Retrying get_records (#%d %ds): %s", self.retries + 1, loop_status, exc)
            else:
                log.error("Client error occurred while reading: %s", exc)
                loop_status = False
        else:
            if not resp['NextShardIterator']:
                # the shard has been closed
                log.info("Our shard has been closed, exiting")
                return False

            self.shard_iter = resp['NextShardIterator']
            self.record_queue.put((self.shard_id, resp))
            self.retries = 0

        return loop_status
