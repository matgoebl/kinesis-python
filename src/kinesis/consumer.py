from __future__ import absolute_import

import logging
import multiprocessing
import time
import dateutil.parser

import boto3
import six.moves.queue
from kinesis.shard_reader import ShardReader as ShardReaderV2

log = logging.getLogger(__name__)


class KinesisCheckPointType(object):
    BY_RECORD = 1
    BY_SEC = 10


class KinesisConsumer(object):
    """Consume from a kinesis stream

    A process is started for each shard we are to consume from.  Each process passes messages back up to the parent,
    which are returned via the main iterator.
    """
    LOCK_DURATION = 30

    def __init__(self, stream_name, boto3_session=None, state=None, reader_sleep_time=None,
                 default_iterator_type='LATEST',
                 start_at_timestamp=None,
                 checkpoint_type=KinesisCheckPointType.BY_RECORD,
                 checkpoint_interval=1):

        self.stream_name = stream_name
        self.error_queue = multiprocessing.Queue()
        self.record_queue = multiprocessing.Queue()

        self.boto3_session = boto3_session or boto3.Session()
        self.kinesis_client = self.boto3_session.client('kinesis')

        self.state = state
        self.checkpoint_type = checkpoint_type
        self.checkpoint_interval = checkpoint_interval

        self.reader_sleep_time = reader_sleep_time
        self.start_at_timestamp = start_at_timestamp

        self.shards = {}
        self.stream_data = None
        self.run = True
        self.default_iterator_type = default_iterator_type

        self.checkpoints = {}

    def state_shard_id(self, shard_id: str) -> str:
        return '_'.join([self.stream_name, shard_id])

    def shutdown_shard_reader(self, shard_id):
        try:
            self.shards[shard_id].shutdown()
            del self.shards[shard_id]
        except KeyError:
            pass

    def setup_shards(self):
        log.debug("Describing stream")
        self.stream_data = self.kinesis_client.describe_stream(StreamName=self.stream_name)
        # XXX TODO: handle StreamStatus -- our stream might not be ready, or might be deleting

        setup_again = False
        for shard_data in self.stream_data['StreamDescription']['Shards']:
            ending_sequence = shard_data.get('SequenceNumberRange', {}).get("EndingSequenceNumber", None)
            if ending_sequence:
                log.info("Shard %s is already closed.", shard_data['ShardId'])
                continue
            # see if we can get a lock on this shard id
            try:
                shard_locked = self.state.lock_shard(self.state_shard_id(shard_data['ShardId']), self.LOCK_DURATION)
            except AttributeError:
                # no self.state
                pass
            else:
                if not shard_locked:
                    # if we currently have a shard reader running we stop it
                    if shard_data['ShardId'] in self.shards:
                        log.warning("We lost our lock on shard %s, stopping shard reader", shard_data['ShardId'])
                        self.shutdown_shard_reader(shard_data['ShardId'])

                    # since we failed to lock the shard we just continue to the next one
                    continue

            # we should try to start a shard reader if the shard id specified isn't in our shards
            if shard_data['ShardId'] not in self.shards:
                log.info("Shard reader for %s does not exist, creating...", shard_data['ShardId'])

                try:
                    iterator_args = self.state.get_iterator_args(self.state_shard_id(shard_data['ShardId']))
                except AttributeError:
                    # no self.state
                    if self.start_at_timestamp is not None:
                        iterator_args = {
                            "ShardIteratorType": 'AT_TIMESTAMP',
                            "Timestamp": dateutil.parser.parse(
                                self.start_at_timestamp
                            )
                        }
                    else:
                        iterator_args = dict(ShardIteratorType=self.default_iterator_type)

                log.info("%s iterator arguments: %s", shard_data['ShardId'], iterator_args)

                # get our initial iterator
                shard_iter = self.kinesis_client.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=shard_data['ShardId'],
                    **iterator_args
                )

                self.shards[shard_data['ShardId']] = ShardReaderV2(
                    shard_data['ShardId'],
                    shard_iter['ShardIterator'],
                    self.record_queue,
                    self.error_queue,
                    boto3_session=self.boto3_session,
                    sleep_time=self.reader_sleep_time,
                )
            else:
                log.debug(
                    "Checking shard reader %s process at pid %d",
                    shard_data['ShardId'],
                    self.shards[shard_data['ShardId']].pid
                )

                if not self.shards[shard_data['ShardId']].is_alive():
                    self.shutdown_shard_reader(shard_data['ShardId'])
                    setup_again = True
                else:
                    log.debug("Shard reader %s alive & well", shard_data['ShardId'])

        if setup_again:
            self.setup_shards()

    def shutdown(self):
        for shard_id in self.shards:
            log.info("Shutting down shard reader for %s", shard_id)
            self.shards[shard_id].shutdown()
            self.shards[shard_id].terminate()

        self.stream_data = None
        self.run = False
        self.shards = {}

    def flush_checkpoint(self, shard_id=None):
        sequence_number = None

        if not shard_id:
            log.info("flush checkpoints '%s': '%s'", shard_id, self.checkpoints[shard_id])
            # noinspection PyBroadException
            try:
                sequence_number = self.checkpoints[shard_id]
                state_shard_id = self.state_shard_id(shard_id)
                self.state.checkpoint(state_shard_id, sequence_number)
            except AttributeError:
                # no self.state
                pass
            except Exception:
                log.exception("Unhandled exception check pointing records from %s at %s",
                              shard_id, sequence_number)
                self.shutdown_shard_reader(shard_id)

            del self.checkpoints[shard_id]

    def __iter__(self):
        try:
            # use lock duration - 1 here since we want to renew our lock before it expires
            lock_duration_check = self.LOCK_DURATION - 1

            while self.run:
                last_setup_check = time.time()
                last_flush_checkpoint_time = time.time()
                self.setup_shards()

                while self.run and (time.time() - last_setup_check) < lock_duration_check:
                    try:
                        shard_id, resp = self.record_queue.get(block=True, timeout=0.25)
                    except six.moves.queue.Empty:
                        pass
                    else:
                        state_shard_id = self.state_shard_id(shard_id)
                        for item in resp['Records']:
                            if not self.run:
                                break

                            # log.debug(item)
                            yield item

                            # add interval check pointing
                            if self.checkpoint_type == KinesisCheckPointType.BY_RECORD:
                                self.state.checkpoint(state_shard_id, item['SequenceNumber'])
                            elif self.checkpoint_type == KinesisCheckPointType.BY_SEC:
                                self.checkpoints[shard_id] = item['SequenceNumber']
                                if (time.time() - last_flush_checkpoint_time) >= self.checkpoint_interval:
                                    self.flush_checkpoint(shard_id)
                                    last_flush_checkpoint_time = time.time()
                            else:
                                # don't use checkpoint.
                                pass

                    shard_id = None
                    try:
                        while True:
                            shard_id = self.error_queue.get_nowait()
                            log.error("Error received from shard reader %s", shard_id)
                            self.shutdown_shard_reader(shard_id)
                    except six.moves.queue.Empty:
                        pass

                    if shard_id is not None:
                        # we encountered an error from a shard reader, break out of the inner loop to setup the shards
                        break

        except KeyboardInterrupt:
            self.run = False
        finally:
            self.shutdown()
