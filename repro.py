#!/usr/bin/env python3

# -*- coding: utf-8 -*-

# stdlib imports
import asyncio
import contextlib
import enum
import datetime
import math
import logging
import logging.config
import threading
import random
import shutil
import pathlib
import uuid
from typing import (
    Callable,
    Set
)
# non-stdlib imports
import BTrees.check
import BTrees.OOBTree
import transaction
import persistent
import ZODB

# global defines
logger = logging.getLogger('repro')


class States(enum.Enum):
    Uninitialized = 0
    Running = 10
    Completed = 20
    Deleted = 30


def create_zodb() -> ZODB.DB:
    dir = pathlib.Path('db')
    if dir.exists():
        shutil.rmtree(str(dir))
    dir.mkdir(mode=0o700, parents=True)
    zodb_path = dir / 'db.db'
    zodb = ZODB.DB(str(zodb_path))
    return zodb


class KvpStore:
    """Simple wrapper around ZODB"""
    def __init__(self, zodb: ZODB.DB, name: str) -> None:
        self.name = name
        self._zodb = zodb

    @contextlib.contextmanager
    def transaction(
            self,
            bare: bool = False
    ):
        conn = self._zodb.open()
        try:
            if bare:
                yield conn.root()
            else:
                yield conn.root()[self.name]
        finally:
            transaction.commit()
            conn.close()


def logging_config():
    result = {
        "version": 1,
        "formatters": {
            "verbose_console": {
                "format": "%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s::%(filename)s::%(funcName)s:%(lineno)d %(process)d:%(threadName)s:%(thread)d %(message)s",  # noqa
                "datefmt": "%Y%m%dT%H%M%S"
            },
            "verbose_file": {
                "format": "■■%(asctime)s.%(msecs)03dZ■%(levelname)s■%(name)s■%(filename)s■%(funcName)s■%(lineno)d■%(message)s",  # noqa
                "datefmt": "%Y%m%dT%H%M%S"
            }
        },
        "handlers": {  # TODO: Remove this
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "verbose_console",
                "level": "DEBUG",
                "stream": "ext://sys.stdout"
            },
            "localfile_rotate_size_debug": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "verbose_file",
                "level": "DEBUG",
                "maxBytes": 67108864,
                "backupCount": 70,
                "filename": "debug.log",
                "encoding": "utf-8"
            },
            "localfile_rotate_size_warning": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "verbose_file",
                "level": "WARNING",
                "maxBytes": 67108864,
                "backupCount": 10,
                "filename": "warn.log",
                "encoding": "utf-8"
            }
        },
        "loggers": {
            "": {
                "handlers": [
                    "localfile_rotate_size_debug",
                    "localfile_rotate_size_warning",
                    # "console",  # TODO: remove this
                ],
                "level": "NOTSET"
            },
            "repro": {
                "level": "DEBUG",
                "propagate": True
            },
            "txn": {
                "level": "INFO",
                "propagate": True
            },
            "ZODB": {
                "level": "DEBUG",
                "propagate": True
            }
        }
    }
    return result


def check_btree(btree: BTrees.OOBTree.BTree) -> None:
    BTrees.check.check(btree)

    # Ensure that there is no empty bucket in the middle of the tree
    bucket = btree._firstbucket
    prev = None
    while bucket is not None:
        if bucket._next is not None and not any(bucket):
            raise AssertionError(
                'BTree bucket is empty and has next bucket. Status: {},'
                ' estimated size: {}, serial: {} .'
                ' prev: {}, next: {}'.format(
                    bucket._p_status,
                    bucket._p_estimated_size,
                    bucket._p_serial,
                    prev,
                    bucket._next))
        prev = bucket
        bucket = bucket._next


class BTreeEntry(persistent.Persistent):
    def __init__(
            self,
            state: States,
    ) -> None:
        self.state = str(state.name)
        self.version = 1


def main() -> None:
    random.seed()

    # Some constants
    btree_check_delay_sec = 60
    retention_time_sec = 30 * 60
    adds_sec_mean = 10
    adds_sec_std_dev = 1

    loop = None
    zodb = None
    try:
        logging.config.dictConfig(logging_config())

        logger.info('Starting repro')

        # get and set event loop mode
        loop = asyncio.get_event_loop()

        zodb = create_zodb()

        # Create Btree Holder
        collection = BtreeHolder(zodb)

        node = CollectionHolder(
            collection=collection,
            retention_time_sec=retention_time_sec)
        load_generator = LoadGenerator(
            events_sec_mean=adds_sec_mean,
            events_sec_std_dev=adds_sec_std_dev,
            handler=node.add_history_entry)

        _ = asyncio.ensure_future(
            monitor_btree_async(
                delay_sec=btree_check_delay_sec,
                collection=collection))
        _ = asyncio.ensure_future(
            node.remove_deleted_entries_async())
        _ = asyncio.ensure_future(
            load_generator.generate_load_async())

        logger.info('Running forever')
        loop.run_forever()
    finally:
        if zodb is not None:
            zodb.close()
        if loop is not None:
            if loop.is_running():
                loop.stop()
            loop.close()


class LoadGenerator:
    def __init__(
            self,
            events_sec_mean: float,
            events_sec_std_dev: float,
            handler: Callable[[], None]):
        self._handler = handler
        self.go = True
        self._events_sec_mean = events_sec_mean
        self._events_sec_std_dev = events_sec_std_dev

    async def generate_load_async(self):
        while self.go:
            start = datetime.datetime.utcnow()

            # Calculate target number of events per second
            events_per_sec = math.ceil(self.events_per_second())

            # The events are coming in chunks so simulate that
            for _ in range(0, events_per_sec):
                self._handler()

            end = datetime.datetime.utcnow()
            elapsed = end - start
            remaining = 1 - elapsed.total_seconds()
            logger.info('Sleeping {} seconds'.format(remaining))
            await asyncio.sleep(remaining)

    def events_per_second(self) -> float:
        return random.normalvariate(
            self._events_sec_mean,
            self._events_sec_std_dev)


class PersistentRoot(persistent.Persistent):
    def __init__(self):
        self.history = BTrees.OOBTree.BTree()


def _generate_btree_key(name, version):
    return '{}:{}'.format(name, version)


class BtreeHolder:
    def __init__(
            self,
            zodb: ZODB.DB
    ) -> None:
        self._history_lock = threading.Lock()  # type: threading.Lock
        # create zodb connection
        self.kvpstore = KvpStore(zodb, __name__)
        with self.kvpstore.transaction(bare=True) as root:
            if __name__ not in root:
                root[__name__] = PersistentRoot()

        self.recent_entries = set()  # type: Set[str]

    def add(
            self,
            name: str,
            entry: BTreeEntry) -> None:
        key = _generate_btree_key(name, entry.version)
        with self._history_lock:
            with self.kvpstore.transaction() as info:
                # update history
                info.history[key] = entry

        self.recent_entries.add(key)
        logger.info(
            'new state {} added for entry {}'.format(
                str(entry.state),
                name))

    def remove_deleted_entries(self) -> None:
        to_remove = []
        to_check = self.recent_entries
        self.recent_entries = set()
        with self._history_lock:
            with self.kvpstore.transaction() as info:
                for key in to_check:
                    history = info.history[key]
                    if history.state == str(States.Deleted.name):
                        to_remove.append(key)

                for key in to_remove:
                    info.history.pop(key, None)

    def check_btree(self) -> None:
        with self.kvpstore.transaction() as info:
            check_btree(info.history)


class CollectionHolder:
    def __init__(
            self,
            collection: BtreeHolder,
            retention_time_sec: int):
        self._collection = collection
        self._retention_time_sec = retention_time_sec

    def add_history_entry(self):
        asyncio.ensure_future(self.add_history_entry_async())

    async def add_history_entry_async(self):
        try:
            name = str(uuid.uuid4())
            entry = BTreeEntry(state=States.Uninitialized)
            self._collection.add(name, entry)

            await asyncio.sleep(random.random() * 2)  # nosec

            entry = BTreeEntry(state=States.Running)
            self._collection.add(name, entry)

            await asyncio.sleep(random.random() * 2)  # nosec

            entry = BTreeEntry(state=States.Completed)
            self._collection.add(name, entry)

            # Trigger delete later
            asyncio.get_event_loop().call_later(
                self._retention_time_sec,
                self.delete,
                name)
        except:
            logger.exception('error in add_history_entry_async')

    def delete(self, name: str):
        entry = BTreeEntry(state=States.Deleted)
        self._collection.add(name, entry)

    async def remove_deleted_entries_async(self):
        while True:
            self._collection.remove_deleted_entries()
            await asyncio.sleep(1)


async def monitor_btree_async(
        delay_sec: int,
        collection: BtreeHolder
) -> None:
    while True:
        try:
            collection.check_btree()
            logger.info('Done checking btree')
        except Exception:
            logger.exception('Unexpected exception checking BTree')

        await asyncio.sleep(delay_sec)


if __name__ == '__main__':
    main()
