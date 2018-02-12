import time
import json
import queue
import pathlib
import threading
from collections import namedtuple


class DBase:
    DB_PATH = 'db/'
    DB_MAX_ORDINAL = 0xFFFFFFFF
    DB_TUPLE = namedtuple('DB_TUPLE', ['stamp', 'key', 'value'])
    DB_MAX_SIZE = 8 * 1024 * 1024
    INDEX_TUPLE = namedtuple('INDEX_TUPLE', ['file', 'offset'])
    CACHE_SIZE = 64
    CACHE_TUPLE = namedtuple('CACHE_TUPLE', ['stamp', 'value'])
    CACHE_DURATION = 60.0

    def __init__(self):
        self._index = dict()
        self._index_lock = threading.Lock()
        self._cache = dict()
        self._cache_lock = threading.Lock()
        self._query_history = queue.Queue()

        # init db direction
        self._db_path = pathlib.Path(self.DB_PATH)
        if not self._db_path.exists():
            self._db_path.mkdir()
        if not self._db_path.is_dir():
            raise RuntimeError('not a dir DB_PATH: {}'.format(self.DB_PATH))

        # init db storage file
        self._db_files = list()
        for i in range(self.DB_MAX_ORDINAL):
            child = self._db_path.joinpath('storage.{}.db'.format(i))
            if not child.exists():
                break
            if not child.is_file():
                raise RuntimeError('error file: {}'.format(child))
            self._db_files.append(child.open(mode='r'))
        else:
            raise RuntimeError('out of DB_MAX_ORDINAL: {}'.format(self.DB_MAX_ORDINAL))
        self._num_dbs = len(self._db_files)
        self._active_file = None

        # exit flag for background threads
        self._stop_flag = False

        # cache update thread
        self._cache_update_thread = threading.Thread(target=self._arrange_cache)
        self._cache_update_thread.start()

        # storage merge thread

    def __del__(self):
        for f in self._db_files:
            f.close()
        self.stop()

    def _logging_status(self):
        while True:
            if self._stop_flag:
                break
            time.sleep(10.0)

    def _arrange_cache(self):
        while True:
            if self._stop_flag:
                break
            if len(self._cache) < self.CACHE_SIZE // 2:
                time.sleep(2.0)
                continue
            key = self._query_history.get()
            # stamp, _ = self._cache[key]
            self._query_history.task_done()

    def _merge_storage(self):
        # reduce per db file
        def task_reduce():
            pass

        # merge different db file
        def task_merge():
            pass

        while True:
            if self._stop_flag:
                break
            time.sleep(60.0)

    def stop(self):
        self._stop_flag = True
        self._cache_update_thread.join()

    def _switch_active_db(self):
        child = self._db_path.joinpath('storage.{}.db'.format(self._num_dbs))
        if child.exists():
            raise RuntimeError('error num_dbs: {}'.format(self._num_dbs))
        writer, reader = child.open(mode='a'), child.open(mode='r')
        self._db_files.append(reader)
        if self._active_file is not None:
            self._active_file.close()
        self._active_file = writer
        self._num_dbs += 1

    def update(self, key, value):
        # format value ready for storage
        buffer = self.DB_TUPLE(stamp=time.time(),
                               key=key,
                               value=value)

        # check storage file status
        if self._active_file is None or self._active_file.tell() > self.DB_MAX_SIZE:
            self._switch_active_db()

        # update in-memory index
        self._index[key] = self.INDEX_TUPLE(file=self._db_files[-1],
                                            offset=self._active_file.tell())

        # write to cache
        self._cache[key] = self.CACHE_TUPLE(stamp=time.time(),
                                            value=value)

        # write to disk
        print(json.dumps(buffer), end='\n', file=self._active_file, flush=True)

    def get(self, key, default=None):
        # check key valid
        if key not in self._index:
            return default

        # add to query_history for caching
        self._query_history.put(key)

        # check cache
        if key in self._cache:
            # hint
            _, value = self._cache[key]
        else:
            # seek to disk file and read it
            file, offset = self._index[key]
            file.seek(offset)
            line = file.readline()
            _, _, value = json.loads(line)

        # cache it (or update stamp)
        self._cache[key] = self.CACHE_TUPLE(stamp=time.time(),
                                            value=value)
        return value


def test_db():
    import random

    db = DBase()
    table = dict()
    #
    before = time.time()
    for i in range(0xFFFFF):
        key = random.randrange(0xFF)
        value = random.randrange(0xFFFFFFFF)
        table[key] = value
        db.update(key, value)
    after = time.time()
    print('Update QPS: {}'.format(0xFFFFF / (after - before)))
    #
    for key in table:
        value = table[key]
        assert value == db.get(key)
    #
    time.sleep(10.0)
    #
    before = time.time()
    for i in range(0xFFFFF):
        key = random.randrange(0xFF)
        db.get(key)
    after = time.time()
    print('Get QPS: {}'.format(0xFFFFF / (after - before)))


if __name__ == '__main__':
    test_db()
