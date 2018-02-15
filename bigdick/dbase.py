import time
import json
import queue
import logging
import pathlib
import threading
from collections import namedtuple


class _SafeDict(dict):
    def __init__(self):
        dict.__init__(self)
        self._lock = threading.Lock()

    def __getitem__(self, key):
        self._lock.acquire()
        value = dict.__getitem__(self, key)
        self._lock.release()
        return value

    def __setitem__(self, key, value):
        self._lock.acquire()
        dict.__setitem__(self, key, value)
        self._lock.release()

    def __delitem__(self, key):
        self._lock.acquire()
        dict.__delitem__(self, key)
        self._lock.release()


class _Cache(_SafeDict):
    def __init__(self, index, cache_size, cache_duration):
        _SafeDict.__init__(self)
        self._index = index
        self._cache_size = cache_size
        self._cache_duration = cache_duration
        self._setitem_history = queue.Queue()

    def __setitem__(self, key, value):
        if len(self) >= self._cache_size and key not in self:
            return
        self._setitem_history.put((time.time(), key))
        _SafeDict.__setitem__(self, key, value)

    def free_cache(self):
        # stamps for comparing
        if self._setitem_history.unfinished_tasks == 0:
            time.sleep(1.0)
            return
        (stamp_operation, key), _ = self._setitem_history.get(), self._setitem_history.task_done()
        if key not in self:
            return
        stamp_cache, _ = self[key]

        # system clock changed
        current_time = time.time()
        if current_time < stamp_cache or current_time < stamp_operation:
            logging.warning('system clock changed')
            return

        # stamp_1 != stamp_2, current cache is alive and do not free it
        if not (abs(stamp_cache - stamp_operation) < 1e-1):
            return

        # wait until stamp_operation is old enough
        while True:
            current_time = time.time()
            if current_time - stamp_operation > self._cache_duration:
                break
            time.sleep(2.0)

        # check stamp_cache again
        stamp_cache, _ = self[key]
        if not (abs(stamp_cache - stamp_operation) < 1e-1):  # stamp_1 != stamp_2, cache updated
            return
        del self[key]
        # print('free: {0}, len: {1}'.format(key, len(self)))


class _Index(_SafeDict):
    pass


class _DBase:
    DB_NAME_PREFIX, DB_NAME_SUFFIX = 'storage', 'db'
    #
    DB_TUPLE = namedtuple('DB_TUPLE', ['stamp', 'key', 'value'])
    INDEX_TUPLE = namedtuple('DB_INDEX_TUPLE', ['file', 'offset'])
    CACHE_TUPLE = namedtuple('CACHE_TUPLE', ['stamp', 'value'])

    def __init__(self, db_direction='db/', db_filesize=8 * 1024 * 1024, cache_size=1024, cache_duration=60.0):
        # config file
        self._db_filesize = db_filesize
        self._cache_size = cache_size

        self._index = _Index()
        self._cache = _Cache(index=self._index, cache_size=cache_size, cache_duration=cache_duration)
        self._lock = threading.Lock()  # this lock is for file operations in different thread

        # init db direction
        self._db_direction = pathlib.Path(db_direction)
        if not self._db_direction.exists():
            self._db_direction.mkdir()
        if not self._db_direction.is_dir():
            raise RuntimeError('not a dir DB_PATH: {}'.format(self._db_direction))

        # check valid and sort files
        files = []
        for file in self._db_direction.iterdir():
            if not file.is_file():
                continue
            prefix, idx, suffix = file.name.split('.')
            if prefix != self.DB_NAME_PREFIX or not idx.isdigit() or suffix != self.DB_NAME_SUFFIX:
                continue
            files.append(file)
        files = sorted(files, key=lambda item: int(item.name.split('.')[1]))

        # move file to ensure index continuous
        for idx, file in enumerate(files):
            db_idx = int(file.name.split('.')[1])
            if db_idx != idx:
                name = '{0}.{1}.{2}'.format(self.DB_NAME_PREFIX,
                                            idx,
                                            self.DB_NAME_SUFFIX)
                new_file = self._db_direction.joinpath(name)
                assert not new_file.exists()
                file.replace(new_file)
                files[idx] = new_file

        # init db storage file
        self._db_files = [file.open(mode='r', encoding='utf-8') for file in files]
        self._num_dbs = len(self._db_files)
        self._active_file = None

        # exit flag for background threads
        self._stop_flag = False

        # cache update thread
        self._cache_free_thread = threading.Thread(target=self._free_cache)
        self._cache_free_thread.start()

        # storage merge thread

    def __del__(self):
        for f in self._db_files:
            f.close()

    def _logging_status(self):
        while True:
            if self._stop_flag:
                break
            time.sleep(10.0)

    def _free_cache(self):
        while True:
            if self._stop_flag:
                break
            if len(self._cache) < self._cache_size * 0.8:
                time.sleep(2.0)
                continue
            self._cache.free_cache()

    def _merge_storage(self):
        self._lock.acquire()
        self._lock.release()

    def stop(self):
        self._stop_flag = True
        self._cache_free_thread.join()

    def _switch_active_db(self):
        # validate the new storage file
        child = self._db_direction.joinpath('{}.{}.{}'.format(self.DB_NAME_PREFIX,
                                                              self._num_dbs,
                                                              self.DB_NAME_SUFFIX))
        if child.exists():
            raise RuntimeError('error num_dbs: {}'.format(self._num_dbs))
        self._num_dbs += 1

        # create it
        writer, reader = child.open(mode='a'), child.open(mode='r')
        self._db_files.append(reader)
        if self._active_file is not None:
            self._active_file.close()
        self._active_file = writer

    def update(self, key, value):
        self._lock.acquire()

        # format value ready for storage
        buffer = self.DB_TUPLE(stamp=time.time(),
                               key=key,
                               value=value)

        # check storage file status
        if self._active_file is None or self._active_file.tell() > self._db_filesize:
            self._switch_active_db()

        # write to cache
        self._cache[key] = self.CACHE_TUPLE(stamp=time.time(),
                                            value=value)

        # update in-memory index
        self._index[key] = self.INDEX_TUPLE(file=self._db_files[-1],
                                            offset=self._active_file.tell())

        # write to disk
        print(json.dumps(buffer), end='\n', file=self._active_file, flush=True)

        self._lock.release()

    def get(self, key, default=None):
        self._lock.acquire()

        # check key valid
        if key not in self._index:
            return default

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

        self._lock.release()
        return value


class DB(_DBase):
    pass


def test_db():
    import random

    db = DB(cache_size=64, cache_duration=10.0)

    def update():
        for i in range(0xFFFF):
            key, value = random.randrange(0xFF), random.randrange(0xFFFFFFFF)
            db.update(key, value)
    update()

    def get():
        for i in range(0xFFFF):
            key = random.randrange(0xFF)
            db.get(key)
            time.sleep(0.1)
    get()
    db.stop()


if __name__ == '__main__':
    test_db()
