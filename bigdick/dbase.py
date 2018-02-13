import time
import json
import queue
import pathlib
import threading
from collections import namedtuple


class DBase:
    DB_DIRECTION = 'db/'
    DB_NAME_PREFIX, DB_NAME_SUFFIX = 'storage', 'db'
    DB_TUPLE = namedtuple('DB_TUPLE', ['stamp', 'key', 'value'])
    DB_MAX_SIZE = 8 * 1024 * 1024
    DB_INDEX_TUPLE = namedtuple('DB_INDEX_TUPLE', ['file', 'offset'])
    CACHE_SIZE = 64
    CACHE_TUPLE = namedtuple('CACHE_TUPLE', ['stamp', 'value'])
    CACHE_DURATION = 60.0

    def __init__(self):
        self._index = dict()
        self._index_lock = threading.Lock()
        self._cache = dict()
        self._cache_lock = threading.Lock()

        # init db direction
        self._db_direction = pathlib.Path(self.DB_DIRECTION)
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
        self._cache_arrange_thread = threading.Thread(target=self._arrange_cache)
        self._cache_arrange_thread.start()

        # storage merge thread

    def __del__(self):
        for f in self._db_files:
            f.close()

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
            self._cache_lock.acquire()
            # stamp, _ = self._cache[key]
            self._cache_lock.release()

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
        self._cache_arrange_thread.join()

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
        # format value ready for storage
        buffer = self.DB_TUPLE(stamp=time.time(),
                               key=key,
                               value=value)

        # check storage file status
        if self._active_file is None or self._active_file.tell() > self.DB_MAX_SIZE:
            self._switch_active_db()

        # update in-memory index
        self._index[key] = self.DB_INDEX_TUPLE(file=self._db_files[-1],
                                               offset=self._active_file.tell())

        # write to cache
        self._cache_lock.acquire()
        self._cache[key] = self.CACHE_TUPLE(stamp=time.time(),
                                            value=value)
        self._cache_lock.release()

        # write to disk
        print(json.dumps(buffer), end='\n', file=self._active_file, flush=True)

    def get(self, key, default=None):
        # check key valid
        if key not in self._index:
            return default

        # check cache
        self._cache_lock.acquire()
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
        self._cache_lock.release()
        return value


def test_db():
    import random

    db = DBase()
    table = dict()

    def update():
        before = time.time()
        for i in range(0xFFFF):
            key = random.randrange(0xFF)
            value = random.randrange(0xFFFFFFFF)
            table[key] = value
            db.update(key, value)
        after = time.time()
        print('Update QPS: {}'.format(0xFFFF / (after - before)))
        #
        for key in table:
            value = table[key]
            assert value == db.get(key)

    update()
    time.sleep(5.0)

    def get():
        before = time.time()
        for i in range(0xFFFFF):
            key = random.randrange(0xFF)
            db.get(key)
        after = time.time()
        print('Get QPS: {}'.format(0xFFFFF / (after - before)))

    get()
    db.stop()


if __name__ == '__main__':
    test_db()
