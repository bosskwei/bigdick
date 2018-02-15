import time
import string
import random
import shutil
import tempfile
import threading
import bigdick


def test_cache():
    # test correctness without cache, small cache and huge cache
    ops_write, ops_read = 0xFFF, 0xFFFFF
    cache_sizes = [0, 64, 1024]

    for i in range(3):
        tmp_dir = tempfile.mkdtemp()

        def runtime_space():
            db = bigdick.DB(db_direction=tmp_dir, cache_size=cache_sizes[i], cache_duration=10.0)
            tmp_dict = dict()

            def update():
                before = time.time()
                for _ in range(ops_write):
                    key, value = random.randrange(0xFF), random.randrange(0xFFFF_FFFF)
                    tmp_dict[key] = value
                    db.update(key, value)
                after = time.time()
                print('cache_size: {}, Update QPS: {:.2f}'.format(cache_sizes[i], ops_write / (after - before + 1e-3)))
            update()

            def get():
                keys = [random.randrange(0xFF) for _ in range(ops_read)]
                before = time.time()
                for key in keys:
                    value_1 = tmp_dict[key]
                    value_2 = db.get(key)
                    assert value_1 == value_2
                after = time.time()
                print('cache_size: {}, Get QPS: {:.2f}'.format(cache_sizes[i], ops_read / (after - before + 1e-3)))
            get()
            db.stop()
        runtime_space()
        shutil.rmtree(tmp_dir)
        time.sleep(2.0)


def test_efficiency():
    # test db with hot data and cold data
    ops_write, ops_read = 0xFFF, 0xFFFF
    cache_size = 64
    tmp_dir = tempfile.mkdtemp()
    value = ''.join([random.choice(string.ascii_letters) for _ in range(1024)])

    def runtime_space():
        db = bigdick.DB(db_direction=tmp_dir, cache_size=cache_size, cache_duration=2.5)

        def update():
            before = time.time()
            for _ in range(ops_write):
                key = random.randrange(0xFF)
                db.update(key, value)
            after = time.time()
            print('cache_size: {}, Update QPS: {:.2f}'.format(cache_size, ops_write / (after - before + 1e-3)))
        update()
        time.sleep(5.0)  # wait cache freed

        def get():
            keys = [random.randrange(0x33) if random.random() < 0.8 else random.randrange(0x33, 0xFF) for _ in range(ops_read)]
            before = time.time()
            for key in keys:
                db.get(key)
            after = time.time()
            print('cache_size: {}, Get QPS: {:.2f}'.format(cache_size, ops_read / (after - before + 1e-3)))
        get()
        db.stop()

    runtime_space()
    shutil.rmtree(tmp_dir)


def test_threading():
    n_threads, ops_write, ops_read = 64, 0xFF, 0xFFF
    cache_size = 1024
    tmp_dir = tempfile.mkdtemp()

    def runtime_space():
        db = bigdick.DB(db_direction=tmp_dir, cache_size=cache_size, cache_duration=10.0)

        def update():
            for _ in range(ops_write):
                key, value = random.randrange(0xFF), random.randrange(0xFFFF_FFFF)
                db.update(key, value)
        ts = [threading.Thread(target=update) for _ in range(n_threads)]
        before = time.time()
        _, _ = [t.start() for t in ts], [t.join() for t in ts]
        after = time.time()
        print('cache_size: {}, Update QPS: {:.2f}'.format(cache_size, n_threads * ops_write / (after - before + 1e-3)))

        def get():
            for _ in range(ops_read):
                key = random.randrange(0xFF)
                db.get(key)
        ts = [threading.Thread(target=get) for _ in range(n_threads)]
        before = time.time()
        _, _ = [t.start() for t in ts], [t.join() for t in ts]
        after = time.time()
        print('cache_size: {}, Get QPS: {:.2f}'.format(cache_size, n_threads * ops_read / (after - before + 1e-3)))
        db.stop()

    runtime_space()
    shutil.rmtree(tmp_dir)


if __name__ == '__main__':
    print('test_cache:')
    test_cache()
    print('')

    print('test_efficiency:')
    test_efficiency()
    print('')

    print('test_threading:')
    test_threading()
    print('')
