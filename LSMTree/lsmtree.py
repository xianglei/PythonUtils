import os
import json
import struct
import threading
import time
import snappy
from bitarray import bitarray
from hashlib import md5
from concurrent.futures import ThreadPoolExecutor


class BloomFilter:
    def __init__(self, size, hash_count):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)

    def _hashes(self, item):
        return [int(md5(item.encode('utf-8')).hexdigest(), 16) % self.size for _ in range(self.hash_count)]

    def add(self, item):
        for seed in self._hashes(item):
            self.bit_array[seed] = 1

    def check(self, item):
        for seed in self._hashes(item):
            if self.bit_array[seed] == 0:
                return False
        return True


class MemTable:
    def __init__(self):
        self.table = {}
        self.size = 0

    def put(self, column, key, value):
        if column not in self.table:
            self.table[column] = {}
        self.table[column][key] = value
        self.size += 1

    def get(self, column, key):
        if column in self.table and key in self.table[column]:
            return self.table[column][key]
        return None

    def delete(self, column, key):
        if column in self.table and key in self.table[column]:
            del self.table[column][key]
            self.size -= 1

    def flush(self):
        return self.table

    def clear(self):
        self.table = {}
        self.size = 0


class SSTable:
    def __init__(self, path):
        self.path = path
        self.index = {}

    def write(self, table):
        self.index = {}
        with open(self.path, 'wb') as f:
            for column, kv in table.items():
                start_pos = f.tell()
                compressed_data = snappy.compress(json.dumps(kv).encode('utf-8'))
                f.write(compressed_data)
                end_pos = f.tell()
                self.index[column] = (start_pos, end_pos)

    def read(self, column):
        if column not in self.index:
            return {}
        start_pos, end_pos = self.index[column]
        with open(self.path, 'rb') as f:
            f.seek(start_pos)
            compressed_data = f.read(end_pos - start_pos)
            return json.loads(snappy.decompress(compressed_data).decode('utf-8'))

    def get_columns(self):
        return list(self.index.keys())


class WAL:
    def __init__(self, path):
        self.path = path
        self.file = open(self.path, 'ab')

    def append(self, operation, column, key, value=None):
        if operation == 'put':
            value_data = snappy.compress(json.dumps(value).encode('utf-8'))
            entry = struct.pack('>3s32s32sI', b'put', column.encode('utf-8'), key.encode('utf-8'), len(value_data)) + value_data
        else:
            entry = struct.pack('>3s32s32s', b'del', column.encode('utf-8'), key.encode('utf-8'))
        self.file.write(entry)
        self.file.flush()

    def replay(self):
        entries = []
        with open(self.path, 'rb') as f:
            while True:
                entry_header = f.read(71)
                if not entry_header:
                    break
                operation, column, key, value_len = struct.unpack('>3s32s32sI', entry_header)
                if operation == b'put':
                    try:
                        compressed_value = f.read(value_len)
                        value = json.loads(snappy.decompress(compressed_value).decode('utf-8'))
                        entries.append((operation.decode('utf-8'), column.decode('utf-8').strip('\x00'), key.decode('utf-8').strip('\x00'), value))
                    except Exception as e:
                        print(f"Error decompressing or decoding value: {e}")
                else:
                    entries.append((operation.decode('utf-8'), column.decode('utf-8').strip('\x00'), key.decode('utf-8').strip('\x00')))
        return entries

    def clear(self):
        self.file.close()
        self.file = open(self.path, 'w+b')


class LSMTree:
    def __init__(self, db_path):
        if not os.path.exists(db_path):
            try:
                os.makedirs(db_path)
            except IOError as e:
                raise e
        self.db_path = db_path
        self.memtable = MemTable()
        self.sstable_paths = [os.path.join(db_path, f'sstable_{i}.db') for i in range(3)]
        self.sstables = [SSTable(path) for path in self.sstable_paths]
        self.bloom_filter = BloomFilter(10000, 4)
        self.wal = WAL(os.path.join(db_path, 'wal.log'))
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.init_sstables()
        self.recover_from_wal()
        self.start_flush_timer()

    def init_sstables(self):
        for sstable in self.sstables:
            columns = sstable.get_columns()
            for column in columns:
                kv = sstable.read(column)
                for key in kv:
                    self.bloom_filter.add(f'{column}:{key}')

    def recover_from_wal(self):
        entries = self.wal.replay()
        for entry in entries:
            if entry[0] == 'put':
                self.memtable.put(entry[1], entry[2], entry[3])
            elif entry[0] == 'delete':
                self.memtable.delete(entry[1], entry[2])
        self.wal.clear()

    def put(self, column, key, value):
        with self.lock:
            self.wal.append('put', column, key, value)
            self.memtable.put(column, key, value)
            self.bloom_filter.add(f'{column}:{key}')
            if self.memtable.size > 200:
                self.flush()

    def get(self, column, key):
        with self.lock:
            if not self.bloom_filter.check(f'{column}:{key}'):
                return None
            value = self.memtable.get(column, key)
            if value is not None:
                return value
            for sstable in self.sstables:
                table = sstable.read(column)
                if key in table:
                    return table[key]
            return None

    def delete(self, column, key):
        with self.lock:
            self.wal.append('delete', column, key)
            self.memtable.delete(column, key)

    def flush(self):
        memtable_data = self.memtable.flush()
        self.merge_sstables(memtable_data)
        self.memtable.clear()

    def merge_sstables(self, memtable_data):
        for i, sstable in enumerate(self.sstables):
            table = {}
            columns = sstable.get_columns()
            for column in columns:
                table[column] = sstable.read(column)
            for column, kv in memtable_data.items():
                if column not in table:
                    table[column] = {}
                table[column].update(kv)
            sstable.write(table)
            if len(table) > 200:
                self.compact(i)

    def compact(self, level):
        if level == len(self.sstables) - 1:
            return  # 最后一级不再合并
        current_table = {}
        columns = self.sstables[level].get_columns()
        for column in columns:
            current_table[column] = self.sstables[level].read(column)
        next_table = {}
        columns = self.sstables[level + 1].get_columns()
        for column in columns:
            next_table[column] = self.sstables[level + 1].read(column)
        for column, kv in current_table.items():
            if column not in next_table:
                next_table[column] = {}
            next_table[column].update(kv)
        self.sstables[level + 1].write(next_table)
        self.sstables[level].write({})  # 清空当前 SSTable

    def start_flush_timer(self):
        def flush_interval():
            while True:
                time.sleep(30)
                with self.lock:
                    self.flush()

        flush_thread = threading.Thread(target=flush_interval)
        flush_thread.daemon = True
        flush_thread.start()

    def query(self, column, start_key, end_key):
        results = []

        with self.lock:
            if column in self.memtable.table:
                keys = sorted(self.memtable.table[column].keys())
                for key in keys:
                    if start_key <= key <= end_key:
                        results.append((key, self.memtable.table[column][key]))

            for sstable in self.sstables:
                table = sstable.read(column)
                keys = sorted(table.keys())
                for key in keys:
                    if start_key <= key <= end_key:
                        results.append((key, table[key]))

        return results

"""
# 使用示例
db_path = './lsm_db'
lsm_tree = LSMTree(db_path)

# 插入数据
for i in range(1000):
    lsm_tree.put('column1', f'key{i}', {'value': i})

# 查询数据
print(lsm_tree.get('column1', 'key2'))  # 输出: {'value': 2}

# 范围查询
print(lsm_tree.query('column1', 'key0', 'key10'))  # 输出前10条记录
"""
