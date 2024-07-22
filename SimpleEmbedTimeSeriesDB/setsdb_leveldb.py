#!/usr/bin/env python3
# coding: utf-8

import leveldb
import struct
import json
import os


class TimeSeriesDB:
    def __init__(self, path):
        db_path = os.path.join(path, 'db')
        if not os.path.exists(path):
            try:
                os.makedirs(db_path)
            except Exception as e:
                raise e
        db_index = os.path.join(path, 'index')
        if not os.path.exists(db_index):
            try:
                os.makedirs(db_index)
            except Exception as e:
                raise e
        self.db = leveldb.LevelDB(db_path, create_if_missing=True)
        self.index_db = leveldb.LevelDB(db_index, create_if_missing=True)

    def serialize_key(self, metric, timestamp):
        return f"{metric}:{timestamp}".encode('utf-8')

    def serialize_index_key(self, metric, timestamp, tags):
        return f"{metric}:{timestamp}:{json.dumps(tags, sort_keys=True)}".encode('utf-8')

    def add_data_point(self, metric, timestamp, value, tags):
        key = self.serialize_key(metric, timestamp)
        self.db.Put(key, struct.pack('d', value))
        self.add_index(metric, timestamp, tags)

    def add_data_points(self, data_points):
        batch = leveldb.WriteBatch()
        index_batch = leveldb.WriteBatch()
        for dp in data_points:
            key = self.serialize_key(dp['metric'], dp['timestamp'])
            batch.Put(key, struct.pack('d', dp['value']))
            index_key = self.serialize_index_key(dp['metric'], dp['timestamp'], dp['tags'])
            index_batch.Put(index_key, b'')
        self.db.Write(batch)
        self.index_db.Write(index_batch)

    def add_index(self, metric, timestamp, tags):
        index_key = self.serialize_index_key(metric, timestamp, tags)
        self.index_db.Put(index_key, b'')

    def get_data_point(self, metric, timestamp):
        key = self.serialize_key(metric, timestamp)
        value = self.db.Get(key)
        if value:
            return struct.unpack('d', value)[0]
        return None

    def delete_data_point(self, metric, timestamp, tags):
        key = self.serialize_key(metric, timestamp)
        self.db.Delete(key)
        self.delete_index(metric, timestamp, tags)

    def delete_index(self, metric, timestamp, tags):
        index_key = self.serialize_index_key(metric, timestamp, tags)
        self.index_db.Delete(index_key)

    def update_data_point(self, metric, timestamp, value, tags):
        key = self.serialize_key(metric, timestamp)
        if self.db.Get(key):
            self.db.Put(key, struct.pack('d', value))
            self.add_index(metric, timestamp, tags)
        else:
            raise KeyError(f"Data point for metric '{metric}' at timestamp '{timestamp}' not found.")

    def query_range(self, metric, start_timestamp, end_timestamp, tags=None):
        results = []
        for key, _ in self.index_db.RangeIter():
            k_metric, k_timestamp, k_tags = key.decode('utf-8').split(':', 2)
            k_timestamp = int(k_timestamp)
            k_tags = json.loads(k_tags)
            if tags is not None:
                if (k_metric == metric and start_timestamp <= k_timestamp <= end_timestamp and
                        all(item in k_tags.items() for item in tags.items())):
                    value = self.get_data_point(k_metric, k_timestamp)
                    results.append({
                        'metric': k_metric,
                        'timestamp': k_timestamp,
                        'value': value,
                        'tags': k_tags
                    })
            else:
                if k_metric == metric and start_timestamp <= k_timestamp <= end_timestamp:
                    value = self.get_data_point(k_metric, k_timestamp)
                    results.append({
                        'metric': k_metric,
                        'timestamp': k_timestamp,
                        'value': value,
                        'tags': k_tags
                    })

        return results

    def query_by_tags(self, metric, tags):
        results = []
        for key, _ in self.index_db.RangeIter():
            k_metric, k_timestamp, k_tags = key.decode('utf-8').split(':', 2)
            k_timestamp = int(k_timestamp)
            k_tags = json.loads(k_tags)
            if k_metric == metric and all(item in k_tags.items() for item in tags.items()):
                value = self.get_data_point(k_metric, k_timestamp)
                results.append({
                    'metric': k_metric,
                    'timestamp': k_timestamp,
                    'value': value,
                    'tags': k_tags
                })
        return results

    def list_all_keys(self):
        keys = []
        for key, _ in self.db.RangeIter():
            keys.append(key.decode('utf-8'))
        return keys

    def query_by_metric(self, metric):
        results = []
        for key, value in self.db.RangeIter():
            key_str = key.decode('utf-8')
            if key_str.startswith(f'{metric}:'):
                metric, timestamp = key_str.split(':')
                timestamp = int(timestamp)
                value = struct.unpack('d', value)[0]
                results.append({
                    'metric': metric,
                    'timestamp': timestamp,
                    'value': value
                })
        return results

    def query_by_tag_key(self, tag_key, tag_value):
        results = []
        for key, _ in self.index_db.RangeIter():
            k_metric, k_timestamp, k_tags = key.decode('utf-8').split(':', 2)
            k_timestamp = int(k_timestamp)
            k_tags = json.loads(k_tags)
            if k_tags.get(tag_key) == tag_value:
                value = self.get_data_point(k_metric, k_timestamp)
                results.append({
                    'metric': k_metric,
                    'timestamp': k_timestamp,
                    'value': value,
                    'tags': k_tags
                })
        return results

    def close(self):
        # LevelDB does not require explicit close method.
        pass

"""
# 使用示例
if __name__ == '__main__':
    db = TimeSeriesDB('./metrics')

    # 添加单个数据点
    tags = {'location': 'office'}
    db.add_data_point('temperature', 1627588732, 23.5, tags)

    # 更新数据点
    db.update_data_point('temperature', 1627588732, 24.0, tags)

    # 查询数据点
    data_point = db.get_data_point('temperature', 1627588732)
    print('Updated data point:', data_point)

    # 添加多个数据点
    data_points = [
        {'metric': 'temperature', 'timestamp': 1627588733, 'value': 23.7, 'tags': {'location': 'office'}},
        {'metric': 'temperature', 'timestamp': 1627588734, 'value': 23.8, 'tags': {'location': 'office'}},
        {'metric': 'humidity', 'timestamp': 1627588732, 'value': 55.0, 'tags': {'location': 'office'}},
        {'metric': 'humidity', 'timestamp': 1627588733, 'value': 54.5, 'tags': {'location': 'office'}}
    ]
    db.add_data_points(data_points)

    # 查询时间范围内的数据点
    range_results = db.query_range('temperature', 1627588732, 1627588734, {'location': 'office'})
    print('Range query results:', range_results)

    # 按标签查询数据点
    tag_results = db.query_by_tags('humidity', {'location': 'office'})
    print('Tag query results:', tag_results)

    # 列出所有键
    all_keys = db.list_all_keys()
    print('All keys in the database:', all_keys)

    #
    metrics = db.query_by_metric('temperature')
    print('All data points for metric temperature:', metrics)

"""
