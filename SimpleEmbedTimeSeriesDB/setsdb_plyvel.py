#!/usr/bin/env python3
# coding: utf-8

import plyvel
import struct
import json
import os


class TimeSeriesDB:
    def __init__(self, path):
        db_path = os.path.join(path, 'db')
        index_db_path = os.path.join(path, 'index')

        # Create directories if they don't exist
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        if not os.path.exists(index_db_path):
            os.makedirs(index_db_path)

        # Open LevelDB databases with advanced options
        self.db = plyvel.DB(db_path, create_if_missing=True, compression='snappy')
        self.index_db = plyvel.DB(index_db_path, create_if_missing=True, compression='snappy')

    def serialize_key(self, metric, timestamp):
        return f"{metric}:{timestamp}".encode('utf-8')

    def serialize_index_key(self, metric, timestamp, tags):
        return f"{metric}:{timestamp}:{json.dumps(tags, sort_keys=True)}".encode('utf-8')

    def add_data_point(self, metric, timestamp, value, tags):
        key = self.serialize_key(metric, timestamp)
        self.db.put(key, struct.pack('d', value))
        self.add_index(metric, timestamp, tags)

    def add_data_points(self, data_points):
        batch = plyvel.WriteBatch()
        index_batch = plyvel.WriteBatch()
        for dp in data_points:
            key = self.serialize_key(dp['metric'], dp['timestamp'])
            batch.put(key, struct.pack('d', dp['value']))
            index_key = self.serialize_index_key(dp['metric'], dp['timestamp'], dp['tags'])
            index_batch.put(index_key, b'')
        self.db.write(batch)
        self.index_db.write(index_batch)

    def add_index(self, metric, timestamp, tags):
        index_key = self.serialize_index_key(metric, timestamp, tags)
        self.index_db.put(index_key, b'')

    def get_data_point(self, metric, timestamp):
        key = self.serialize_key(metric, timestamp)
        value = self.db.get(key)
        if value:
            return struct.unpack('d', value)[0]
        return None

    def delete_data_point(self, metric, timestamp, tags):
        key = self.serialize_key(metric, timestamp)
        self.db.delete(key)
        self.delete_index(metric, timestamp, tags)

    def delete_index(self, metric, timestamp, tags):
        index_key = self.serialize_index_key(metric, timestamp, tags)
        self.index_db.delete(index_key)

    def update_data_point(self, metric, timestamp, value, tags):
        key = self.serialize_key(metric, timestamp)
        if self.db.get(key):
            self.db.put(key, struct.pack('d', value))
            self.add_index(metric, timestamp, tags)
        else:
            raise KeyError(f"Data point for metric '{metric}' at timestamp '{timestamp}' not found.")

    def query_range(self, metric, start_timestamp, end_timestamp, tags):
        results = []
        for key, _ in self.index_db:
            k_metric, k_timestamp, k_tags = key.decode('utf-8').split(':', 2)
            k_timestamp = int(k_timestamp)
            k_tags = json.loads(k_tags)
            if (k_metric == metric and start_timestamp <= k_timestamp <= end_timestamp and
                    all(item in k_tags.items() for item in tags.items())):
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
        for key, _ in self.index_db:
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
        for key, _ in self.db:
            keys.append(key.decode('utf-8'))
        return keys

    def query_by_metric(self, metric):
        results = []
        for key, value in self.db:
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
        for key, _ in self.index_db:
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
        self.db.close()
        self.index_db.close()
