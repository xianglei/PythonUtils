# 自撸工具项目

一边学一边用, 慢慢扩充吧, 也许有的已经有相应的模块了, 但是自己撸一遍, 也是对自己的一个锻炼.

## SimpleEmbedTimeSeriesDB

基于leveldb实现的嵌入式时间序列数据库, 因为有些项目无法安装influxdb, 所以走个嵌入式tsdb会更方便一些.

使用Leveldb模块的可用, 使用plyvel模块的版本因为我的mac m芯片用不了,还没调试. 理论上plyvel版本的更好

实现功能:

1. 添加单个数据点
2. 更新数据点
3. 查询数据点
4. 批量插入数据点
5. 自动创建索引以提高查询性能和支持范围查询
6. 查询时间范围内的数据点
7. 按标签查询数据点
8. 列出所有键
9. 按指标查询数据点

```python
if __name__ == '__main__':
    # ./metrics 是数据库存储路径
    db = TimeSeriesDB('./metrics')

    # 添加单个数据点
    tags = {'location': 'office'}
    # temperature 是指标, 1627588732 是时间戳, 23.5 是值, tags 是标签
    db.add_data_point('temperature', 1627588732, 23.5, tags)

    # 更新数据点
    db.update_data_point('temperature', 1627588732, 24.0, tags)

    # 查询数据点
    data_point = db.get_data_point('temperature', 1627588732)
    print('Updated data point:', data_point)

    # 添加多个数据点, 批量插入
    data_points = [
        {'metric': 'temperature', 'timestamp': 1627588733, 'value': 23.7, 'tags': {'location': 'office'}},
        {'metric': 'temperature', 'timestamp': 1627588734, 'value': 23.8, 'tags': {'location': 'office'}},
        {'metric': 'humidity', 'timestamp': 1627588732, 'value': 55.0, 'tags': {'location': 'office'}},
        {'metric': 'humidity', 'timestamp': 1627588733, 'value': 54.5, 'tags': {'location': 'office'}}
    ]
    db.add_data_points(data_points)

    # 查询时间范围内的数据点, 带tags查询所有数据
    range_results = db.query_range('temperature', 1627588732, 1627588734, {'location': 'office'})
    print('Range query results:', range_results)
    # 查询时间范围内的数据点, 不带tags查询所有数据
    range_results = db.query_range('humidity', 1627588732, 1627588734)
    print('Range query results:', range_results)

    # 按标签查询数据点
    tag_results = db.query_by_tags('humidity', {'location': 'office'})
    print('Tag query results:', tag_results)

    # 列出所有键
    all_keys = db.list_all_keys()
    print('All keys in the database:', all_keys)

    # 按指标获取数据点
    metrics = db.query_by_metric('temperature')
    print('All data points for metric temperature:', metrics)
```

## LSMTree

半学习项目, 实现一个基于LSM算法的kv数据库, 用于学习LSM树的实现原理.

实现功能

1. 基于内存的kv库 MemTable
2. 按kv对数量数量条目阈值刷写磁盘持久存储 SSTable
3. wal日志记录与数据恢复
4. 布隆过滤器
5. 使用snappy对存储数据和wal进行压缩
6. SSTable的merge
7. SSTable的compaction

```python
db_path = './lsm_db'
lsm_tree = LSMTree(db_path)

# 插入数据
for i in range(1000):
    lsm_tree.put('column1', f'key{i}', {'value': i})

# 查询数据
print(lsm_tree.get('column1', 'key2'))  # 输出: {'value': 2}

# 范围查询
print(lsm_tree.query('column1', 'key0', 'key10'))  # 输出前10条记录
```
