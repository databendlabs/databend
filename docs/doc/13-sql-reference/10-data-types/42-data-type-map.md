---
title: Map
---

The MAP data structure is utilized for holding a set of `Key:Value` pairs, and stores data using a nested data structure of Array(Tuple(key, value)). It is appropriate in situations where the data type is constant, but the `Key`'s value cannot be entirely ascertained.

## Understanding Key:Value

The `Key` is of a specified basic data type, including Boolean, Number, Decimal, String, Date, or Timestamp. A `Key`'s value cannot be Null, and duplicates are not allowed. The `Value` can be any data type, including nested arrays, tuples, and so on.

Map data can be generated through `Key:Value` pairs enclosed in curly braces or by using the Map function to convert two arrays into a Map. The Map function takes two arrays as input, where the elements in the first array serve as the keys and the elements in the second array serve as the values. See an example below:

```sql
-- Input arrays: [1, 2] and ['v1', 'v2']
-- Resulting Map: {1: 'v1', 2: 'v2'}

SELECT {'k1': 1, 'k2': 2}, map([1, 2], ['v1', 'v2']);
+-----------------+---------------------------+
| {'k1':1,'k2':2} | map([1, 2], ['v1', 'v2']) |
+-----------------+---------------------------+
| {'k1':1,'k2':2} | {1:'v1',2:'v2'}           |
+-----------------+---------------------------+
```

## Map and Bloom Filter Index

In Databend Map, a bloom filter index is created for the value with certain data types: `Numeric`, `String`, `Timestamp`, and `Date`.

This makes it easier and faster to search for values in the MAP data structure.

The implementation of the bloom filter index in Databend Map is in [PR#10457](https://github.com/datafuselabs/databend/pull/10457).

The bloom filter is particularly effective in reducing query time when the queried value does not exist. 

For example:
```sql
select * from nginx_log where log['ip'] = '205.91.162.148';
+----+----------------------------------------+
| id | log                                    |
+----+----------------------------------------+
| 1  | {'ip':'205.91.162.148','url':'test-1'} |
+----+----------------------------------------+
1 row in set
Time: 1.733s
    
select * from nginx_log where log['ip'] = '205.91.162.141';
+----+-----+
| id | log |
+----+-----+
+----+-----+
0 rows in set
Time: 0.129s
```

## Examples

**Create a table with a Map column for storing web traffic data**

```sql
CREATE TABLE web_traffic_data(id INT64, traffic_info MAP(STRING, STRING));

DESC web_traffic_data;
+-------------+--------------------+------+---------+-------+
| Field       | Type               | Null | Default | Extra |
+-------------+--------------------+------+---------+-------+
| id          | INT64              | NO   |         |       |
| traffic_info| MAP(STRING, STRING)| NO   | {}      |       |
+-------------+--------------------+------+---------+-------+
```

**Insert Map data containing IP addresses and URLs visited**

```sql
INSERT INTO web_traffic_data VALUES(1, {'ip': '192.168.1.1', 'url': 'example.com/home'}),
(2, {'ip': '192.168.1.2', 'url': 'example.com/about'}),
(3, {'ip': '192.168.1.1', 'url': 'example.com/contact'});
```

**Query**

```sql
SELECT * FROM web_traffic_data;

+----+-----------------------------------+
| id | traffic_info                      |
+----+-----------------------------------+
| 1  | {'ip':'192.168.1.1','url':'example.com/home'}    |
| 2  | {'ip':'192.168.1.2','url':'example.com/about'}   |
| 3  | {'ip':'192.168.1.1','url':'example.com/contact'} |
+----+-----------------------------------+
```

**Query the number of visits per IP address**

```sql
SELECT traffic_info['ip'] as ip_address, COUNT(*) as visits
FROM web_traffic_data
GROUP BY traffic_info['ip'];

+-------------+--------+
| ip_address  | visits |
+-------------+--------+
| 192.168.1.1 |      2 |
| 192.168.1.2 |      1 |
+-------------+--------+
```

**Query the most visited URLs**
```sql
SELECT traffic_info['url'] as url, COUNT(*) as visits
FROM web_traffic_data
GROUP BY traffic_info['url']
ORDER BY visits DESC
LIMIT 3;

+---------------------+--------+
| url                 | visits |
+---------------------+--------+
| example.com/home    |      1 |
| example.com/about   |      1 |
| example.com/contact |      1 |
+---------------------+--------+
```
