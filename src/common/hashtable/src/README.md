# Hashtable implementation in rust


This package is a port from its cpp implementation from [ClickHouse](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/HashTable.h).
It is quite difficult to implement a high-performance hashtable from scratch. After comparing many implementations of hashtables, we found that ClickHouse's hashtable is the most suitable for OLAP systems. Therefore, we rewrote it into this Rust library.
