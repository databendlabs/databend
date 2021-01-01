---
id: query-operator
title: Query Operators
---

DataFuse supports most of the standard operators defined in SQL:1999.

These operators include arithmetic operators, comparison operators, logical operators, etc.


| Category | Operators | Example |
|--------- | --------- | ------- |
| Arithmetic Operators | + , - , * , / , % | SELECT number+1 FROM system.numbers_mt(1)  |
| Comparison Operators | = , != , < , <= , > , >= | SELECT number FROM system.numbers_mt WHERE number> 9900 |
| Logical Operators | AND , NOT , OR | SELECT number FROM system.numbers_mt WHERE number> 1 AND number <= 5 |



