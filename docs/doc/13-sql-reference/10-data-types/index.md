---
title: Data Types
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.100"/>

This page explains various aspects of data types, including lists of data types, data type conversions, casting methods, and the handling of NULL values and NOT NULL constraints.

## List of Data Types

The following is a list of general data types in Databend:

| Data Type                                                           | Alias  | Storage Size | Min Value                | Max Value                      | 
|---------------------------------------------------------------------|--------|--------------|--------------------------|--------------------------------|
| [BOOLEAN](./00-data-type-logical-types.md)                          | BOOL   | 1 byte       | N/A                      | N/A                            |
| [TINYINT](./10-data-type-numeric-types.md#integer-data-types)       | INT8   | 1 byte       | -128                     | 127                            |
| [SMALLINT](./10-data-type-numeric-types.md#integer-data-types)      | INT16  | 2 bytes      | -32768                   | 32767                          |
| [INT](./10-data-type-numeric-types.md#integer-data-types)           | INT32  | 4 bytes      | -2147483648              | 2147483647                     |
| [BIGINT](./10-data-type-numeric-types.md#integer-data-types)        | INT64  | 8 bytes      | -9223372036854775808     | 9223372036854775807            |
| [FLOAT](./10-data-type-numeric-types.md#floating-point-data-types)  | N/A    | 4 bytes      | -3.40282347e+38          | 3.40282347e+38                 |
| [DOUBLE](./10-data-type-numeric-types.md#floating-point-data-types) | N/A    | 8 bytes      | -1.7976931348623157E+308 | 1.7976931348623157E+308        |
| [DECIMAL](./11-data-type-decimal-types.md)                          | N/A    | 16/32 bytes  | -10^P / 10^S             | 10^P / 10^S                    |
| [DATE](./20-data-type-time-date-types.md)                           | N/A    | 4 bytes      | 1000-01-01               | 9999-12-31                     |
| [TIMESTAMP](./20-data-type-time-date-types.md)                      | N/A    | 8 bytes      | 0001-01-01 00:00:00      | 9999-12-31 23:59:59.999999 UTC |
| [VARCHAR](./30-data-type-string-types.md)                           | STRING | N/A          | N/A                      | N/A                            |

The following is a list of semi-structured data types in Databend:

| Data Type                              | Alias | Sample                           | Description                                                                       |
|----------------------------------------|-------|----------------------------------|-----------------------------------------------------------------------------------|
| [ARRAY](./40-data-type-array-types.md) | N/A   | [1, 2, 3, 4]                   | A collection of values of the same data type, accessed by their index.            |
| [TUPLE](./41-data-type-tuple-types.md) | N/A   | ('2023-02-14','Valentine Day') | An ordered collection of values of different data types, accessed by their index. |
| [MAP](./42-data-type-map.md)           | N/A   | {"a":1, "b":2, "c":3}          | A set of key-value pairs where each key is unique and maps to a value.            |                             |
| [VARIANT](./43-data-type-variant.md)   | JSON  | [1,{"a":1,"b":{"c":2}}]        | Collection of elements of different data types, including `ARRAY` and `OBJECT`.   |
| [BITMAP](44-data-type-bitmap.md)   | N/A  | 0101010101        | A binary data type used to represent a set of values, where each bit represents the presence or absence of a value.   |

## Data Type Conversions

### Explicit Castings

We have two kinds of expression to cast a value to another datatype.
1. `CAST` function, if error happens during cast, it throws error.

We also support pg casting style: `CAST(c as INT)` is same as `c::Int`

2. `TRY_CAST` function if error happens during cast, it returns NULL.


### Implicit Castings ("Coercion")

Some basic rules about "Coercion" aka (Auto casting)

1. All integer datatypes can be implicitly casted into  `BIGINT` aka (`INT64`) datatype.

e.g.
```sql
Int --> bigint
UInt8 --> bigint
Int32 --> bigint
```


2. All numeric datatypes can be implicitly cast into  `Double` aka (`Float64`) datatype.

e.g.
```sql
Int --> Double
Float --> Double
Int32 --> Double
```

3. ALL non-nullable datatypes `T` can be implicitly casted into `Nullable(T)` datatype.

e.g.
```sql
Int --> Nullable<Int>
String -->  Nullable<String>
```

4. All datatypes can be implicitly casted into `Variant` datatype.

e.g.

```sql
Int --> Variant
```

5. String datatype is the lowest datatype that can't be implicitly casted to other datatypes.
6. `Array<T>` --> `Array<U>` if `T` --> `U`.
7. `Nullable<T>` --> `Nullable<U>` if `T`--> `U`.
8. `Null` --> `Nullable<T>` for any `T` datatype.
9. Numeric can be implicitly casted to other numeric datatype if there is no lossy.

### FAQ

> Why can't the numeric type be automatically converted to the String type.

It's trivial and even works in other popular databases. But it'll introduce ambiguity.

e.g.

```sql
select 39 > '301';
select 39 = '  39  ';
```

We don't know how to compare them with numeric rules or String rules. Because they are different result according to different rules.

`select 39 > 301` is false where `select '39' > '301'` is true.

To make the syntax more precise and less ambiguous, we throw the error to user and get more precise SQL.


> Why can't the boolean type be automatically converted to the numeric type.

This will also bring ambiguity.
e.g.

```sql
select true > 0.5;
```

> What's the error message: "can't cast from nullable data into non-nullable type".

That means you got a null in your source column. You can use `TRY_CAST` function or make your target type be a nullable type.


> `select concat(1, col)` not work

You can improve the SQL to `select concat('1', col)`.

We may improve the expression in the future which could parse the literal `1` into String value if possible (the concat function just accept String parameters).

## NULL Values and NOT NULL Constraint

NULL values are employed to represent data that is either nonexistent or unknown. In Databend, every column is inherently capable of containing NULL values, which implies that a column can accommodate NULLs alongside regular data. 

If you need a column that does not allow NULL values, use the NOT NULL constraint. If a column is configured to disallow NULL values in Databend, and you do not explicitly provide a value for that column when inserting data, the default value associated with the column's data type will be automatically applied. 

| Data Type                | Default Value           |
|--------------------------|-------------------------|
| Integer Data Types       | 0                       |
| Floating-Point Data Types| 0.0                     |
| Character and String     | Empty string ('')       |
| Date and Time Data Types | '1970-01-01' for DATE, '1970-01-01 00:00:00' for TIMESTAMP |
| Boolean Data Type        | False                   |

For example, if you create a table as follows:

```sql
CREATE TABLE test(
    id Int64,
    name String NOT NULL,
    age Int32
);

DESC test;

Field|Type   |Null|Default|Extra|
-----+-------+----+-------+-----+
id   |BIGINT |YES |NULL   |     |
name |VARCHAR|NO  |''     |     |
age  |INT    |YES |NULL   |     |
```

- The "id" column can contain NULL values because it lacks the "NOT NULL" constraint. This means it can store integers or be left empty to represent missing data.

- The "name" column must always have a value due to the "NOT NULL" constraint, disallowing NULL values.

- The "age" column, like "id," can also hold NULL values as it doesn't have a "NOT NULL" constraint, allowing for empty entries or NULLs to indicate unknown ages.

The following INSERT statement inserts a row with a NULL value for the "age" column. This is allowed because the "age" column does not have a NOT NULL constraint, so it can hold NULL values to represent missing or unknown data.

```sql
INSERT INTO test (id, name, age) VALUES (2, 'Alice', NULL);
```

The following INSERT statement inserts a row into the "test" table with values for the "id" and "name" columns, without providing a value for the "age" column. This is allowed because the "age" column does not have a NOT NULL constraint, so it can be left empty or assigned a NULL value to indicate missing or unknown data.

```sql
INSERT INTO test (id, name) VALUES (1, 'John');
```

The following INSERT statement attempts to insert a row without a value for the "name" column. The default value of the column type will be applied.

```sql
INSERT INTO test (id, age) VALUES (3, 45);
```
