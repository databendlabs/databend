---
title: DataType conversion
description: DataType conversion
---

## Explicit Casting

We have two kinds of expression to cast a value to another datatype.
1. `CAST` function, if error happens during cast, it throws error.

We also support pg casting style: `CAST(c as INT)` is same as `c::Int`

2. `TRY_CAST` function if error happens during cast, it returns NULL.


## Implicit Casting ("Coercion")

Some basic rules about "Coercion" aka (Auto casting)

1. All integer datatypes can be implicitly casted into  `BIGINT` aka (`INT64`) datatype.

e.g.
```
Int --> bigint
UInt8 --> bigint
Int32 --> bigint
```


2. All numeric datatypes can be implicitly cast into  `Double` aka (`Float64`) datatype.

e.g.
```
Int --> Double
Float --> Double
Int32 --> Double
```

3. ALL non-nullable datatypes `T` can be implicitly casted into `Nullable(T)` datatype.

e.g.
```
Int --> Nullable<Int>
String -->  Nullable<String>
```

4. All datatypes can be implicitly casted into `Variant` datatype.

e.g.

```
Int --> Variant
```

5. String datatype is the lowest datatype that can't be implicitly casted to other datatypes.
6. `Array<T>` --> `Array<U>` if `T` --> `U`.
7. `Nullable<T>` --> `Nullable<U>` if `T`--> `U`.
8. `Null` --> `Nullable<T>` for any `T` datatype.
9. Numeric can be implicitly casted to other numeric datatype if there is no lossy.


## FAQ

> Why can't the numeric type be automatically converted to the String type.

It's trivial and even works in other popular databases. But it'll introduce ambiguity.

e.g.

```
select 39 > '301';
select 39 = '  39  ';
```

We don't know how to compare them with numeric rules or String rules. Because they are different result according to different rules.

`select 39 > 301` is false where `select '39' > '301'` is true.

To make the syntax more precise and less ambiguous, we throw the error to user and get more precise SQL.


> Why can't the boolean type be automatically converted to the numeric type.

This will also bring ambiguity.
e.g.

```
select true > 0.5;
```

> What's the error message: "can't cast from nullable data into non-nullable type".

That means you got a null in your source column. You can use `TRY_CAST` function or make your target type be a nullable type.


> `select concat(1, col)` not work

You can improve the SQL to `select concat('1', col)`.

We may improve the expression in the future which could parse the literal `1` into String value if possible (the concat function just accept String parameters).
