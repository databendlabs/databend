---
title: 'Array Functions'
---


SQL Array Functions and Usage.

| Function                             | Description                                                                                  | Example                               | Result                   |
|--------------------------------------|----------------------------------------------------------------------------------------------|---------------------------------------|--------------------------|
| **GET(array, index)**                | Returns an element from the array by index (1-based)                                         | **GET([1, 2], 2)**                    | 2                        |
| **LENGTH(array)**                    | Returns the length of the array                                                              | **LENGTH([1, 2])**                    | 2                        |
| **RANGE(start, end)**                    | Returns an array collected by [start, end)                                                     | **RANGE(1, 3)**                    | [1, 2]                        |
| **ARRAY_CONCAT(array1, array2)**     | Concats two arrays                                                                           | **ARRAY_CONCAT([1, 2], [3, 4]**       | [1,2,3,4]                |
| **ARRAY_CONTAINS(array, item)**      | Checks if the array contains a specific element                                              | **ARRAY_CONTAINS([1, 2], 1)**         | 1                        |
| **ARRAY_INDEXOF(array, item)**       | Returns the index(1-based) of an element if the array contains the element                   | **ARRAY_INDEXOF([1, 2, 9], 9)**       | 3                        |
| **ARRAY_SLICE(array, start[, end])** | Extracts a slice from the array by index (1-based)                                           | **ARRAY_SLICE([1, 21, 32, 4], 2, 3)** | [21,32]                  |
| **ARRAY_SORT(array)**                | Sorts elements in the array in ascending order                                               | **ARRAY_SORT([1, 4, 3, 2])**          | [1,2,3,4]                |
| **ARRAY_AGGREGATE(array, name)**     | Aggregates elements in the array with an aggregate function (sum, count, avg, min, max, any, ...) | **ARRAY_AGGREGATE([1, 2, 3, 4], 'SUM')**  | 10                       |
| **ARRAY_REDUCE(array, name)**        | Alias for **ARRAY_AGGREGATE**                                                  | **ARRAY_REDUCE([1, 2, 3, 4], 'SUM')**  | 10                       |
| **ARRAY_UNIQUE(array)**              | Counts unique elements in the array (except NULL)                                            | **ARRAY_UNIQUE([1, 2, 3, 3, 4])**     | 4                        |
| **ARRAY_DISTINCT(array)**            | Removes all duplicates and NULLs from the array without preserving the original order        | **ARRAY_DISTINCT([1, 2, 2, 4])**      | [1,2,4]                  |
| **ARRAY_PREPEND(item, array)**       | Prepends an element to the array                                                             | **ARRAY_PREPEND(1, [3, 4])**          | [1,3,4]                  |
| **ARRAY_APPEND(array, item)**        | Appends an element to the array                                                              | **ARRAY_APPEND([3, 4], 5)**           | [3,4,5]                  |
| **ARRAY_REMOVE_FIRST(array)**        | Removes the first element from the array                                                     | **ARRAY_REMOVE_FIRST([1, 2, 3])**     | [2,3]                    |
| **ARRAY_REMOVE_LAST(array)**         | Removes the last element from the array                                                      | **ARRAY_REMOVE_LAST([1, 2, 3])**      | [1,2]                    |
| **UNNEST(array)**                    | Unnests the array and returns the set of elements                                            | **UNNEST([1, 2])**                    | 1<br/>2<br/>**(2 rows)** |
| **ARRAY_TRANSFORM(array, lambda)**   | Returns an array that is the result of applying the lambda function to each element of the input array     | **ARRAY_TRANSFORM([1, 2, 3], x -> x + 1)**      | [2,3,4]                   |
| **ARRAY_APPLY(array, lambda)**       | Alias for **ARRAY_TRANSFORM**                   | **ARRAY_APPLY([1, 2, 3], x -> x + 1)**      | [2,3,4]                   |
| **ARRAY_FILTER(array, lambda)**      | Constructs an array from those elements of the input array for which the lambda function returns true     | **ARRAY_FILTER([1, 2, 3], x -> x > 1)**      | [2,3]                   |

:::note
**ARRAY_SORT(array)** can accept two optional parameters, `order` and `nullposition`, which can be specified through the syntax **ARRAY_SORT(array, order, nullposition)**.
   - `order` specifies the sorting order as either ascending (ASC) or descending (DESC). Defaults to ASC.
   - `nullposition` determines the position of NULL values in the sorting result, at the beginning (NULLS FIRST) or at the end (NULLS LAST) of the sorting output. Defaults to NULLS FIRST.
:::

:::note
**ARRAY_AGGREGATE(array, name)** supports the following aggregation functions, `avg`, `count`, `max`, `min`, `sum`, `any`, `stddev_samp`, `stddev_pop`, `stddev`, `std`, `median`, `approx_count_distinct`, `kurtosis`, `skewness`.

**ARRAY_AGGREGATE(array, name)** function also support rewrite as **ARRAY_<name\>(array)**. Following is a list of existing rewrites, `array_avg`, `array_count`, `array_max`, `array_min`, `array_sum`, `array_any`, `array_stddev_samp`, `array_stddev_pop`, `array_stddev`, `array_std`, `array_median`, `array_approx_count_distinct`, `array_kurtosis`, `array_skewness`.
:::

:::note
**UNNEST(array)** can also be used as a table function.
:::

:::note
Lambda function consists of a parameter and a lambda expression, separated by `->` operator.
Only scalar functions are supported for lambda expressions. Aggregate functions, window functions, table functions, and subqueries are not supported.
:::
