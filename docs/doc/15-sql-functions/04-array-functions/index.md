---
title: 'Array Functions'
---

| Function                             | Description                                                                            | Example                               | Result    |
|--------------------------------------|----------------------------------------------------------------------------------------|---------------------------------------|-----------|
| **get(array, index)**                | Returns an element from the array by index                                           | **get([1, 2], 2)**                    | 2         |
| **length(array)**                    | Returns the length of the array                                                          | **length([1, 2])**                    | 2         |
| **array_concat(array1, array2)**     | Concats two arrays                                                                      | **array_concat([1, 2], [3, 4]**       | [1,2,3,4] |
| **array_contains(array, item)**      | Checks if the array contains a specific element                                                           | **array_contains([1, 2], 1)**         | 1         |
| **array_indexof(array, item)**       | Returns the index of an element if the array contains the element         | **array_indexof([1, 2, 9], 9)**       | 3         |
| **array_slice(array, start[, end])** | Extracts a slice from the array                          | **array_slice([1, 21, 32, 4], 2, 3)** | [21,32]   |
| **array_sort(array)**                | Sorts elements in the array in ascending order                                                                        | **array_sort([1, 4, 3, 2])**          | [1,2,3,4] |
| **array_<aggr\>(array)**            | Aggregates elements in the array with an aggregate function (sum, count, avg, min, max, any) | **array_sum([1, 2, 3, 4]**            | 10        |
| **array_unique(array)**              | Counts unique elements in the array (except NULL)                                 | **array_unique([1, 2, 3, 3, 4])**     | 4         |
| **array_distinct(array)**            | Removes all duplicates and NULLs from the array without preserving the original order  | **array_distinct([1, 2, 2, 4])**      | [1,2,4]   |
| **array_prepend(item, array)**       | Prepends an element to the array                                                             | **array_prepend(1, [3, 4])**          | [1,3,4]   |
| **array_append(array, item)**        | Appends an element to the array                                                              | **array_append([3, 4], 5)**           | [3,4,5]   |
| **array_remove_first(array)**        | Removes the first element from the array                                                 | **array_remove_first([1, 2, 3])**     | [2,3]     |
| **array_remove_last(array)**         | Removes the last element from the array                                                  | **array_remove_last([1, 2, 3])**      | [1,2]     |

:::note
- Databend indexes elements in an array starting from `1`.
- **array_sort(array)** can accept two optional parameters, `order` and `nullposition`, which can be specified through the syntax **array_sort(array, order, nullposition)**.
    - `order` specifies the sorting order as either ascending (ASC) or descending (DESC). Defaults to ASC.
    - `nullposition` determines the position of NULL values in the sorting result, at the beginning (NULLS FIRST) or at the end (NULLS LAST) of the sorting output. Defaults to NULLS FIRST.
:::