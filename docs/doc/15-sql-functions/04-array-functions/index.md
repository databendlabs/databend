---
title: 'Array Functions'
---

Array Functions

## Syntax

| Function    | Description             |  Example | Result
| ----------- | ------------------ |  ---------- | --------|
| `get(array, index)`  | Get item from an array by index(**1-based**)     |  `get([1,2], 2)`         |  2
| `length(array)`  | Output the length of an array    |  `length([1,2])`         |  2
| `array_concat(array1, array2)`  | Concat two arrays     |  `array_concat([1,2], [3, 4]`         |  [1,2,3,4]
| `array_contains(array, item)`  | Check if array has some item     |  `array_contains([1,2], 1)`         |  1
| `array_indexof(array, item)`  | Returns the index(**1-based**) of the element if the list contains the element.	list_contains([1, 2, NULL],      |  `array_indexof([1,2,9], 9);`         |  3
| `array_slice(array, start[, end])`  | Extract a sublist using slice conventions(index **1-based**)     |  `array_slice([1,21,32,4], 2, 3)`         |  `[21, 32]`
| `array_sort(array)`  | Sort an array     |  `array_sort([1,4,3,2])`         |  `[1,2,3,4]`
| `array_<aggr>(array)`  | Aggregate the array using aggr name(currently support: sum, count, avg, min, max, any)     |  `array_sum([1,2,3,4]`         |  10
| `array_unique(array)`  | Counts the unique elements of an array other than NULL |  `array_unique([1,2,3,3,4])`         |  4
| `array_distinct(array)`  | Removes all duplicates and NULLs from an array. Does not preserve the original order.      |  `array_distinct([1,2,2,4])`         |  [1,2,4]
| `array_prepend(item, array)`  | Prepend item into an array      |  `array_prepend(1, [3, 4])`         |  [1,3,4]
| `array_append(array, item)`  | Append item into an array      |  `array_append([3, 4], 5)`         |  [3,4,5]
| `array_remove_first(array)`  | Remove the first element from an array      |  `array_remove_first([1,2,3])`         |  [2,3]
| `array_remove_last(array)`  | Remove the last element from an array      |  `array_remove_last([1,2,3])`         |  [1,2]


Note that `array_sort` function have two additional optional arguments.
The second optional argument is the sort order, ASC and DESC are supported, the default value is ASC.
The third optional argument is the position of the NULL value, NULLS FIRST and NULLS LAST are supported, the default is NULLS FIRST.
