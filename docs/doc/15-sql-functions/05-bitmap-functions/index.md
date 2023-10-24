---
title: Bitmap Functions
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.2.26"/>

| Function                                   	| Description                                                                                                  	| Example                                                            	| Result    	|
|--------------------------------------------	|--------------------------------------------------------------------------------------------------------------	|--------------------------------------------------------------------	|-----------	|
| BITMAP_COUNT(bitmap)                       	| Counts the number of bits set to 1 in the bitmap.                                                            	| bitmap_count(build_bitmap([1,4,5]))                                	| 3         	|
| BITMAP_CONTAINS(bitmap, value)             	| Checks if the bitmap contains a specific value.                                                              	| bitmap_contains(build_bitmap([1,4,5]), 1)                          	| 1         	|
| BITMAP_HAS_ALL(bitmap1, bitmap2)           	| Checks if the first bitmap contains all the bits in the second bitmap.                                       	| bitmap_has_all(build_bitmap([1,4,5]), build_bitmap([1,2]))         	| 0         	|
| BITMAP_HAS_ANY(bitmap1, bitmap2)           	| Checks if the first bitmap has any bit matching the bits in the second bitmap.                               	| bitmap_has_any(build_bitmap([1,4,5]), build_bitmap([1,2]))         	| 1         	|
| BITMAP_MAX(bitmap)                         	| Gets the maximum value in the bitmap.                                                                        	| bitmap_max(build_bitmap([1,4,5]))                                  	| 5         	|
| BITMAP_MIN(bitmap)                         	| Gets the minimum value in the bitmap.                                                                        	| bitmap_min(build_bitmap([1,4,5]))                                  	| 1         	|
| BITMAP_OR(bitmap1, bitmap2)                	| Performs a bitwise OR operation on the two bitmaps.                                                          	| bitmap_or(build_bitmap([1,4,5]), build_bitmap([6,7]))::String      	| 1,4,5,6,7 	|
| BITMAP_AND(bitmap1, bitmap2)               	| Performs a bitwise AND operation on the two bitmaps.                                                         	| bitmap_and(build_bitmap([1,4,5]), build_bitmap([4,5]))::String     	| 4,5       	|
| BITMAP_XOR(bitmap1, bitmap2)               	| Performs a bitwise XOR (exclusive OR) operation on the two bitmaps.                                          	| bitmap_xor(build_bitmap([1,4,5]), build_bitmap([5,6,7]))::String   	| 1,4,6,7   	|
| BITMAP_NOT(bitmap1, bitmap2)               	| Alias for "bitmap_and_not(bitmap1, bitmap2)".                            	| bitmap_count(bitmap_not(build_bitmap([2,3,9]), build_bitmap([2,3,5])))     	| 1  	|
| BITMAP_INTERSECT(bitmap)                      | Counts the number of bits set to 1 in the bitmap by performing a logical INTERSECT operation.                 | bitmap_intersect(to_bitmap('1, 3, 5'))::String                     | 1, 3, 5        	|
| BITMAP_UNION(bitmap)                          | Counts the number of bits set to 1 in the bitmap by performing a logical UNION operation.                     | bitmap_union(to_bitmap('1, 3, 5'))::String                     | 1, 3, 5        	|
| BITMAP_AND_NOT(bitmap1, bitmap2)           	| Generates a new bitmap with elements from the first bitmap (bitmap1) that are not in the second (bitmap2).                                                     	| bitmap_count(bitmap_and_not(build_bitmap([2,3,9]), build_bitmap([2,3,5]))) 	| 1   	|
| BITMAP_SUBSET_LIMIT(bitmap, start, limit)  	| Generates a sub-bitmap of the source bitmap, beginning with a range from the start value, with a size limit. 	| bitmap_subset_limit(build_bitmap([1,4,5]), 2, 2)::String           	| 4,5       	|
| BITMAP_SUBSET_IN_RANGE(bitmap, start, end) 	| Generates a sub-bitmap of the source bitmap within a specified range.                                        	| bitmap_subset_in_range(build_bitmap([5,7,9]), 6, 9)::String        	| 7         	|
| SUB_BITMAP(bitmap, start, size)            	| Generates a sub-bitmap of the source bitmap, beginning from the start index, with a specified size.          	| sub_bitmap(build_bitmap([1, 2, 3, 4, 5]), 1, 3)::String            	| 2,3,4     	|
| BITMAP_AND_COUNT(bitmap)                   	| Counts the number of bits set to 1 in the bitmap by performing a logical AND operation.                      	| bitmap_and_count(to_bitmap('1, 3, 5'))                             	| 3         	|
| BITMAP_NOT_COUNT(bitmap)                      | Counts the number of bits set to 0 in the bitmap by performing a logical NOT operation.                       | bitmap_not_count(to_bitmap('1, 3, 5'))                                |  3            |
| BITMAP_OR_COUNT(bitmap)                    	| Counts the number of bits set to 1 in the bitmap by performing a logical OR operation.                       	| bitmap_or_count(to_bitmap('1, 3, 5'))                              	| 3         	|
| BITMAP_XOR_COUNT(bitmap)                   	| Counts the number of bits set to 1 in the bitmap by performing a logical XOR (exclusive OR) operation.       	| bitmap_xor_count(to_bitmap('1, 3, 5'))                             	| 3         	|
| INTERSECT_COUNT('bitmap_value1', 'bitmap_value2')(bitmap_column1, bitmap_column2) | Counts the number of intersecting bits between two bitmap columns.   | intersect_count('a', 'c')(v, tag) from agg_bitmap_test | 1 |
