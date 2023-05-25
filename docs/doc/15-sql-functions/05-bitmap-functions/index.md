---
title: Bitmap Functions
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.45"/>

| Function                         | Description                                                                    | Example                                                            | Result    |
|----------------------------------|--------------------------------------------------------------------------------|--------------------------------------------------------------------|-----------|
| bitmap_contains(bitmap, value)   | Checks if the bitmap contains a specific value.                                | bitmap_contains(build_bitmap([1,4,5]), 1)                          | 1         |
| bitmap_has_all(bitmap1, bitmap2) | Checks if the first bitmap contains all the bits in the second bitmap.         | bitmap_has_all(build_bitmap([1,4,5]), build_bitmap([1,2]))         | 0         |
| bitmap_has_any(bitmap1, bitmap2) | Checks if the first bitmap has any bit matching the bits in the second bitmap. | bitmap_has_any(build_bitmap([1,4,5]), build_bitmap([1,2]))         | 1         |
| bitmap_max(bitmap)               | Gets the maximum value in the bitmap.                                          | bitmap_max(build_bitmap([1,4,5]))                                  | 5         |
| bitmap_min(bitmap)               | Gets the minimum value in the bitmap.                                          | bitmap_min(build_bitmap([1,4,5]))                                  | 1         |
| bitmap_or(bitmap1, bitmap2)      | Performs a bitwise OR operation on the two bitmaps.                            | bitmap_or(build_bitmap([1,4,5]), build_bitmap([6,7]))::String      | 1,4,5,6,7 |
| bitmap_and(bitmap1, bitmap2)     | Performs a bitwise AND operation on the two bitmaps.                           | bitmap_and(build_bitmap([1,4,5]), build_bitmap([4,5]))::String     | 4,5       |
| bitmap_xor(bitmap1, bitmap2)     | Performs a bitwise XOR (exclusive OR) operation on the two bitmaps.            | bitmap_xor(build_bitmap([1,4,5]), build_bitmap([5,6,7]))::String   | 1,4,6,7   |
| bitmap_not(bitmap1, bitmap2)     | Performs a bitwise NOT operation on the bitmap with respect to another bitmap. | bitmap_not(build_bitmap([2,3]), build_bitmap([2,3,5]))::String     | (empty)   |
| bitmap_and_not(bitmap1, bitmap2) | Performs a bitwise AND-NOT operation on the two bitmaps.                       | bitmap_and_not(build_bitmap([2,3]), build_bitmap([2,3,5]))::String | (empty)   |