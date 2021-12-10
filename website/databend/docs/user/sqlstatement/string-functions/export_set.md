---
title: EXPORT_SET
---

Returns a string such that for every bit set in the value bits,
you get an on string and for every bit not set in the value, you get an off string.
Bits in bits are examined from right to left (from low-order to high-order bits).
Strings are added to the result from left to right, separated by the separator string (the default being the comma character ,).
The number of bits examined is given by number_of_bits, which has a default of 64 if not specified.
Number_of_bits is silently clipped to 64 if larger than 64.

## Syntax

```sql
EXPORT_SET(bits,on,off[,separator[,number_of_bits]])
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| bits | The const integer. |
| on | The expression string. |
| off | The expression string. |
| separator | The const string. |
| number_of_bits | The const integer. |

## Return Type

A string data type value.

## Examples

```txt
SELECT EXPORT_SET(5,'Y','N',',',4);
+---------------------------------+
| EXPORT_SET(5, 'Y', 'N', ',', 4) |
+---------------------------------+
| Y,N,Y,N                         |
+---------------------------------+

SELECT EXPORT_SET(6,'1','0',',',10);
+----------------------------------+
| EXPORT_SET(6, '1', '0', ',', 10) |
+----------------------------------+
| 0,1,1,0,0,0,0,0,0,0              |
+----------------------------------+
```
