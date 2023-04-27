---
id: string-soundex
title: SOUNDEX
---

Generates the Soundex code for a string.

- A Soundex code consists of a letter followed by three numerical digits. Databend's implementation returns more than 4 digits, but you can [SUBSTR](substr.md) the result to get a standard Soundex code.
- All non-alphabetic characters in the string are ignored.
- All international alphabetic characters outside the A-Z range are ignored unless they're the first letter.


:::tip What is Soundex?
Soundex converts an alphanumeric string to a four-character code that is based on how the string sounds when spoken in English. For more information, see https://en.wikipedia.org/wiki/Soundex
:::

See also: [SOUNDS LIKE](soundslike.md)

## Syntax

```sql
SOUNDEX(<str>)
```

## Arguments

| Arguments | Description |
|-----------|-------------|
| str  | The string. |

## Return Type

Returns a code of type VARCHAR or a NULL value.

## Examples

```sql
SELECT SOUNDEX('Databend');

---
D153

-- All non-alphabetic characters in the string are ignored.
SELECT SOUNDEX('Databend!');;

---
D153

-- All international alphabetic characters outside the A-Z range are ignored unless they're the first letter.
SELECT SOUNDEX('Databend，你好');

---
D153

SELECT SOUNDEX('你好，Databend');

---
你3153

-- SUBSTR the result to get a standard Soundex code.
SELECT SOUNDEX('Databend Cloud'),SUBSTR(SOUNDEX('Databend Cloud'),1,4);

soundex('databend cloud')|substring(soundex('databend cloud') from 1 for 4)|
-------------------------+-------------------------------------------------+
D153243                  |D153                                             |

SELECT SOUNDEX(NULL);
+-------------------------------------+
| `SOUNDEX(NULL)`                     |
+-------------------------------------+
| <null>                              |
+-------------------------------------+
```