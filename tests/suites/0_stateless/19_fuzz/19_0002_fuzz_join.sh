#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

## TODO after except join get correct result

## fuzz join
## ideas
# The logical relationships of JOIN:

# 1. The relationship between LEFT JOIN and RIGHT JOIN:
#    - All rows in LEFT JOIN should appear in FULL OUTER JOIN.
#    - All rows in RIGHT JOIN should also appear in FULL OUTER JOIN.
#    - FULL OUTER JOIN is the union of LEFT JOIN and RIGHT JOIN.

# 2. The complementarity of LEFT JOIN and RIGHT JOIN:
#    - If certain rows in the result of LEFT JOIN have NULLs in the right table, these rows should have no corresponding entries in RIGHT JOIN.
#    - Similarly, if certain rows in the result of RIGHT JOIN have NULLs in the left table, these rows should have no corresponding entries in LEFT JOIN.

# 3. Verifying the decomposability of FULL OUTER JOIN:
#    - FULL OUTER JOIN = LEFT JOIN + RIGHT JOIN - overlapping parts.
