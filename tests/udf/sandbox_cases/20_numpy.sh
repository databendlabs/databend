echo "Creating numpy UDF"
create_numpy_udf_sql=$(
	cat <<'SQL'
CREATE OR REPLACE FUNCTION numpy_plus_one(INT)
RETURNS INT
LANGUAGE PYTHON
PACKAGES = ('numpy')
HANDLER = 'numpy_plus_one'
AS $$
import numpy as np

def numpy_plus_one(x: int) -> int:
    return int(np.add(x, 1))
$$
SQL
)
execute_query "$create_numpy_udf_sql"

echo "Executing numpy UDF"
check_result "SELECT numpy_plus_one(2) AS result" '[["3"]]' "$UDF_SETTINGS_JSON"
