#!/usr/bin/env bash

set -euo pipefail

OUT_DIR="${OUT_DIR:-target/geometry-snowflake-compare}"
SNOWSQL_BIN="${SNOWSQL_BIN:-snowsql}"
SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-hbsuaxo-gw46180}"
SNOWFLAKE_USER="${SNOWFLAKE_USER:-SNOW1030}"

mkdir -p "${OUT_DIR}"

snowflake_sql="${OUT_DIR}/snowflake_geometry_compare.sql"
snowflake_geography_only_sql="${OUT_DIR}/snowflake_geography_only_compare.sql"
databend_sql="${OUT_DIR}/databend_geometry_compare.sql"
snowflake_out="${OUT_DIR}/snowflake_geometry_compare.csv"
snowflake_geography_only_out="${OUT_DIR}/snowflake_geography_only_compare.csv"

cat >"${snowflake_sql}" <<'SQL'
SELECT 'st_simplify_linestring' AS case_id,
       ST_ASWKT(ST_SIMPLIFY(TO_GEOMETRY('LINESTRING(0 0, 1 0, 1 1, 2 1)'), 0.5)) AS value
UNION ALL
SELECT 'st_simplify_polygon',
       ST_ASWKT(ST_SIMPLIFY(TO_GEOMETRY('POLYGON((0 0, 1 0, 1 1, 0.5 0.5, 0 1, 0 0))'), 0.6))
UNION ALL
SELECT 'st_isvalid_polygon',
       CAST(ST_ISVALID(TO_GEOMETRY('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) AS STRING)
UNION ALL
SELECT 'st_covers_point_inside',
       CAST(ST_COVERS(TO_GEOMETRY('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))'), TO_GEOMETRY('POINT(1 1)')) AS STRING)
UNION ALL
SELECT 'st_covers_point_outside',
       CAST(ST_COVERS(TO_GEOMETRY('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))'), TO_GEOMETRY('POINT(5 5)')) AS STRING)
UNION ALL
SELECT 'st_coveredby_point_inside',
       CAST(ST_COVEREDBY(TO_GEOMETRY('POINT(1 1)'), TO_GEOMETRY('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))')) AS STRING)
UNION ALL
SELECT 'st_perimeter_rectangle',
       CAST(ST_PERIMETER(TO_GEOMETRY('POLYGON((0 0, 0 3, 4 3, 4 0, 0 0))')) AS STRING)
UNION ALL
SELECT 'st_azimuth_east',
       CAST(ST_AZIMUTH(TO_GEOMETRY('POINT(0 0)'), TO_GEOMETRY('POINT(1 0)')) AS STRING)
UNION ALL
SELECT 'st_azimuth_northeast',
       CAST(ST_AZIMUTH(TO_GEOMETRY('POINT(0 0)'), TO_GEOMETRY('POINT(1 1)')) AS STRING)
UNION ALL
SELECT 'st_buffer_point_non_null',
       CAST(ST_BUFFER(TO_GEOMETRY('POINT(0 0)'), 1) IS NOT NULL AS STRING)
ORDER BY case_id;
SQL

cat >"${snowflake_geography_only_sql}" <<'SQL'
-- Snowflake exposes ST_HAUSDORFFDISTANCE as GEOGRAPHY only. Databend currently
-- implements the function added in issue #19821 for GEOMETRY, so do not diff
-- this row directly against Databend GEOMETRY results.
SELECT 'snowflake_geography_only_st_hausdorffdistance' AS case_id,
       CAST(ST_HAUSDORFFDISTANCE(TO_GEOGRAPHY('POINT(0 0)'), TO_GEOGRAPHY('POINT(0 1)')) AS STRING)
SQL

cat >"${databend_sql}" <<'SQL'
SELECT 'st_simplify_linestring' AS case_id,
       ST_ASWKT(ST_SIMPLIFY(TO_GEOMETRY('LINESTRING(0 0, 1 0, 1 1, 2 1)'), 0.5)) AS value
UNION ALL
SELECT 'st_simplify_polygon',
       ST_ASWKT(ST_SIMPLIFY(TO_GEOMETRY('POLYGON((0 0, 1 0, 1 1, 0.5 0.5, 0 1, 0 0))'), 0.6))
UNION ALL
SELECT 'st_isvalid_polygon',
       CAST(ST_ISVALID(TO_GEOMETRY('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')) AS STRING)
UNION ALL
SELECT 'st_covers_point_inside',
       CAST(ST_COVERS(TO_GEOMETRY('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))'), TO_GEOMETRY('POINT(1 1)')) AS STRING)
UNION ALL
SELECT 'st_covers_point_outside',
       CAST(ST_COVERS(TO_GEOMETRY('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))'), TO_GEOMETRY('POINT(5 5)')) AS STRING)
UNION ALL
SELECT 'st_coveredby_point_inside',
       CAST(ST_COVEREDBY(TO_GEOMETRY('POINT(1 1)'), TO_GEOMETRY('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))')) AS STRING)
UNION ALL
SELECT 'st_perimeter_rectangle',
       CAST(ST_PERIMETER(TO_GEOMETRY('POLYGON((0 0, 0 3, 4 3, 4 0, 0 0))')) AS STRING)
UNION ALL
SELECT 'st_azimuth_east',
       CAST(ST_AZIMUTH(TO_GEOMETRY('POINT(0 0)'), TO_GEOMETRY('POINT(1 0)')) AS STRING)
UNION ALL
SELECT 'st_azimuth_northeast',
       CAST(ST_AZIMUTH(TO_GEOMETRY('POINT(0 0)'), TO_GEOMETRY('POINT(1 1)')) AS STRING)
UNION ALL
SELECT 'st_buffer_point_non_null',
       CAST(ST_BUFFER(TO_GEOMETRY('POINT(0 0)'), 1) IS NOT NULL AS STRING)
ORDER BY case_id;
SQL

snow_args=(
    -a "${SNOWFLAKE_ACCOUNT}"
    -u "${SNOWFLAKE_USER}"
    -f "${snowflake_sql}"
    -o output_format=csv
    -o header=true
    -o timing=false
    -o friendly=false
)

if [[ -n "${SNOWFLAKE_WAREHOUSE:-}" ]]; then
    snow_args+=(-w "${SNOWFLAKE_WAREHOUSE}")
fi
if [[ -n "${SNOWFLAKE_DATABASE:-}" ]]; then
    snow_args+=(-d "${SNOWFLAKE_DATABASE}")
fi
if [[ -n "${SNOWFLAKE_SCHEMA:-}" ]]; then
    snow_args+=(-s "${SNOWFLAKE_SCHEMA}")
fi
if [[ -n "${SNOWFLAKE_ROLE:-}" ]]; then
    snow_args+=(-r "${SNOWFLAKE_ROLE}")
fi

"${SNOWSQL_BIN}" "${snow_args[@]}" >"${snowflake_out}"
snow_geography_only_args=("${snow_args[@]}")
snow_geography_only_args[5]="${snowflake_geography_only_sql}"
"${SNOWSQL_BIN}" "${snow_geography_only_args[@]}" >"${snowflake_geography_only_out}"

echo "Snowflake output: ${snowflake_out}"
echo "Snowflake GEOGRAPHY-only output: ${snowflake_geography_only_out}"
echo "Databend SQL: ${databend_sql}"
echo "Run the Databend SQL with your local bendsql/databend-sqllogictests setup and diff case_id/value rows."
