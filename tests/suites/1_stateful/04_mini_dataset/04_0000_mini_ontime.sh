#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists ontime_mini;" | $BENDSQL_CLIENT_CONNECT
## Create table
cat $TESTS_DATA_DIR/ddl/ontime.sql | sed 's/ontime/ontime_mini/g' | $BENDSQL_CLIENT_CONNECT

ontime_statements=(
  ## Load data
  "COPY INTO ontime_mini FROM 'https://ci.databend.org/dataset/stateful/ontime_2006_100000.csv' FILE_FORMAT = ( type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1 );"
  ## run test
  "SELECT DayOfWeek, count(*) AS c FROM ontime_mini WHERE (Year >= 2000) AND (Year <= 2008) GROUP BY DayOfWeek ORDER BY c DESC;"
  "SELECT DayOfWeek, count(*) AS c FROM ontime_mini WHERE (DepDelay > 10) AND (Year >= 2000) AND (Year <= 2008) GROUP BY DayOfWeek ORDER BY c DESC;"
  "SELECT Origin, count(*) AS c FROM ontime_mini WHERE (DepDelay > 10) AND (Year >= 2000) AND (Year <= 2008) GROUP BY Origin ORDER BY c DESC LIMIT 10;"
  "SELECT IATA_CODE_Reporting_Airline AS Carrier, count() FROM ontime_mini WHERE (DepDelay > 10) AND (Year = 2007) GROUP BY Carrier ORDER BY count() DESC;"
  "SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(CAST(DepDelay > 10, Int8)) * 1000 AS c3 FROM ontime_mini WHERE Year = 2007 GROUP BY Carrier ORDER BY c3 DESC;"
  "SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(CAST(DepDelay > 10, Int8)) * 1000 AS c3 FROM ontime_mini WHERE (Year >= 2000) AND (Year <= 2008) GROUP BY Carrier ORDER BY c3 DESC;"
  "SELECT IATA_CODE_Reporting_Airline AS Carrier, avg(DepDelay) * 1000 AS c3 FROM ontime_mini WHERE (Year >= 2000) AND (Year <= 2008) GROUP BY Carrier ORDER BY Carrier;"
  "SELECT Year, avg(DepDelay) FROM ontime_mini GROUP BY Year;"
  "SELECT avg(c1) FROM ( SELECT Year, Month, count(*) AS c1 FROM ontime_mini GROUP BY Year, Month ) AS a;"
  "SELECT sum(Year), count(Month) FROM ( SELECT Year, Month FROM ontime_mini GROUP BY Year, Month ) AS a;"
  "SELECT OriginCityName, DestCityName, count(*) AS c FROM ontime_mini GROUP BY OriginCityName, DestCityName ORDER BY c DESC LIMIT 10;"
)

for i in "${ontime_statements[@]}"; do
  echo "$i" | $BENDSQL_CLIENT_CONNECT
done

## Clean table
echo "drop table if exists ontime_mini all;" | $BENDSQL_CLIENT_CONNECT
