#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists hits;" | $BENDSQL_CLIENT_CONNECT
## Create table
cat $TESTS_DATA_DIR/ddl/hits.sql | $BENDSQL_CLIENT_CONNECT

hits_statements=(
  ## load data
  "COPY INTO hits FROM 'https://ci.databend.org/dataset/stateful/hits_100k.tsv' FILE_FORMAT = ( type = 'tsv' record_delimiter = '\n' skip_header = 1 );"
  ## run test
  "SELECT '====== SQL1 ======';"
  "SELECT COUNT(*) FROM hits;"
  "SELECT '====== SQL2 ======';"
  "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;"
  "SELECT '====== SQL3 ======';"
  "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;"
  # "SELECT '====== SQL4 ======';"
  # "SELECT AVG(UserID) FROM hits;"
  "SELECT '====== SQL5 ======';"
  "SELECT COUNT(DISTINCT UserID) FROM hits;"
  "SELECT '====== SQL6 ======';"
  #"SELECT COUNT(DISTINCT SearchPhrase) FROM hits;" # wait for bugfix  https://github.com/datafuselabs/databend/issues/7743
  "SELECT COUNT(DISTINCT SearchPhrase) FROM (select SearchPhrase from hits order by SearchPhrase)"
  "SELECT '====== SQL7 ======';"
  "SELECT MIN(EventDate), MAX(EventDate) FROM hits;"
  "SELECT '====== SQL8 ======';"
  "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*),AdvEngineID DESC;"
  "SELECT '====== SQL9 ======';"
  "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u, RegionID DESC LIMIT 10;"
  "SELECT '====== SQL10 ======';"
  "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c, RegionID DESC LIMIT 10;"
  "SELECT '====== SQL11 ======';"
  "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u, MobilePhoneModel DESC LIMIT 10;"
  "SELECT '====== SQL12 ======';"
  "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY MobilePhoneModel, MobilePhone, u DESC LIMIT 10;"
  "SELECT '====== SQL13 ======';"
  "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c, SearchPhrase DESC LIMIT 10;"
  "SELECT '====== SQL14 ======';"
  "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u, SearchPhrase DESC LIMIT 10;"
  "SELECT '====== SQL15 ======';"
  "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c, SearchPhrase DESC LIMIT 10;"
  "SELECT '====== SQL16 ======';"
  "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY UserID DESC LIMIT 10;"
  "SELECT '====== SQL17 ======';"
  "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY UserID, SearchPhrase DESC LIMIT 10;"
  "SELECT '====== SQL18 ======';"
  "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY UserID, SearchPhrase DESC LIMIT 10;"
  "SELECT '====== SQL19 ======';"
  "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY UserID, m, SearchPhrase DESC LIMIT 10;"
  "SELECT '====== SQL20 ======';"
  "SELECT UserID FROM hits WHERE UserID = 567281026039120763;"
  "SELECT '====== SQL21 ======';"
  "SELECT COUNT(*) FROM hits WHERE URL LIKE '%produkty%';"
  "SELECT '====== SQL22 ======';"
  "SELECT SearchPhrase AS c, MIN(URL), COUNT(*) FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
  "SELECT '====== SQL23 ======';"
  "SELECT SearchPhrase AS c, MIN(URL), MIN(Title), COUNT(*), COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
  "SELECT '====== SQL24 ======';"
  "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;"
  "SELECT '====== SQL25 ======';" #the limit sort is unstable sort, add limit result to make result stable.
  "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 20;"
  "SELECT '====== SQL26 ======';"
  "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;"
  "SELECT '====== SQL27 ======';"
  "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;"
  "SELECT '====== SQL28 ======';"
  "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;"
  "SELECT '====== SQL29 ======';"
  "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;"
  "SELECT '====== SQL30 ======';"
  "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;"
  "SELECT '====== SQL31 ======';"
  "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c, ClientIP DESC LIMIT 10;"
  "SELECT '====== SQL32 ======';"
  "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c, WatchID DESC LIMIT 10;"
  "SELECT '====== SQL33 ======';"
  "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c, WatchID DESC LIMIT 10;"
  "SELECT '====== SQL34 ======';"
  "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;"
  "SELECT '====== SQL35 ======';"
  "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;"
  "SELECT '====== SQL36 ======';"
  "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c, ClientIP DESC LIMIT 10;"
  "SELECT '====== SQL37 ======';"
  "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY URL DESC LIMIT 10;"
  "SELECT '====== SQL38 ======';"
  "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY Title DESC LIMIT 10;"
  "SELECT '====== SQL39 ======';"
  "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY URL DESC LIMIT 10 OFFSET 1000;"
  "SELECT '====== SQL40 ======';"
  "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY Dst DESC LIMIT 10 OFFSET 1000;"
  "SELECT '====== SQL41 ======';"
  "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;"
  "SELECT '====== SQL42 ======';"
  "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;"
  "SELECT '====== SQL43 ======';"
  "SELECT DATE_TRUNC(minute, EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY  M ORDER BY  M  LIMIT 10 OFFSET 1000;"
)

for i in "${hits_statements[@]}"; do
  echo "$i" | $BENDSQL_CLIENT_CONNECT
done

## Clean up
echo "drop table if exists hits all;" | $BENDSQL_CLIENT_CONNECT
