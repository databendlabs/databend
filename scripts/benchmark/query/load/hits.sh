#!/bin/bash

set -e

cat <<SQL | bendsql query
select version();
SQL

cat <<SQL | bendsql query
DROP TABLE IF EXISTS hits ALL;
SQL

cat <<SQL | bendsql query
  CREATE TABLE hits (
    WatchID BIGINT NOT NULL,
    JavaEnable SMALLINT NOT NULL,
    Title VARCHAR NOT NULL,
    GoodEvent SMALLINT NOT NULL,
    EventTime TIMESTAMP NOT NULL,
    EventDate Date NOT NULL,
    CounterID INTEGER NOT NULL,
    ClientIP INTEGER NOT NULL,
    RegionID INTEGER NOT NULL,
    UserID BIGINT NOT NULL,
    CounterClass SMALLINT NOT NULL,
    OS SMALLINT NOT NULL,
    UserAgent SMALLINT NOT NULL,
    URL VARCHAR NOT NULL,
    Referer VARCHAR NOT NULL,
    IsRefresh SMALLINT NOT NULL,
    RefererCategoryID SMALLINT NOT NULL,
    RefererRegionID INTEGER NOT NULL,
    URLCategoryID SMALLINT NOT NULL,
    URLRegionID INTEGER NOT NULL,
    ResolutionWidth SMALLINT NOT NULL,
    ResolutionHeight SMALLINT NOT NULL,
    ResolutionDepth SMALLINT NOT NULL,
    FlashMajor SMALLINT NOT NULL,
    FlashMinor SMALLINT NOT NULL,
    FlashMinor2 VARCHAR NOT NULL,
    NetMajor SMALLINT NOT NULL,
    NetMinor SMALLINT NOT NULL,
    UserAgentMajor SMALLINT NOT NULL,
    UserAgentMinor VARCHAR(255) NOT NULL,
    CookieEnable SMALLINT NOT NULL,
    JavascriptEnable SMALLINT NOT NULL,
    IsMobile SMALLINT NOT NULL,
    MobilePhone SMALLINT NOT NULL,
    MobilePhoneModel VARCHAR NOT NULL,
    Params VARCHAR NOT NULL,
    IPNetworkID INTEGER NOT NULL,
    TraficSourceID SMALLINT NOT NULL,
    SearchEngineID SMALLINT NOT NULL,
    SearchPhrase VARCHAR NOT NULL,
    AdvEngineID SMALLINT NOT NULL,
    IsArtifical SMALLINT NOT NULL,
    WindowClientWidth SMALLINT NOT NULL,
    WindowClientHeight SMALLINT NOT NULL,
    ClientTimeZone SMALLINT NOT NULL,
    ClientEventTime TIMESTAMP NOT NULL,
    SilverlightVersion1 SMALLINT NOT NULL,
    SilverlightVersion2 SMALLINT NOT NULL,
    SilverlightVersion3 INTEGER NOT NULL,
    SilverlightVersion4 SMALLINT NOT NULL,
    PageCharset VARCHAR NOT NULL,
    CodeVersion INTEGER NOT NULL,
    IsLink SMALLINT NOT NULL,
    IsDownload SMALLINT NOT NULL,
    IsNotBounce SMALLINT NOT NULL,
    FUniqID BIGINT NOT NULL,
    OriginalURL VARCHAR NOT NULL,
    HID INTEGER NOT NULL,
    IsOldCounter SMALLINT NOT NULL,
    IsEvent SMALLINT NOT NULL,
    IsParameter SMALLINT NOT NULL,
    DontCountHits SMALLINT NOT NULL,
    WithHash SMALLINT NOT NULL,
    HitColor VARCHAR NOT NULL,
    LocalEventTime TIMESTAMP NOT NULL,
    Age SMALLINT NOT NULL,
    Sex SMALLINT NOT NULL,
    Income SMALLINT NOT NULL,
    Interests SMALLINT NOT NULL,
    Robotness SMALLINT NOT NULL,
    RemoteIP INTEGER NOT NULL,
    WindowName INTEGER NOT NULL,
    OpenerName INTEGER NOT NULL,
    HistoryLength SMALLINT NOT NULL,
    BrowserLanguage VARCHAR NOT NULL,
    BrowserCountry VARCHAR NOT NULL,
    SocialNetwork VARCHAR NOT NULL,
    SocialAction VARCHAR NOT NULL,
    HTTPError SMALLINT NOT NULL,
    SendTiming INTEGER NOT NULL,
    DNSTiming INTEGER NOT NULL,
    ConnectTiming INTEGER NOT NULL,
    ResponseStartTiming INTEGER NOT NULL,
    ResponseEndTiming INTEGER NOT NULL,
    FetchTiming INTEGER NOT NULL,
    SocialSourceNetworkID SMALLINT NOT NULL,
    SocialSourcePage VARCHAR NOT NULL,
    ParamPrice BIGINT NOT NULL,
    ParamOrderID VARCHAR NOT NULL,
    ParamCurrency VARCHAR NOT NULL,
    ParamCurrencyID SMALLINT NOT NULL,
    OpenstatServiceName VARCHAR NOT NULL,
    OpenstatCampaignID VARCHAR NOT NULL,
    OpenstatAdID VARCHAR NOT NULL,
    OpenstatSourceID VARCHAR NOT NULL,
    UTMSource VARCHAR NOT NULL,
    UTMMedium VARCHAR NOT NULL,
    UTMCampaign VARCHAR NOT NULL,
    UTMContent VARCHAR NOT NULL,
    UTMTerm VARCHAR NOT NULL,
    FromTag VARCHAR NOT NULL,
    HasGCLID SMALLINT NOT NULL,
    RefererHash BIGINT NOT NULL,
    URLHash BIGINT NOT NULL,
    CLID INTEGER NOT NULL
  );
SQL

cat <<SQL | bendsql query
COPY INTO hits FROM 's3://repo.databend.rs/hits_p/' credentials=(aws_key_id='$REPO_ACCESS_KEY_ID' aws_secret_key='$REPO_SECRET_ACCESS_KEY') pattern ='.*[.]tsv' file_format=(type='TSV' field_delimiter='\\t' record_delimiter='\\n' skip_header=1);
SQL

cat <<SQL | bendsql query
SELECT count(*) FROM hits;
SQL
