CREATE TABLE wrong_csv
(
    Id     INT,
    City   VARCHAR,
    Score  INT
);

CREATE TABLE wrong_ndjson
(
    a Boolean,
    b Int,
    c Float,
    d String,
    e Date,
    f Timestamp,
    g Array(Int),
    h Tuple(Int, String),
    i Variant
);

CREATE TABLE wrong_tsv
(
    a string,
    b int
);

CREATE TABLE wrong_xml (
  id          INT,
  name        VARCHAR,
  data        VARCHAR,
  create_time TIMESTAMP,
  empty       VARCHAR NULL
);
