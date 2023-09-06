CREATE TABLE wrong_csv
(
    Id     INT NOT NULL,
    City   VARCHAR NOT NULL,
    Score  INT NOT NULL
);

CREATE TABLE wrong_ndjson
(
    a Boolean NOT NULL,
    b Int NOT NULL,
    c Float NOT NULL,
    d String NOT NULL,
    e Date NOT NULL,
    f Timestamp NOT NULL,
    g Array(Int) NOT NULL,
    h Tuple(Int, String) NOT NULL,
    i Variant NOT NULL
);

CREATE TABLE wrong_tsv
(
    a string NOT NULL,
    b int NOT NULL
);

CREATE TABLE wrong_xml (
  id          INT NOT NULL,
  name        VARCHAR NOT NULL,
  data        VARCHAR NOT NULL,
  create_time TIMESTAMP NOT NULL,
  empty       VARCHAR NULL
);
