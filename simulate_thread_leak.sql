drop table if exists sample;
CREATE TABLE sample
(
    Id      INT,
    City    VARCHAR,
    Score   INT,
    Country VARCHAR DEFAULT 'China'
);

drop stage if exists s1;
CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV);
