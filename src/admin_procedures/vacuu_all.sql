-- Vacuum objects in one go
CREATE OR REPLACE PROCEDURE sys_vacuum_all()
RETURNS STRING
LANGUAGE SQL
as
$$
begin

-- Vacuum all temporary tables that have not been cleaned by accident (session panic, force cluster restart etc.)
SELECT * FROM FUSE_VACUUM_TEMPORARY_TABLE();

--  Vacuum dropped database / tables
VACUUM DROP TABLE;

-- Vacuum all table historical data (using vacuum2)
CALL SYSTEM$FUSE_VACUUM2();

end;
$$
