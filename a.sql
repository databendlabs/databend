
explain merge into t1 using (SELECT * EXCLUDE (rn) FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY t DESC) AS rn FROM tbcc WHERE a in(1,2,3))) as t2 on t1.a = t2.a when matched then update * when not matched then insert *;
