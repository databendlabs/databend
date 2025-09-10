SET GLOBAL enterprise_license='eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJEYXRhYmVuZCBDbG91ZCIsImlhdCI6MTc1MjA0NzM0MSwibmJmIjoxNzUyMDE5MjAwLCJleHAiOjE3ODM2NDE2MDAsInR5cGUiOiJ0cmlhbCIsIm9yZyI6Ind1YnhfdGVzdCJ9.cdRFWq6EtS94GUYdpHFp2uCISZz5y4oo-3oQXtgsJXGo6MBMMqBM9ElK1JDA6K3kzSWz1Dj4YUKIeprf_MeH0g';

-- Create workload group
CREATE WORKLOAD GROUP analytics WITH cpu_quota = '10%', memory_quota = '10%', max_concurrency = 3;

-- Create user and grant permissions
CREATE USER ana IDENTIFIED BY '123';
GRANT ALL ON *.* TO ana;

-- Assign user to workload group
ALTER USER ana WITH SET WORKLOAD GROUP = 'analytics';


CREATE TABLE yy AS SELECT number FROM numbers(2048000);

-- load:
select avg(number) from yy;
SELECT sum(sqrt(power(number, 3)) * sin(number) * cos(number/2) * log(abs(number) + 1)) FROM yy;
SELECT count(*) FROM yy a JOIN yy b ON a.number < b.number WHERE a.number * b.number < 1000000;
SELECT count(*) FROM yy a JOIN yy b ON a.number < b.number WHERE a.number * b.number < 1000000 and a.number < 10000;


-- Remove user from workload group (user will use default unlimited resources)
ALTER USER ana WITH UNSET WORKLOAD GROUP;



--  CREATE WORKLOAD GROUP default_workload_group WITH memory_quota = '100%', max_concurrency = 16;

--  create user test_user_workload_group identified by '123' with SET WORKLOAD GROUP='default_workload_group';
