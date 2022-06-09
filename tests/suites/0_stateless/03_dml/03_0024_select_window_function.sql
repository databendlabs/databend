DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

DROP TABLE IF EXISTS sales;
CREATE TABLE `sales` (
  `year` varchar(64) DEFAULT NULL,
  `country` varchar(64) DEFAULT NULL,
  `product` varchar(64) DEFAULT NULL,
  `profit` int DEFAULT NULL
) Engine = Fuse;

SET enable_new_processor_framework=0;

INSERT INTO `sales` VALUES ('2000','Finland','Computer',1500),('2000','Finland','Phone',100),('2001','Finland','Phone',10),('2000','India','Calculator',75),('2000','India','Calculator',75),('2000','India','Computer',1200),('2000','USA','Calculator',75),('2000','USA','Computer',1500),('2001','USA','Calculator',50),('2001','USA','Computer',1500),('2001','USA','Computer',1200),('2001','USA','TV',150),('2001','USA','TV',100),('2001','China','TV',110),('2001','China','Computer',200);

select '================sep================';
select country, year, sum(profit) over() from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(order by country) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country order by year) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country order by year rows between 1 preceding and 1 following) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country order by year rows between unbounded preceding and 1 following) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country order by year rows between 1 preceding and unbounded following) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country order by year rows between unbounded preceding and current row) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country order by year rows between current row and unbounded following) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country order by year rows between unbounded preceding and unbounded following) from sales order by country, year;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between 500 preceding and 500 following) from sales order by country, profit;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between unbounded preceding and 500 following) from sales order by country, profit;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between 500 preceding and unbounded following) from sales order by country, profit;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between current row and 500 following) from sales order by country, profit;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between 500 preceding and current row) from sales order by country, profit;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between unbounded preceding and current row) from sales order by country, profit;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between current row and unbounded following) from sales order by country, profit;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between unbounded preceding and unbounded following) from sales order by country, profit;
select '================sep================';
select country, year, sum(profit) over(partition by country order by profit range between 500 preceding and 500 following) as sum, avg(profit) over(partition by country order by profit range between 500 preceding and 500 following) as avg from sales order by country, profit;

DROP DATABASE db1;