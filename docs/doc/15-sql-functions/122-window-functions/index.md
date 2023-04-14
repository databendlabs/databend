---
title: 'Window Functions'
---

## Overview 

A window function operates on a group ("window") of related rows.

For each input row, a window function returns one output row that depends on the specific row passed to the function and the values of the other rows in the window.

There are two main types of order-sensitive window functions:

* `Rank-related functions`: Rank-related functions list information based on the "rank" of a row. For example, ranking stores in descending order by profit per year, the store with the most profit will be ranked 1, and the second-most profitable store will be ranked 2, and so on.

* `Window frame functions`: Window frame functions enable you to perform rolling operations, such as calculating a running total or a moving average, on a subset of the rows in the window.

## List of Functions that Support Windows

The list below shows all the window functions.

| Function Name                                                         | Category     | Window | Window Frame | Notes |
|-----------------------------------------------------------------------|--------------|--------|--------------|-------|
| [ARRAY_AGG](../10-aggregate-functions/aggregate-array-agg.md)         | General      | ✔      |              |       |
| [AVG](../10-aggregate-functions/aggregate-avg.md)                     | General      | ✔      | ✔            |       |
| [AVG_IF](../10-aggregate-functions/aggregate-avg-if.md)               | General      | ✔      | ✔            |       |
| [COUNT](../10-aggregate-functions/aggregate-count.md)                 | General      | ✔      | ✔            |       |
| [COUNT_IF](../10-aggregate-functions/aggregate-count-if.md)           | General      | ✔      | ✔            |       |
| [COVAR_POP](../10-aggregate-functions/aggregate-covar-pop.md)         | General      | ✔      |              |       |
| [COVAR_SAMP](../10-aggregate-functions/aggregate-covar-samp.md)       | General      | ✔      |              |       |
| [MAX](../10-aggregate-functions/aggregate-max.md)                     | General      | ✔      | ✔            |       |
| [MAX_IF](../10-aggregate-functions/aggregate-max-if.md)               | General      | ✔      | ✔            |       |
| [MIN](../10-aggregate-functions/aggregate-min.md)                     | General      | ✔      | ✔            |       |
| [MIN_IF](../10-aggregate-functions/aggregate-min-if.md)               | General      | ✔      | ✔            |       |
| [STDDEV_POP](../10-aggregate-functions/aggregate-stddev-pop.md)       | General      | ✔      | ✔            |       |
| [STDDEV_SAMP](../10-aggregate-functions/aggregate-stddev-samp.md)     | General      | ✔      | ✔            |       |
| [MEDIAN](../10-aggregate-functions/aggregate-median.md)               | General      | ✔      | ✔            |       |
| [QUANTILE_CONT](../10-aggregate-functions/aggregate-quantile-cont.md) | General      | ✔      | ✔            |       |
| [QUANTILE_DISC](../10-aggregate-functions/aggregate-quantile-disc.md) | General      | ✔      | ✔            |       |
| [KURTOSIS](../10-aggregate-functions/aggregate-kurtosis.md)           | General      | ✔      | ✔            |       |
| [SKEWNESS](../10-aggregate-functions/aggregate-skewness.md)           | General      | ✔      | ✔            |       |
| [SUM](../10-aggregate-functions/aggregate-sum.md)                     | General      | ✔      | ✔            |       |
| [SUM_IF](../10-aggregate-functions/aggregate-sum-if.md)               | General      | ✔      | ✔            |       |
| [DENSE_RANK](01-window-function-dense-rank.md)                        | Rank-related | ✔      | ✔            |       |
| [RANK](01-window-function-rank.md)                                    | Rank-related | ✔      | ✔            |       |
| [ROW_NUMBER](01-window-function-row-number.md)                        | Rank-related | ✔      |              |       |


## Window Syntax

```sql
<function> ( [ <arguments> ] ) OVER ( { named window | inline window } )

named window ::=
    { window_name | ( window_name ) }

inline window ::=
    [ PARTITION BY <expression_list> ]
    [ ORDER BY <expression_list> ]
    [ window frame ]
```
The `named window` is a window that is defined in the `WINDOW` clause of the `SELECT` statement, eg: `SELECT a, SUM(a) OVER w FROM t WINDOW w AS ( inline window )`.

The `<function>` is one of ([aggregate function](../10-aggregate-functions/index.md), rank function, value function).

The `OVER` clause specifies that the function is being used as a window function.

The `PARTITION BY` sub-clause allows rows to be grouped into sub-groups, for example by city, by year, etc. The `PARTITION BY` clause is optional. You can analyze an entire group of rows without breaking it into sub-groups.

The `ORDER BY` clause orders rows within the window. 

The `window frame` clause specifies the window frame type and the window frame extent. The `window frame` clause is optional. If you omit the `window frame` clause, the default window frame type is `RANGE` and the default window frame extent is `UNBOUNDED PRECEDING AND CURRENT ROW`.


## Window Frame Syntax

`window frame` can be one of the following types:

```sql
cumulativeFrame ::=
    {
       { ROWS | RANGE } BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     | { ROWS | RANGE } BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    }
```

```sql
slidingFrame ::=
    {
       ROWS BETWEEN <N> { PRECEDING | FOLLOWING } AND <N> { PRECEDING | FOLLOWING }
     | ROWS BETWEEN UNBOUNDED PRECEDING AND <N> { PRECEDING | FOLLOWING }
     | ROWS BETWEEN <N> { PRECEDING | FOLLOWING } AND UNBOUNDED FOLLOWING
    }
```


## Examples

**Create the table**
```sql
CREATE TABLE employees (
  employee_id INT,
  first_name VARCHAR,
  last_name VARCHAR,
  department VARCHAR,
  salary INT
);
```

**Insert data**
```sql
INSERT INTO employees (employee_id, first_name, last_name, department, salary) VALUES
  (1, 'John', 'Doe', 'IT', 75000),
  (2, 'Jane', 'Smith', 'HR', 85000),
  (3, 'Mike', 'Johnson', 'IT', 90000),
  (4, 'Sara', 'Williams', 'Sales', 60000),
  (5, 'Tom', 'Brown', 'HR', 82000),
  (6, 'Ava', 'Davis', 'Sales', 62000),
  (7, 'Olivia', 'Taylor', 'IT', 72000),
  (8, 'Emily', 'Anderson', 'HR', 77000),
  (9, 'Sophia', 'Lee', 'Sales', 58000),
  (10, 'Ella', 'Thomas', 'IT', 67000);
```

**Example 1: Ranking employees by salary**

In this example, we use the RANK() function to rank employees based on their salaries in descending order. The highest salary will get a rank of 1, and the lowest salary will get the highest rank number.
```sql
SELECT employee_id, first_name, last_name, department, salary, RANK() OVER (ORDER BY salary DESC) AS rank
FROM employees;
```

Result:

| employee_id | first_name | last_name | department | salary | rank |
|-------------|------------|-----------|------------|--------|------|
| 3           | Mike       | Johnson   | IT         | 90000  | 1    |
| 2           | Jane       | Smith     | HR         | 85000  | 2    |
| 5           | Tom        | Brown     | HR         | 82000  | 3    |
| 8           | Emily      | Anderson  | HR         | 77000  | 4    |
| 1           | John       | Doe       | IT         | 75000  | 5    |
| 7           | Olivia     | Taylor    | IT         | 72000  | 6    |
| 10          | Ella       | Thomas    | IT         | 67000  | 7    |
| 6           | Ava        | Davis     | Sales      | 62000  | 8    |
| 4           | Sara       | Williams  | Sales      | 60000  | 9    |
| 9           | Sophia     | Lee       | Sales      | 58000  | 10   |



**Example 2: Calculating the total salary per department**

In this example, we use the SUM() function with PARTITION BY to calculate the total salary paid per department. Each row will show the department and the total salary for that department.
```sql
SELECT department, SUM(salary) OVER (PARTITION BY department) AS total_salary
FROM employees;
```

Result:

| department | total_salary |
|------------|--------------|
| HR         | 244000       |
| HR         | 244000       |
| HR         | 244000       |
| IT         | 304000       |
| IT         | 304000       |
| IT         | 304000       |
| IT         | 304000       |
| Sales      | 180000       |
| Sales      | 180000       |
| Sales      | 180000       |


**Example 3: Calculating a running total of salaries per department**

In this example, we use the SUM() function with a cumulative window frame to calculate a running total of salaries within each department. The running total is calculated based on the employee's salary ordered by their employee_id.
```sql
SELECT employee_id, first_name, last_name, department, salary, 
       SUM(salary) OVER (PARTITION BY department ORDER BY employee_id
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM employees;
```

Result:

| employee_id | first_name | last_name | department | salary | running_total |
|-------------|------------|-----------|------------|--------|---------------|
| 2           | Jane       | Smith     | HR         | 85000  | 85000         |
| 5           | Tom        | Brown     | HR         | 82000  | 167000        |
| 8           | Emily      | Anderson  | HR         | 77000  | 244000        |
| 1           | John       | Doe       | IT         | 75000  | 75000         |
| 3           | Mike       | Johnson   | IT         | 90000  | 165000        |
| 7           | Olivia     | Taylor    | IT         | 72000  | 237000        |
| 10          | Ella       | Thomas    | IT         | 67000  | 304000        |
| 4           | Sara       | Williams  | Sales      | 60000  | 60000         |
| 6           | Ava        | Davis     | Sales      | 62000  | 122000        |
| 9           | Sophia     | Lee       | Sales      | 58000  | 180000        |
