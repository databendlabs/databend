---
title: 'Aggregate Functions'
---

Aggregate functions are essential tools in SQL that allow you to perform calculations on a set of values and return a single result.

These functions help you extract and summarize data from databases to gain valuable insights. 

| Function Name                                               | What It Does                                                | 
|-------------------------------------------------------------|-------------------------------------------------------------|
| [COUNT](aggregate-count.md)                                 | Counts the number of rows that meet certain criteria        | 
| [COUNT_IF](aggregate-count-if.md)                           | Counts rows meeting a specified condition                   | 
| [COUNT_DISTINCT](aggregate-count-distinct.md)               | Counts the number of distinct values in a column            | 
| [APPROX_COUNT_DISTINCT](aggregate-approx-count-distinct.md) | Estimates the number of distinct values with HyperLogLog    | 
| [SUM](aggregate-sum.md)                                     | Adds up the values of a specific column                     | 
| [AVG](aggregate-avg.md)                                     | Calculates the average value of a specific column           | 
| [AVG_IF](aggregate-avg-if.md)                               | Calculates the average for rows meeting a condition         | 
| [MIN](aggregate-min.md)                                     | Finds the smallest value in a specific column               | 
| [MIN_IF](aggregate-min-if.md)                               | Finds the minimum value for rows meeting a condition        | 
| [MAX](aggregate-max.md)                                     | Finds the largest value in a specific column                | 
| [MAX_IF](aggregate-max-if.md)                               | Finds the maximum value for rows meeting a condition        | 
| [ANY](aggregate-any.md)                                     | Checks if any row meets the specified condition             | 
| [ARG_MAX](aggregate-arg-max.md)                             | Finds the arg value for the maximum val value               | 
| [ARG_MIN](aggregate-arg-min.md)                             | Finds the arg value for the minimum val value               | 
| [COVAR_POP](aggregate-covar-pop.md)                         | Returns the population covariance of a set of number pairs  | 
| [COVAR_SAMP](aggregate-covar-samp.md)                       | Returns the sample covariance of a set of number pairs      | 
| [STDDEV_POP](aggregate-stddev-pop.md)                       | Calculates the population standard deviation of a column    | 
| [STDDEV_SAMP](aggregate-stddev-samp.md)                     | Calculates the sample standard deviation of a column        | 
| [MEDIAN](aggregate-median.md)                               | Calculates the median value of a specific column            | 
| [QUANTILE](aggregate-quantile.md)                           | Calculates the quantile for a specific column               | 
| [RETENTION](aggregate-retention.md)                         | Calculates retention for a set of events                    | 
| [WINDOW_FUNNEL](aggregate-windowfunnel.md)                  | Analyzes user behavior in a time-ordered sequence of events | 
| [LIST](aggregate-list.md)                                   | Converts all the values of a column to an Array             |

