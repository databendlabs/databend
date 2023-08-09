---
title: 'Aggregate Functions'
---

Aggregate functions are essential tools in SQL that allow you to perform calculations on a set of values and return a single result.

These functions help you extract and summarize data from databases to gain valuable insights. 

| Function Name                                               | What It Does                                                              | 
|-------------------------------------------------------------|---------------------------------------------------------------------------|
| [ANY](aggregate-any.md)                                     | Checks if any row meets the specified condition                           | 
| [APPROX_COUNT_DISTINCT](aggregate-approx-count-distinct.md) | Estimates the number of distinct values with HyperLogLog                  | 
| [ARG_MAX](aggregate-arg-max.md)                             | Finds the arg value for the maximum val value                             | 
| [ARG_MIN](aggregate-arg-min.md)                             | Finds the arg value for the minimum val value                             | 
| [AVG_IF](aggregate-avg-if.md)                               | Calculates the average for rows meeting a condition                       | 
| [ARRAY_AGG](aggregate-array-agg.md)                         | Converts all the values of a column to an Array                           |
| [AVG](aggregate-avg.md)                                     | Calculates the average value of a specific column                         | 
| [COUNT_DISTINCT](aggregate-count-distinct.md)               | Counts the number of distinct values in a column                          | 
| [COUNT_IF](aggregate-count-if.md)                           | Counts rows meeting a specified condition                                 | 
| [COUNT](aggregate-count.md)                                 | Counts the number of rows that meet certain criteria                      | 
| [COVAR_POP](aggregate-covar-pop.md)                         | Returns the population covariance of a set of number pairs                | 
| [COVAR_SAMP](aggregate-covar-samp.md)                       | Returns the sample covariance of a set of number pairs                    | 
| [GROUP_ARRAY_MOVING_AVG](aggregate-group-array-moving-avg.md) | Returns an array with elements calculates the moving average of input values  |
| [GROUP_ARRAY_MOVING_SUM](aggregate-group-array-moving-sum.md) | Returns an array with elements calculates the moving sum of input values  |
| [KURTOSIS](aggregate-kurtosis.md)                           | Calculates the excess kurtosis of a set of values                         | 
| [MAX_IF](aggregate-max-if.md)                               | Finds the maximum value for rows meeting a condition                      | 
| [MAX](aggregate-max.md)                                     | Finds the largest value in a specific column                              | 
| [MEDIAN](aggregate-median.md)                               | Calculates the median value of a specific column                          | 
| [MEDIAN_TDIGEST](aggregate-median-tdigest.md)               | Calculates the median value of a specific column using t-digest algorithm | 
| [MIN_IF](aggregate-min-if.md)                               | Finds the minimum value for rows meeting a condition                      | 
| [MIN](aggregate-min.md)                                     | Finds the smallest value in a specific column                             | 
| [QUANTILE_CONT](aggregate-quantile-cont.md)                 | Calculates the interpolated quantile for a specific column                |
| [QUANTILE_DISC](aggregate-quantile-disc.md)                 | Calculates the quantile for a specific column                             | 
| [QUANTILE_TDIGEST](aggregate-quantile-tdigest.md)           | Calculates the quantile using t-digest algorithm                          |
| [RETENTION](aggregate-retention.md)                         | Calculates retention for a set of events                                  | 
| [SKEWNESS](aggregate-skewness.md)                           | Calculates the skewness of a set of values                                | 
| [STDDEV_POP](aggregate-stddev-pop.md)                       | Calculates the population standard deviation of a column                  | 
| [STDDEV_SAMP](aggregate-stddev-samp.md)                     | Calculates the sample standard deviation of a column                      | 
| [STRING_AGG](aggregate-string-agg.md)                       | Converts all the non-NULL values to String, separated by the delimiter    |
| [SUM_IF](aggregate-sum-if.md)                               | Adds up the values meeting a condition of a specific column               | 
| [SUM](aggregate-sum.md)                                     | Adds up the values of a specific column                                   | 
| [WINDOW_FUNNEL](aggregate-windowfunnel.md)                  | Analyzes user behavior in a time-ordered sequence of events               | 
