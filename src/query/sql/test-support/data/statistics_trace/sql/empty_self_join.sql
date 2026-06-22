SELECT t1.a
FROM statistics_trace_t AS t1
     INNER JOIN statistics_trace_t AS t2 ON t1.a = t2.a
