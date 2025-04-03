# Operator-Specific Optimizations

This directory contains operator-specific optimization rules for the Databend query optimizer. Unlike the rule-based transformations in the `transforms/rule/` directory that focus on general transformation patterns, the optimizations here are specialized for specific logical operators.

## Directory Structure

The `operator/` directory is organized by operator type:

- **aggregate/**: Optimizations specific to aggregate operations
  - `normalize_aggregate.rs`: Normalizes aggregate expressions
  - `stats_aggregate.rs`: Optimizes aggregates based on statistics

- **decorrelate/**: Optimizations for handling correlated subqueries
  - Contains utilities for subquery rewriting and unnesting

- **filter/**: Optimizations for filter operations
  - Includes deduplication of join conditions
  - Inference of filter properties
  - Normalization of disjunctive filters
  - Pull-up filter optimizations

- **join/**: Optimizations specific to join operations
  - Includes transformations like converting single joins to inner joins

## Usage

These operator-specific optimizers are typically used in the query optimization pipeline to transform logical plans into more efficient forms. They focus on the semantics and properties of specific operators rather than general transformation patterns.
