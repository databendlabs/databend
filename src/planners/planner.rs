// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

pub struct Planner;

impl Planner {
    /// Creates a new planner.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}
