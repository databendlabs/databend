// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::Expression;

/// Extras is a wrapper for push down items.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Extras {
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// Optional filter expression plan
    pub filters: Vec<Expression>,
    /// Optional limit to skip read
    pub limit: Option<usize>,
}

impl Extras {
    pub fn default() -> Self {
        Extras {
            projection: None,
            filters: vec![],
            limit: None,
        }
    }
}
