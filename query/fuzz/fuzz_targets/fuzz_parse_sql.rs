// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use datafuse_query::sql::DfParser;
use honggfuzz::fuzz;

fn main() {
    loop {
        fuzz!(|data: String| {
            let _ = DfParser::parse_sql(&data);
        });
    }
}
