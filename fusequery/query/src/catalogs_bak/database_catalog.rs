// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use crate::catalogs::utils::overlaid::InMemory;

pub struct DatabaseCatalog {
    local_db_meta: InMemory,
}
