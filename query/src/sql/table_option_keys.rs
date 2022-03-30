// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;

use lazy_static::lazy_static;

pub const OPT_KEY_SNAPSHOT_VER: &str = "snapshot_ver";
pub const OPT_KEY_DATABASE_ID: &str = "database_id";

lazy_static! {
    pub static ref RESERVED_OPTS: HashSet<&'static str> = {
        let mut r = HashSet::new();
        r.insert(OPT_KEY_DATABASE_ID);
        r.insert(OPT_KEY_SNAPSHOT_VER);
        r
    };
}

pub fn is_reserved_opt_key<S: AsRef<str>>(opt_key: S) -> bool {
    RESERVED_OPTS.contains(opt_key.as_ref().to_lowercase().as_str())
}
