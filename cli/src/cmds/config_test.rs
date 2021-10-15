// Copyright 2020 Datafuse Labs.
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

use std::cell::RefCell;
use std::collections::HashMap;

use databend_query::configs::Config as QueryConfig;
use metasrv::configs::Config as MetaConfig;
use tempfile::tempdir;

use crate::cmds::status::LocalMetaConfig;
use crate::cmds::status::LocalQueryConfig;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;
use crate::cmds::config::MirrorAsset;

struct MockMirror {
    url: String
}

impl MockMirror {
    fn set_url(&mut self, url: String) {
        self.url = url
    }
    fn get_url(&self) -> String {
       return self.url.clone()
    }
}

impl MirrorAsset for MockMirror {
    const BASE_URL: String =  "".to_string();
    const DATABEND_URL: String = "".to_string();
    const DATABEND_TAG_URL: String = "".to_string();
    const CLIENT_URL: String = "".to_string();

    fn get_base_url(&self) -> String {
        return self.url.clone()
    }

    fn get_databend_url(&self) -> String {
        todo!()
    }

    fn get_databend_tag_url(&self) -> String {
        todo!()
    }

    fn get_client_url(&self) -> String {
        todo!()
    }
}

#[test]
fn test_mirror() -> Result<()> {
    Ok(())
}
