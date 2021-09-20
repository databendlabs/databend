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

use tempfile::tempdir;

use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;

#[test]
fn test_status() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        databend_dir: "/tmp/.databend".to_string(),
        download_url: "".to_string(),
        tag_url: "".to_string(),
        clap: RefCell::new(Default::default()),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();

    let mut status = Status::read(conf)?;
    status.version = "xx".to_string();
    status.write()?;

    Ok(())
}
