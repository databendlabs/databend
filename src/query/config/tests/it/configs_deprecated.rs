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

use std::fs::File;
use std::io::Write;

use common_config::Config;
use common_exception::Result;
use pretty_assertions::assert_eq;

#[test]
fn test_meta_address_deprecated() -> Result<()> {
    let d = tempfile::tempdir()?;
    let file_path = d.path().join("meta-address-deprecated.toml");
    let mut file = File::create(&file_path)?;
    write!(
        file,
        r#"
[meta]
embedded_dir = "./.databend/meta_embedded_1"
address = "127.0.0.1:9191"
username = "root"
password = "root"
client_timeout_in_second = 60
auto_sync_interval = 60 
"#
    )?;

    temp_env::with_var("CONFIG_FILE", Some(file_path), || {
        let parsed = Config::load();
        let expect = "error....";
        let actual = format!("{:?}", parsed);
        assert_eq!(expect, actual);
    });

    Ok(())
}
