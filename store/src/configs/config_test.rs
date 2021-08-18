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

use crate::configs::Config;

#[test]
fn test_fuse_commit_version() -> anyhow::Result<()> {
    let v = &crate::configs::config::FUSE_COMMIT_VERSION;
    assert!(v.len() > 0);
    Ok(())
}

#[test]
fn test_tls_rpc_enabled() -> anyhow::Result<()> {
    let mut conf = Config::empty();
    assert_eq!(false, conf.tls_rpc_server_enabled());
    conf.rpc_tls_server_key = "test".to_owned();
    assert_eq!(false, conf.tls_rpc_server_enabled());
    conf.rpc_tls_server_cert = "test".to_owned();
    assert_eq!(true, conf.tls_rpc_server_enabled());
    Ok(())
}
