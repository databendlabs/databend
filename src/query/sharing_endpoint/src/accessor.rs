// Copyright 2021 Datafuse Labs
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

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::init_operator;
use opendal::Operator;

use crate::configs::Config;

mod share_spec_accessor;
mod share_table_accessor;
mod share_table_meta_accessor;

#[derive(Clone)]
pub struct SharingAccessor {
    op: Operator,
    config: Config,
}

// file would have two kind of path:
// 1. with root. e.g. /root1/root2/root3/db1/tb1/file1
// 2. without root e.g. db1/tb1/file1
// after it would be converted to file1
// and then it would use the location in table spec to form the final path
// {location}/file1
pub fn truncate_root(root: String, loc: String) -> String {
    let root = root.trim_matches('/');
    let loc = loc.trim_matches('/');
    return if loc.starts_with(root) {
        let o1 = loc.strip_prefix(root).unwrap();

        let updated = o1.trim_matches('/');
        let arr = updated.split('/').collect::<Vec<&str>>();
        if arr.len() > 2 {
            return arr[2..].join("/");
        }
        updated.to_string()
    } else {
        let arr = loc.split('/').collect::<Vec<&str>>();
        if arr.len() > 2 {
            return arr[2..].join("/");
        }
        loc.to_string()
    };
}

impl SharingAccessor {
    #[async_backtrace::framed]
    pub async fn init(cfg: &Config) -> Result<()> {
        GlobalInstance::set(Self::try_create(cfg).await?);

        Ok(())
    }

    pub fn instance() -> SharingAccessor {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    pub async fn try_create(cfg: &Config) -> Result<SharingAccessor> {
        let operator = init_operator(&cfg.storage.params)?;

        Ok(SharingAccessor {
            op: operator,
            config: cfg.clone(),
        })
    }

    fn get_root(&self) -> String {
        match self.config.storage.params {
            StorageParams::S3(ref s3) => s3.root.trim_matches('/').to_string(),
            StorageParams::Oss(ref oss) => oss.root.trim_matches('/').to_string(),
            _ => "".to_string(),
        }
    }
}
