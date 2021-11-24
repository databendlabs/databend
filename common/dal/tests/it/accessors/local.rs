//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_base::tokio;
use common_dal::DataAccessor;
use common_dal::Local;
use tempfile::TempDir;

async fn local_read(loops: u32) -> common_exception::Result<()> {
    let tmp_root_dir = TempDir::new().unwrap();
    let root_path = tmp_root_dir.path().to_str().unwrap();
    let local_da = Local::new(root_path);

    let mut files = vec![];
    for i in 0..loops {
        let file = format!("test_{}", i);
        let random_bytes: Vec<u8> = (0..122).map(|_| rand::random::<u8>()).collect();
        local_da.put(file.as_str(), random_bytes).await?;
        files.push(file)
    }

    for x in files {
        local_da.read(x.as_str()).await?;
    }
    Ok(())
}

// enable this if need to re-produce issue #2997
#[tokio::test]
#[ignore]
async fn test_da_local_hangs() -> common_exception::Result<()> {
    let read_fut = local_read(100);
    futures::executor::block_on(read_fut)
}

#[tokio::test]
async fn test_da_local_normal() -> common_exception::Result<()> {
    let read_fut = local_read(1000);
    read_fut.await
}
