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
use common_runtime::tokio;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

use crate::fs::FileSystem;
use crate::fs::ListResult;
use crate::localfs::LocalFS;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_localfs_read_all() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let root = dir.path();

    let f = LocalFS::try_create(root.to_str().unwrap().to_string())?;
    {
        // read absent file
        let got = f.read_all("foo.txt").await;
        assert_eq!(
            "LocalFS: fail to read: \"foo.txt\", cause: No such file or directory (os error 2)",
            got.err().unwrap().message()
        );
    }
    {
        // add foo.txt and read
        f.add("foo.txt", "123".as_bytes()).await?;
        let got = f.read_all("foo.txt").await?;
        assert_eq!("123", std::str::from_utf8(&got)?);
    }
    {
        // add foo.txt twice, fail
        let got = f.add("foo.txt", "123".as_bytes()).await;
        assert!(got
            .err()
            .unwrap()
            .to_string()
            .starts_with("Code: 1002, displayText"));
    }
    {
        // add long/bar.txt and read
        f.add("long/bar.txt".into(), "456".as_bytes()).await?;
        let got = f.read_all("long/bar.txt").await?;
        assert_eq!("456", std::str::from_utf8(&got)?);
    }

    {
        // add long/path/file.txt and read
        f.add("long/path/file.txt".into(), "789".as_bytes()).await?;
        let got = f.read_all("long/path/file.txt").await?;
        assert_eq!("789", std::str::from_utf8(&got)?);
    }
    {
        // list
        let got = f.list("long".into()).await?;
        assert_eq!(
            ListResult {
                dirs: vec!["path".into()],
                files: vec!["bar.txt".into()]
            },
            got
        );
        assert_eq!(
            "[path/, bar.txt, ]",
            format!("{}", got),
            "impl Display for ListResult"
        );
    }

    Ok(())
}
