// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use pretty_assertions::assert_eq;
use tempfile::tempdir;

use crate::fs::IFileSystem;
use crate::fs::ListResult;
use crate::fs::LocalFS;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_localfs_read_all() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let root = dir.path();

    let f = LocalFS::try_create(root)?;
    {
        // read absent file
        let got = f.read_all("foo.txt").await;
        assert_eq!(
            "No such file or directory (os error 2)",
            got.err().unwrap().root_cause().to_string()
        );
    }
    {
        // add foo.txt and read
        f.add("foo.txt".to_string(), "123".as_bytes()).await?;
        let got = f.read_all("foo.txt").await?;
        assert_eq!("123", std::str::from_utf8(&got)?);
    }
    {
        // add foo.txt twice, fail
        let got = f.add("foo.txt".to_string(), "123".as_bytes()).await;
        assert_eq!(
            "LocalFS: fail to open foo.txt",
            got.err().unwrap().to_string()
        );
    }
    {
        // add long/bar.txt and read
        f.add("long/bar.txt", "456".as_bytes()).await?;
        let got = f.read_all("long/bar.txt").await?;
        assert_eq!("456", std::str::from_utf8(&got)?);
    }

    {
        // add long/path/file.txt and read
        f.add("long/path/file.txt", "789".as_bytes()).await?;
        let got = f.read_all("long/path/file.txt").await?;
        assert_eq!("789", std::str::from_utf8(&got)?);
    }
    {
        // list
        let got = f.list("long").await?;
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
