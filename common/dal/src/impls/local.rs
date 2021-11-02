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

use std::io::Error;
use std::io::ErrorKind;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

use async_compat::CompatExt;
use common_base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::Bytes;
use crate::DataAccessor;
use crate::InputStream;

pub struct Local {
    root: PathBuf,
}

impl Local {
    pub fn new(root: &str) -> Local {
        Local {
            root: PathBuf::from(root),
        }
    }
    pub fn with_path(root_path: PathBuf) -> Local {
        Local { root: root_path }
    }
}

impl Local {
    fn prefix_with_root(&self, path: &str) -> Result<PathBuf> {
        let path = normalize_path(&self.root.join(&path));
        if path.starts_with(&self.root) {
            Ok(path)
        } else {
            // TODO customize error code
            Err(ErrorCode::from(Error::new(
                ErrorKind::Other,
                format!("please dont play with me, malicious path {:?}", path),
            )))
        }
    }
}

#[async_trait::async_trait]
impl DataAccessor for Local {
    fn get_input_stream(&self, path: &str, _stream_len: Option<u64>) -> Result<InputStream> {
        let path = self.prefix_with_root(path)?;
        let std_file = std::fs::File::open(path)?;
        let tokio_file = tokio::fs::File::from_std(std_file);
        Ok(Box::new(tokio_file.compat()))
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        let path = self.prefix_with_root(path)?;
        let mut file = tokio::fs::File::open(path).await?;
        let mut contents = vec![];
        let _ = file.read_to_end(&mut contents).await?;
        Ok(contents)
    }

    // not "atomic", for test purpose only
    async fn put(&self, path: &str, content: Vec<u8>) -> common_exception::Result<()> {
        let path = self.prefix_with_root(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| ErrorCode::UnknownException(""))?; // TODO customized error code
        tokio::fs::create_dir_all(parent).await?;
        let mut new_file = tokio::fs::File::create(path).await?;
        new_file.write_all(&content).await?;
        new_file.flush().await?;
        Ok(())
    }

    // not "atomic", for test purpose only
    async fn put_stream(
        &self,
        path: &str,
        input_stream: Box<
            dyn Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>>
                + Send
                + Unpin
                + 'static,
        >,
        _stream_len: usize,
    ) -> common_exception::Result<()> {
        let path = self.prefix_with_root(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| ErrorCode::UnknownException(""))?; // TODO customized error code
        tokio::fs::create_dir_all(parent).await?;
        let mut new_file = tokio::fs::File::create(path).await?;
        let mut s = Box::pin(input_stream);
        while let Some(Ok(v)) = s.next().await {
            new_file.write_all(&v).await?
        }
        new_file.flush().await?;
        Ok(())
    }
}

// from cargo::util::path
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}
