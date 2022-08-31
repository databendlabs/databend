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

use std::fmt::Debug;
use std::sync::Arc;

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::io::write_block;
use databend_query::storages::fuse::io::BlockReader;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use opendal::ops::OpRead;
use opendal::ops::OpWrite;
use opendal::Accessor;
use opendal::BytesReader;
use opendal::Operator;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::tests::create_query_context;

#[test]
fn test_meta_locations() -> Result<()> {
    let test_prefix = "test_pref";
    let locs = TableMetaLocationGenerator::with_prefix(test_prefix.to_owned());
    let ((path, _ver), _id) = locs.gen_block_location();
    assert!(path.starts_with(test_prefix));
    let seg_loc = locs.gen_segment_info_location();
    assert!(seg_loc.starts_with(test_prefix));
    let uuid = Uuid::new_v4();
    let snapshot_loc = locs.snapshot_location_from_uuid(&uuid, TableSnapshot::VERSION)?;
    assert!(snapshot_loc.starts_with(test_prefix));
    Ok(())
}

#[tokio::test]
async fn test_column_reader_retry_should_return_original_error() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    let operator = ctx.get_storage_operator()?;
    let reader = operator.object("not_exist");
    let r = BlockReader::read_column(reader, 0, 0, 100).await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(ErrorCode::storage_not_found_code(), e.code());
    Ok(())
}

type IOError = std::io::Error;

#[tokio::test]
async fn test_column_reader_retry() -> Result<()> {
    // transient err
    {
        let errors = std::iter::from_fn(|| {
            Some(std::io::Error::new(
                std::io::ErrorKind::Other,
                "other error",
            ))
        });
        let mock = Arc::new(Mock::with_exception(errors));
        let op = Operator::new(mock.clone());
        let reader = op.object("does not matter");
        let r = BlockReader::read_column(reader, 0, 0, 1).await;
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert_eq!(ErrorCode::storage_other_code(), e.code());
        // read operation should have been retried
        assert!(mock.read_count() > 1);
    }
    // permanent err
    {
        let errors = std::iter::from_fn(|| {
            Some(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "not found",
            ))
        });
        let mock = Arc::new(Mock::with_exception(errors));
        let op = Operator::new(mock.clone());
        let reader = op.object("does not matter");
        let r = BlockReader::read_column(reader, 0, 0, 1).await;
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert_eq!(ErrorCode::storage_not_found_code(), e.code());
        // read operation should have NOT been retried
        assert_eq!(1, mock.read_count());
    }
    Ok(())
}

#[tokio::test]
async fn test_meta_reader_retry() -> Result<()> {
    // transient err
    {
        let errors = std::iter::from_fn(|| {
            Some(std::io::Error::new(
                std::io::ErrorKind::Other,
                "other error",
            ))
        });
        let mock = Arc::new(Mock::with_exception(errors));
        let op = Operator::new(mock.clone());
        let reader = op.object("does not matter");
        let r = BlockReader::read_column(reader, 0, 0, 1).await;
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert_eq!(ErrorCode::storage_other_code(), e.code());
        // read operation should have been retried
        assert!(mock.read_count() > 1);
    }
    // permanent err
    {
        let errors = std::iter::from_fn(|| {
            Some(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "not found",
            ))
        });
        let mock = Arc::new(Mock::with_exception(errors));
        let op = Operator::new(mock.clone());
        let reader = op.object("does not matter");
        let r = BlockReader::read_column(reader, 0, 0, 1).await;
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert_eq!(ErrorCode::storage_not_found_code(), e.code());
        // read operation should have NOT been retried
        assert_eq!(1, mock.read_count());
    }
    Ok(())
}

#[tokio::test]
async fn test_block_writer_retry() -> Result<()> {
    // transient err
    let errors = std::iter::from_fn(|| {
        Some(std::io::Error::new(
            std::io::ErrorKind::Other,
            "other error",
        ))
    });
    let mock = Arc::new(Mock::with_exception(errors));
    let op = Operator::new(mock.clone());
    let block = DataBlock::empty();
    let r = write_block(block, &op, "loc").await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(ErrorCode::storage_other_code(), e.code());
    // write operation should have been retried
    assert!(mock.write_count() > 1);
    Ok(())
}

#[derive(Debug)]
struct MockState<P> {
    // oneshot err
    errors: P,
    read_op_count: u32,
    write_op_count: u32,
}

impl<P> MockState<P>
where P: Iterator<Item = IOError>
{
    fn with_error(e: P) -> Self {
        Self {
            errors: e,
            read_op_count: 0,
            write_op_count: 0,
        }
    }
    fn take_err(&mut self) -> IOError {
        self.errors.next().unwrap()
    }

    fn increase_read_op_count(&mut self) {
        self.read_op_count += 1;
    }
    fn increase_write_op_count(&mut self) {
        self.write_op_count += 1;
    }
}

#[derive(Debug)]
struct Mock<P> {
    // oneshot err
    err: Mutex<MockState<P>>,
}

impl<P> Mock<P>
where P: Iterator<Item = IOError>
{
    fn with_exception(e: P) -> Self {
        Self {
            err: Mutex::new(MockState::with_error(e)),
        }
    }

    fn read_count(&self) -> u32 {
        let v = &*self.err.lock();
        v.read_op_count
    }
    fn write_count(&self) -> u32 {
        let v = &*self.err.lock();
        v.write_op_count
    }
}

#[async_trait::async_trait]
impl<P> Accessor for Mock<P>
where
    P: Debug + Send,
    P: Iterator<Item = IOError>,
{
    async fn read(&self, _args: &OpRead) -> std::io::Result<BytesReader> {
        let v = &mut (*self.err.lock());
        let err = v.take_err();
        v.increase_read_op_count();
        Err(err)
    }

    async fn write(&self, _args: &OpWrite, _r: BytesReader) -> std::io::Result<u64> {
        let v = &mut (*self.err.lock());
        let err = v.take_err();
        v.increase_write_op_count();
        Err(err)
    }
}
