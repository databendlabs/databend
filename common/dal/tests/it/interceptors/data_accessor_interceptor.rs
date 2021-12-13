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

use std::sync::Arc;

use common_base::tokio;
use common_base::tokio::io::SeekFrom;
use common_dal::DalContext;
use common_dal::DalMetrics;
use common_dal::DataAccessor;
use common_dal::DataAccessorInterceptor;
use common_dal::Local;
use common_metrics::init_default_metrics_recorder;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use tempfile::TempDir;

struct TestFixture {
    _tmp_dir: TempDir,
    ctx: Arc<DalContext>,
    da_with_metric: Arc<dyn DataAccessor>,
    raw_da: Local,
}

impl TestFixture {
    fn new() -> Self {
        let ctx = Arc::new(
            DalContext::create(false, "".to_string(), 0, "".to_string(), "".to_string()).unwrap(),
        );
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().to_str().unwrap();
        let raw_da = Local::new(path);

        // da with metric
        let local = Local::new(path);
        let da_with_metric = DataAccessorInterceptor::new(ctx.clone(), Arc::new(local));

        init_default_metrics_recorder();

        Self {
            _tmp_dir: tmp_dir,
            ctx,
            da_with_metric: Arc::new(da_with_metric),
            raw_da,
        }
    }

    fn get_metrics(&self) -> DalMetrics {
        self.ctx.get_metrics()
    }

    async fn gen_rand_content(&self, path: &str, len: i64) -> common_exception::Result<()> {
        let random_bytes: Vec<u8> = (0..len).map(|_| rand::random::<u8>()).collect();
        self.raw_da.put(path, random_bytes).await
    }
}
#[tokio::test]
async fn test_dal_metrics_write() -> common_exception::Result<()> {
    // setup
    let fixture = TestFixture::new();
    let dal = &fixture.da_with_metric;

    let len = 100;
    let random_bytes: Vec<u8> = (0..len).map(|_| rand::random::<u8>()).collect();

    // write 100 bytes
    dal.put("test_path", random_bytes).await?;

    // check
    let metrics = fixture.get_metrics();
    assert_eq!(len, metrics.write_bytes);
    Ok(())
}

#[tokio::test]
async fn test_dal_metrics_put_stream() -> common_exception::Result<()> {
    // setup
    let fixture = TestFixture::new();
    let dal = &fixture.da_with_metric;

    let len = 100;
    let random_bytes: Vec<u8> = (0..len).map(|_| rand::random::<u8>()).collect();

    // write 100 bytes
    let stream = Box::pin(futures::stream::once(async move {
        Ok(bytes::Bytes::from(random_bytes))
    }));
    dal.put_stream("test_path", Box::new(stream), len).await?;

    // check
    let metrics = fixture.get_metrics();
    assert_eq!(len, metrics.write_bytes);
    Ok(())
}

#[tokio::test]
async fn test_dal_metrics_read() -> common_exception::Result<()> {
    // setup
    let fixture = TestFixture::new();
    let dal = &fixture.da_with_metric;

    let len = 100;
    fixture.gen_rand_content("test_path", len).await?;

    // read all
    let mut input_stream = dal.get_input_stream("test_path", None)?;
    let mut buf = Vec::new();
    input_stream.read_to_end(&mut buf).await?;

    // check
    let metrics = fixture.get_metrics();
    assert_eq!(len as usize, metrics.read_bytes);

    Ok(())
}

#[tokio::test]
async fn test_dal_metrics_partial_read() -> common_exception::Result<()> {
    // setup
    let fixture = TestFixture::new();
    let dal = &fixture.da_with_metric;

    let len = 100;
    fixture.gen_rand_content("test_path", len).await?;

    // partial read
    let seek_pos = 10;
    let mut input_stream = dal.get_input_stream("test_path", None)?;
    let mut buf = Vec::new();
    input_stream.seek(SeekFrom::Current(seek_pos)).await?;
    input_stream.read_to_end(&mut buf).await?;

    // check
    let partial_read_len = len - seek_pos;
    let metrics = fixture.get_metrics();
    assert_eq!(partial_read_len as usize, metrics.read_bytes);

    Ok(())
}
