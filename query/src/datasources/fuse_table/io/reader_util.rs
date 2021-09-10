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
use std::sync::mpsc::channel;
use std::sync::Arc;

use common_dal::DataAccessor;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::AsyncReadExt;
use serde::de::DeserializeOwned;

use crate::sessions::DatafuseQueryContextRef;

pub fn do_read(
    da: Arc<dyn DataAccessor>,
    ctx: &DatafuseQueryContextRef,
    loc: &str,
) -> Result<Vec<u8>> {
    let (tx, rx) = channel();
    let location = loc.to_string();
    ctx.execute_task(async move {
        let input_stream = da.get_input_stream(&location, None);
        match input_stream.await {
            Ok(mut input) => {
                let mut buffer = vec![];
                // TODO send this error
                input.read_to_end(&mut buffer).await?;
                let _ = tx.send(Ok(buffer));
            }
            Err(e) => {
                let _ = tx.send(Err(e));
            }
        }
        Ok::<(), ErrorCode>(())
    })?;

    // TODO error code
    rx.recv().map_err(ErrorCode::from_std_error)?
}

pub fn do_read_obj<T: DeserializeOwned>(
    da: Arc<dyn DataAccessor>,
    ctx: &DatafuseQueryContextRef,
    loc: &str,
) -> Result<T> {
    let bytes = do_read(da, ctx, loc)?;
    let r = serde_json::from_slice::<T>(&bytes)?;
    Ok(r)
}

pub async fn do_read_async(da: Arc<dyn DataAccessor>, location: &str) -> Result<Vec<u8>> {
    let mut input_stream = da.get_input_stream(location, None).await?;
    let mut buffer = vec![];
    input_stream.read_to_end(&mut buffer).await?;
    Ok(buffer)
}

pub async fn do_read_obj_async<T: DeserializeOwned>(
    da: Arc<dyn DataAccessor>,
    loc: &str,
) -> Result<T> {
    let bytes = do_read_async(da, loc).await?;
    let r = serde_json::from_slice::<T>(&bytes)?;
    Ok(r)
}
