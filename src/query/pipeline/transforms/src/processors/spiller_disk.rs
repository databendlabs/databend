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

use std::collections::HashMap;
use std::io::IoSlice;
use std::path::PathBuf;

use databend_common_base::base::GlobalUniqName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::DataBlock;

pub struct DiskSpiller {
    root: PathBuf,

    max_size: usize,
    /// Record columns layout for spilled data, will be used when read data from disk
    pub columns_layout: HashMap<String, Vec<usize>>,
}

impl DiskSpiller {
    /// Write a [`DataBlock`] to storage.
    pub async fn spill_block(&mut self, data: DataBlock) -> Result<String> {
        let unique_name = GlobalUniqName::unique();

        let location = self.root.join(unique_name);

        // let file = DmaFile::create(location.as_path())
        //     .await
        //     .map_err(error_from_glommio)?;
        // let mut writer = DmaStreamWriterBuilder::new(file).build();

        // let data = data.convert_to_full();
        // let columns = data.columns();

        // let location = location.as_os_str().to_str().unwrap().to_string();

        // let columns_data = columns
        //     .iter()
        //     .map(|entry| serialize_column(entry.value.as_column().unwrap()))
        //     .collect::<Vec<_>>();
        // let layouts = columns_data
        //     .iter()
        //     .map(|bytes| bytes.len())
        //     .collect::<Vec<_>>();
        // self.columns_layout.insert(location.clone(), layouts);
        // let bufs = columns_data
        //     .iter()
        //     .map(|data| IoSlice::new(&data))
        //     .collect::<Vec<_>>();
        // writer.write_vectored(&bufs).await?;
        // writer.close().await?;

        todo!();

        // Ok(location)
    }
}
