// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;
use std::sync::Mutex;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;

/// please do not keep this, this code is just for test purpose
type BlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = DataBlock> + Sync + Send + 'static>>;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct InsertIntoPlan {
    pub db_name: String,
    pub tbl_name: String,
    pub schema: DataSchemaRef,

    #[serde(skip, default = "InsertIntoPlan::empty_stream")]
    pub input_stream: Arc<Mutex<Option<BlockStream>>>,
}

impl PartialEq for InsertIntoPlan {
    fn eq(&self, other: &Self) -> bool {
        self.db_name == other.db_name
            && self.tbl_name == other.tbl_name
            && self.schema == other.schema
    }
}

impl InsertIntoPlan {
    pub fn empty_stream() -> Arc<Mutex<Option<BlockStream>>> {
        Default::default()
    }
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
    pub fn set_input_stream(&self, input_stream: BlockStream) {
        let mut writer = self.input_stream.lock().unwrap();
        *writer = Some(input_stream);
    }
}
