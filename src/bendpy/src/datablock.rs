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

use common_expression::block_debug::box_render;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use pyo3::prelude::*;

#[pyclass(name = "DataBlock", module = "databend", subclass)]

pub struct PyDataBlock {
    block: DataBlock,
    schema: DataSchemaRef,
}

#[pymethods]
impl PyDataBlock {
    fn __repr__(&self, _py: Python) -> PyResult<String> {
        let block = self.block.slice(0..10);
        let s: String = box_render(&self.schema, &[block]).unwrap();
        Ok(s)
    }

    fn num_rows(&self) -> usize {
        self.block.num_rows()
    }

    fn num_columns(&self) -> usize {
        self.block.num_columns()
    }
}

#[pyclass(name = "DataBlocks", module = "databend", subclass)]
pub struct PyDataBlocks {
    pub(crate) blocks: Vec<DataBlock>,
    pub(crate) schema: DataSchemaRef,
}

impl PyDataBlocks {
    pub fn box_render(&self) -> String {
        let blocks: Vec<DataBlock> = self.blocks.iter().take(10).cloned().collect();
        box_render(&self.schema, &blocks).unwrap()
    }
}

#[pymethods]
impl PyDataBlocks {
    fn __repr__(&self, _py: Python) -> PyResult<String> {
        Ok(self.box_render())
    }

    fn num_rows(&self) -> usize {
        self.blocks.iter().map(|b| b.num_rows()).sum()
    }

    fn num_columns(&self) -> usize {
        self.schema.num_fields()
    }
}
