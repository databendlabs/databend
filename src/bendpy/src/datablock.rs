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

use databend_common_expression::block_debug::box_render;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use pyo3::prelude::*;

use crate::dataframe::PyBoxSize;

#[pyclass(name = "DataBlock", module = "databend", subclass)]

pub struct PyDataBlock {
    block: DataBlock,
    schema: DataSchemaRef,
    display_width: PyBoxSize,
}

#[pymethods]
impl PyDataBlock {
    fn __repr__(&self, _py: Python) -> PyResult<String> {
        let block = self.block.slice(0..10);
        let s: String = box_render(
            &self.schema,
            &[block],
            self.display_width.bs_max_display_rows,
            self.display_width.bs_max_width,
            self.display_width.bs_max_col_width,
            false,
        )
        .unwrap();
        Ok(s)
    }

    fn num_rows(&self) -> usize {
        self.block.num_rows()
    }

    fn num_columns(&self) -> usize {
        self.block.num_columns()
    }

    fn get_box(&self) -> PyBoxSize {
        self.display_width.clone()
    }
}

#[pyclass(name = "DataBlocks", module = "databend", subclass)]
pub struct PyDataBlocks {
    pub(crate) blocks: Vec<DataBlock>,
    pub(crate) schema: DataSchemaRef,
    pub(crate) display_width: PyBoxSize,
}

impl PyDataBlocks {
    pub fn box_render(&self, max_rows: usize, max_width: usize, max_col_width: usize) -> String {
        let blocks: Vec<DataBlock> = self.blocks.iter().take(10).cloned().collect();
        box_render(
            &self.schema,
            &blocks,
            max_rows,
            max_width,
            max_col_width,
            false,
        )
        .unwrap()
    }
}

#[pymethods]
impl PyDataBlocks {
    fn __repr__(&self, _py: Python) -> PyResult<String> {
        Ok(self.box_render(
            self.display_width.bs_max_display_rows,
            self.display_width.bs_max_width,
            self.display_width.bs_max_col_width,
        ))
    }

    fn num_rows(&self) -> usize {
        self.blocks.iter().map(|b| b.num_rows()).sum()
    }

    fn num_columns(&self) -> usize {
        self.schema.num_fields()
    }

    fn get_box(&self) -> PyBoxSize {
        self.display_width.clone()
    }
}
