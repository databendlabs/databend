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

use std::sync::Arc;

use arrow::pyarrow::PyArrowType;
use arrow::pyarrow::ToPyArrow;
use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sql::plans::Plan;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use tokio_stream::StreamExt;

use crate::datablock::PyDataBlocks;
use crate::schema::PySchema;
use crate::utils::wait_for_future;

#[pyclass(name = "BoxSize", module = "databend", subclass, skip_from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct PyBoxSize {
    pub(crate) bs_max_display_rows: usize,
    pub(crate) bs_max_width: usize,
    pub(crate) bs_max_col_width: usize,
}

#[pyclass(name = "DataFrame", module = "databend", subclass, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyDataFrame {
    ctx: Arc<QueryContext>,
    pub(crate) df: Plan,
    display_width: PyBoxSize,
}

impl PyDataFrame {
    /// creates a new PyDataFrame
    pub fn new(ctx: Arc<QueryContext>, df: Plan, display_width: PyBoxSize) -> Self {
        Self {
            ctx,
            df,
            display_width,
        }
    }

    async fn df_collect(&self) -> Result<Vec<DataBlock>> {
        let interpreter = InterpreterFactory::get(self.ctx.clone(), &self.df).await?;
        let stream = interpreter.execute(self.ctx.clone()).await?;
        stream.collect::<Result<Vec<_>>>().await
    }
}

#[pymethods]
impl PyDataFrame {
    fn __repr__(&self, py: Python) -> PyResult<String> {
        let blocks = self.collect(py)?;
        let bs = self.get_box();
        Ok(blocks.box_render(bs.bs_max_display_rows, bs.bs_max_width, bs.bs_max_width))
    }

    #[pyo3(signature = (num=20))]
    fn show(&self, py: Python, num: usize) -> PyResult<()> {
        let blocks = self.collect(py)?;
        let bs = self.get_box();
        let result = blocks.box_render(num, bs.bs_max_width, bs.bs_max_width);

        // Note that println! does not print to the Python debug console and is not visible in notebooks for instance
        let print = py.import("builtins")?.getattr("print")?;
        print.call1((result,))?;
        Ok(())
    }

    pub fn collect(&self, py: Python) -> PyResult<PyDataBlocks> {
        let blocks = wait_for_future(py, self.df_collect());
        let display_width = self.get_box();
        let blocks = blocks.map_err(|err| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("DataFrame collect error: {:?}", err))
        })?;

        Ok(PyDataBlocks {
            blocks,
            schema: self.df.schema(),
            display_width,
        })
    }

    pub fn get_box(&self) -> PyBoxSize {
        self.display_width.clone()
    }

    fn set_max_display_rows(&mut self, max_display_rows: usize) {
        self.display_width.bs_max_display_rows = max_display_rows;
    }

    fn set_max_width(&mut self, max_width: usize) {
        self.display_width.bs_max_width = max_width;
    }

    fn set_max_col_width(&mut self, max_col_width: usize) {
        self.display_width.bs_max_col_width = max_col_width;
    }

    pub fn schema(&self) -> PySchema {
        PySchema {
            schema: self.df.schema(),
        }
    }

    pub fn to_py_arrow(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let blocks = wait_for_future(py, self.df_collect());
        let blocks = blocks.map_err(|err| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("DataFrame collect error: {:?}", err))
        })?;

        blocks
            .into_iter()
            .map(|block| {
                block
                    .to_record_batch_with_dataschema(self.df.schema().as_ref())
                    .unwrap()
                    .to_pyarrow(py)
                    .map(Bound::unbind)
            })
            .collect()
    }

    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    pub fn to_arrow_table(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let batches = self.to_py_arrow(py)?.into_pyobject(py)?;
        let schema = ArrowSchema::from(self.df.schema().as_ref());
        let schema = PyArrowType(schema);
        let schema = schema.into_pyobject(py)?;

        let table_class = py.import("pyarrow")?.getattr("Table")?;
        Ok(table_class
            .call_method1("from_batches", (batches, schema))?
            .unbind())
    }

    /// Convert to pandas dataframe with pyarrow
    /// Collect the batches, pass to Arrow Table & then convert to Pandas DataFrame
    fn to_pandas(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let table = self.to_arrow_table(py)?;
        table.call_method0(py, "to_pandas")
    }

    /// Convert to polars dataframe with pyarrow
    /// Collect the batches, pass to Arrow Table & then convert to polars DataFrame
    fn to_polars(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let table = self.to_arrow_table(py)?;
        let dataframe = py.import("polars")?.getattr("DataFrame")?;
        Ok(dataframe.call1((table,))?.unbind())
    }
}

pub(crate) fn default_box_size() -> PyBoxSize {
    PyBoxSize {
        bs_max_display_rows: 40,
        bs_max_width: 0,
        bs_max_col_width: 20,
    }
}
