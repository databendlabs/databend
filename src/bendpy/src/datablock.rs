use common_expression::DataBlock;
use pyo3::prelude::*;

#[pyclass]

pub struct Block(pub DataBlock);

#[pymethods]
impl Block {
    fn show(&self) {
        println!("{}", self.0.to_string());
    }

    fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    fn num_columns(&self) -> usize {
        self.0.num_columns()
    }
}
