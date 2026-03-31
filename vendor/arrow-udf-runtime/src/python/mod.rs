// Copyright 2024 RisingWave Labs
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

#![doc = include_str!("README.md")]

use crate::into_field::IntoField;
use crate::CallMode;

use self::interpreter::Interpreter;
use anyhow::{bail, Context, Result};
use arrow_array::builder::{ArrayBuilder, Int32Builder, StringBuilder};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use pyo3::types::{PyAnyMethods, PyIterator, PyModule, PyTuple};
use pyo3::{Py, PyObject};
use std::collections::HashMap;
use std::ffi::CString;
use std::fmt::Debug;
use std::sync::Arc;

// #[cfg(Py_3_12)]
mod interpreter;
mod pyarrow;

/// A runtime to execute user defined functions in Python.
///
/// # Usages
///
/// - Create a new runtime with [`Runtime::new`] or [`Runtime::builder`].
/// - For scalar functions, use [`add_function`] and [`call`].
/// - For table functions, use [`add_function`] and [`call_table_function`].
/// - For aggregate functions, create the function with [`add_aggregate`], and then
///     - create a new state with [`create_state`],
///     - update the state with [`accumulate`] or [`accumulate_or_retract`],
///     - merge states with [`merge`],
///     - finally get the result with [`finish`].
///
/// Click on each function to see the example.
///
/// # Parallelism
///
/// As we know, Python has a Global Interpreter Lock (GIL) that prevents multiple threads from executing Python code simultaneously.
/// To work around this limitation, each runtime creates a sub-interpreter with its own GIL. This feature requires Python 3.12 or later.
///
/// [`add_function`]: Runtime::add_function
/// [`add_aggregate`]: Runtime::add_aggregate
/// [`call`]: Runtime::call
/// [`call_table_function`]: Runtime::call_table_function
/// [`create_state`]: Runtime::create_state
/// [`accumulate`]: Runtime::accumulate
/// [`accumulate_or_retract`]: Runtime::accumulate_or_retract
/// [`merge`]: Runtime::merge
/// [`finish`]: Runtime::finish
pub struct Runtime {
    interpreter: Interpreter,
    functions: HashMap<String, Function>,
    aggregates: HashMap<String, Aggregate>,
    converter: pyarrow::Converter,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("functions", &self.functions.keys())
            .field("aggregates", &self.aggregates.keys())
            .finish()
    }
}

/// A user defined function.
struct Function {
    function: PyObject,
    return_field: FieldRef,
    mode: CallMode,
}

/// A user defined aggregate function.
struct Aggregate {
    state_field: FieldRef,
    output_field: FieldRef,
    mode: CallMode,
    create_state: PyObject,
    accumulate: PyObject,
    retract: Option<PyObject>,
    finish: Option<PyObject>,
    merge: Option<PyObject>,
}

/// A builder for `Runtime`.
#[derive(Default, Debug)]
pub struct Builder {
    safe_codes: Option<String>,
}

impl Builder {
    pub fn safe_codes(mut self, safe_codes: String) -> Self {
        self.safe_codes = Some(safe_codes);
        self
    }

    /// Build the `Runtime`.
    pub fn build(self) -> Result<Runtime> {
        let interpreter = Interpreter::new()?;
        interpreter.run(
            r#"
# internal use for json types
import json
import pickle
import decimal

# an internal class used for struct input arguments
class Struct:
    pass
"#,
        )?;

        if let Some(safe_codes) = self.safe_codes {
            interpreter.run(&format!("{safe_codes}"))?;
        }

        Ok(Runtime {
            interpreter,
            functions: HashMap::new(),
            aggregates: HashMap::new(),
            converter: pyarrow::Converter::new(),
        })
    }
}

impl Runtime {
    /// Create a new `Runtime`.
    pub fn new() -> Result<Self> {
        Builder::default().build()
    }

    /// Return a new builder for `Runtime`.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Add a new scalar function or table function.
    ///
    /// # Arguments
    ///
    /// - `name`: The name of the function.
    /// - `return_type`: The data type of the return value.
    /// - `mode`: Whether the function will be called when some of its arguments are null.
    /// - `code`: The Python code of the function.
    ///
    /// The code should define a function with the same name as the function.
    /// The function should return a value for scalar functions, or yield values for table functions.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_udf_runtime::python::Runtime;
    /// # use arrow_udf_runtime::CallMode;
    /// # use arrow_schema::DataType;
    /// let mut runtime = Runtime::new().unwrap();
    /// // add a scalar function
    /// runtime
    ///     .add_function(
    ///         "gcd",
    ///         DataType::Int32,
    ///         CallMode::ReturnNullOnNullInput,
    ///         r#"
    /// def gcd(a: int, b: int) -> int:
    ///     while b:
    ///         a, b = b, a % b
    ///     return a
    /// "#,
    ///     )
    ///     .unwrap();
    /// // add a table function
    /// runtime
    ///     .add_function(
    ///         "series",
    ///         DataType::Int32,
    ///         CallMode::ReturnNullOnNullInput,
    ///         r#"
    /// def series(n: int):
    ///     for i in range(n):
    ///         yield i
    /// "#,
    ///     )
    ///     .unwrap();
    /// ```
    pub fn add_function(
        &mut self,
        name: &str,
        return_type: impl IntoField,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        self.add_function_with_handler(name, return_type, mode, code, name)
    }

    /// Add a new scalar function or table function with custom handler name.
    ///
    /// # Arguments
    ///
    /// - `handler`: The name of function in Python code to be called.
    /// - others: Same as [`add_function`].
    ///
    /// [`add_function`]: Runtime::add_function
    pub fn add_function_with_handler(
        &mut self,
        name: &str,
        return_type: impl IntoField,
        mode: CallMode,
        code: &str,
        handler: &str,
    ) -> Result<()> {
        let function = self.interpreter.with_gil(|py| {
            let code = CString::new(code).unwrap();
            let name = CString::new(name).unwrap();
            Ok(PyModule::from_code(py, &code, &name, &name)?
                .getattr(handler)?
                .into())
        })?;
        let function = Function {
            function,
            return_field: return_type.into_field(name).into(),
            mode,
        };
        self.functions.insert(name.to_string(), function);
        Ok(())
    }

    /// Add a new aggregate function from Python code.
    ///
    /// # Arguments
    ///
    /// - `name`: The name of the function.
    /// - `state_type`: The data type of the internal state.
    /// - `output_type`: The data type of the aggregate value.
    /// - `mode`: Whether the function will be called when some of its arguments are null.
    /// - `code`: The Python code of the aggregate function.
    ///
    /// The code should define at least two functions:
    ///
    /// - `create_state() -> state`: Create a new state object.
    /// - `accumulate(state, *args) -> state`: Accumulate a new value into the state, returning the updated state.
    ///
    /// optionally, the code can define:
    ///
    /// - `finish(state) -> value`: Get the result of the aggregate function.
    ///   If not defined, the state is returned as the result.
    ///   In this case, `output_type` must be the same as `state_type`.
    /// - `retract(state, *args) -> state`: Retract a value from the state, returning the updated state.
    /// - `merge(state, state) -> state`: Merge two states, returning the merged state.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_udf_runtime::python::Runtime;
    /// # use arrow_udf_runtime::CallMode;
    /// # use arrow_schema::DataType;
    /// let mut runtime = Runtime::new().unwrap();
    /// runtime
    ///     .add_aggregate(
    ///         "sum",
    ///         DataType::Int32, // state_type
    ///         DataType::Int32, // output_type
    ///         CallMode::ReturnNullOnNullInput,
    ///         r#"
    /// def create_state():
    ///     return 0
    ///
    /// def accumulate(state, value):
    ///     return state + value
    ///
    /// def retract(state, value):
    ///     return state - value
    ///
    /// def merge(state1, state2):
    ///     return state1 + state2
    ///         "#,
    ///     )
    ///     .unwrap();
    /// ```
    pub fn add_aggregate(
        &mut self,
        name: &str,
        state_type: impl IntoField,
        output_type: impl IntoField,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        let aggregate = self.interpreter.with_gil(|py| {
            let c_code = CString::new(code).unwrap();
            let c_name = CString::new(name).unwrap();
            let module = PyModule::from_code(py, &c_code, &c_name, &c_name)?;
            Ok(Aggregate {
                state_field: state_type.into_field(name).into(),
                output_field: output_type.into_field(name).into(),
                mode,
                create_state: module.getattr("create_state")?.into(),
                accumulate: module.getattr("accumulate")?.into(),
                retract: module.getattr("retract").ok().map(|f| f.into()),
                finish: module.getattr("finish").ok().map(|f| f.into()),
                merge: module.getattr("merge").ok().map(|f| f.into()),
            })
        })?;
        if aggregate.finish.is_none() && aggregate.state_field != aggregate.output_field {
            bail!("`output_type` must be the same as `state_type` when `finish` is not defined");
        }
        self.aggregates.insert(name.to_string(), aggregate);
        Ok(())
    }

    /// Remove a scalar or table function.
    pub fn del_function(&mut self, name: &str) -> Result<()> {
        let function = self.functions.remove(name).context("function not found")?;
        _ = self.interpreter.with_gil(|_| {
            drop(function);
            Ok(())
        });
        Ok(())
    }

    /// Remove an aggregate function.
    pub fn del_aggregate(&mut self, name: &str) -> Result<()> {
        let aggregate = self.functions.remove(name).context("function not found")?;
        _ = self.interpreter.with_gil(|_| {
            drop(aggregate);
            Ok(())
        });
        Ok(())
    }

    /// Call a scalar function.
    ///
    /// # Example
    ///
    /// ```
    #[doc = include_str!("doc_create_function.txt")]
    /// // suppose we have created a scalar function `gcd`
    /// // see the example in `add_function`
    ///
    /// let schema = Schema::new(vec![
    ///     Field::new("x", DataType::Int32, true),
    ///     Field::new("y", DataType::Int32, true),
    /// ]);
    /// let arg0 = Int32Array::from(vec![Some(25), None]);
    /// let arg1 = Int32Array::from(vec![Some(15), None]);
    /// let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();
    ///
    /// let output = runtime.call("gcd", &input).unwrap();
    /// assert_eq!(&**output.column(0), &Int32Array::from(vec![Some(5), None]));
    /// ```
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let function = self.functions.get(name).context("function not found")?;
        // convert each row to python objects and call the function
        let (output, error) = self.interpreter.with_gil(|py| {
            let mut results = Vec::with_capacity(input.num_rows());
            let mut errors = vec![];
            let mut row = Vec::with_capacity(input.num_columns());
            for i in 0..input.num_rows() {
                if function.mode == CallMode::ReturnNullOnNullInput
                    && input.columns().iter().any(|column| column.is_null(i))
                {
                    results.push(py.None());
                    continue;
                }
                row.clear();
                for (column, field) in input.columns().iter().zip(input.schema().fields()) {
                    let pyobj = self.converter.get_pyobject(py, field, column, i)?;
                    row.push(pyobj);
                }
                let args = PyTuple::new(py, row.drain(..))?;
                match function.function.call1(py, args) {
                    Ok(result) => results.push(result),
                    Err(e) => {
                        results.push(py.None());
                        errors.push((i, e.to_string()));
                    }
                }
            }
            let output = self
                .converter
                .build_array(&function.return_field, py, &results)?;
            let error = build_error_array(input.num_rows(), errors);
            Ok((output, error))
        })?;
        if let Some(error) = error {
            let schema = Schema::new(vec![
                function.return_field.clone(),
                Field::new("error", DataType::Utf8, true).into(),
            ]);
            Ok(RecordBatch::try_new(Arc::new(schema), vec![output, error])?)
        } else {
            let schema = Schema::new(vec![function.return_field.clone()]);
            Ok(RecordBatch::try_new(Arc::new(schema), vec![output])?)
        }
    }

    /// Call a table function.
    ///
    /// # Example
    ///
    /// ```
    #[doc = include_str!("doc_create_function.txt")]
    /// // suppose we have created a table function `series`
    /// // see the example in `add_function`
    ///
    /// let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    /// let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    /// let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();
    ///
    /// let mut outputs = runtime.call_table_function("series", &input, 10).unwrap();
    /// let output = outputs.next().unwrap().unwrap();
    /// let pretty = arrow_cast::pretty::pretty_format_batches(&[output]).unwrap().to_string();
    /// assert_eq!(pretty, r#"
    /// +-----+--------+
    /// | row | series |
    /// +-----+--------+
    /// | 0   | 0      |
    /// | 2   | 0      |
    /// | 2   | 1      |
    /// | 2   | 2      |
    /// +-----+--------+"#.trim());
    /// ```
    pub fn call_table_function<'a>(
        &'a self,
        name: &'a str,
        input: &'a RecordBatch,
        chunk_size: usize,
    ) -> Result<RecordBatchIter<'a>> {
        assert!(chunk_size > 0);
        let function = self.functions.get(name).context("function not found")?;

        // initial state
        Ok(RecordBatchIter {
            interpreter: &self.interpreter,
            input,
            function,
            schema: Arc::new(Schema::new(vec![
                Field::new("row", DataType::Int32, true).into(),
                function.return_field.clone(),
            ])),
            chunk_size,
            row: 0,
            generator: None,
            converter: &self.converter,
        })
    }

    /// Create a new state for an aggregate function.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let state = runtime.create_state("sum").unwrap();
    /// assert_eq!(&*state, &Int32Array::from(vec![0]));
    /// ```
    pub fn create_state(&self, name: &str) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let state = self.interpreter.with_gil(|py| {
            let state = aggregate.create_state.call0(py)?;
            let state = self
                .converter
                .build_array(&aggregate.state_field, py, &[state])?;
            Ok(state)
        })?;
        Ok(state)
    }

    /// Call accumulate of an aggregate function.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let state = runtime.create_state("sum").unwrap();
    ///
    /// let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
    /// let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    /// let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();
    ///
    /// let state = runtime.accumulate("sum", &state, &input).unwrap();
    /// assert_eq!(&*state, &Int32Array::from(vec![9]));
    /// ```
    pub fn accumulate(
        &self,
        name: &str,
        state: &dyn Array,
        input: &RecordBatch,
    ) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        // convert each row to python objects and call the accumulate function
        let new_state = self.interpreter.with_gil(|py| {
            let mut state = self
                .converter
                .get_pyobject(py, &aggregate.state_field, state, 0)?;

            let mut row = Vec::with_capacity(1 + input.num_columns());
            for i in 0..input.num_rows() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput
                    && input.columns().iter().any(|column| column.is_null(i))
                {
                    continue;
                }
                row.clear();
                row.push(state.clone_ref(py));
                for (column, field) in input.columns().iter().zip(input.schema().fields()) {
                    let pyobj = self.converter.get_pyobject(py, field, column, i)?;
                    row.push(pyobj);
                }
                let args = PyTuple::new(py, row.drain(..))?;
                state = aggregate.accumulate.call1(py, args)?;
            }
            let output = self
                .converter
                .build_array(&aggregate.state_field, py, &[state])?;
            Ok(output)
        })?;
        Ok(new_state)
    }

    /// Call accumulate or retract of an aggregate function.
    ///
    /// The `ops` is a boolean array that indicates whether to accumulate or retract each row.
    /// `false` for accumulate and `true` for retract.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let state = runtime.create_state("sum").unwrap();
    ///
    /// let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
    /// let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    /// let ops = BooleanArray::from(vec![false, false, true, false]);
    /// let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();
    ///
    /// let state = runtime.accumulate_or_retract("sum", &state, &ops, &input).unwrap();
    /// assert_eq!(&*state, &Int32Array::from(vec![3]));
    /// ```
    pub fn accumulate_or_retract(
        &self,
        name: &str,
        state: &dyn Array,
        ops: &BooleanArray,
        input: &RecordBatch,
    ) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let retract = aggregate
            .retract
            .as_ref()
            .context("function does not support retraction")?;
        // convert each row to python objects and call the accumulate function
        let new_state = self.interpreter.with_gil(|py| {
            let mut state = self
                .converter
                .get_pyobject(py, &aggregate.state_field, state, 0)?;

            let mut row = Vec::with_capacity(1 + input.num_columns());
            for i in 0..input.num_rows() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput
                    && input.columns().iter().any(|column| column.is_null(i))
                {
                    continue;
                }
                row.clear();
                row.push(state.clone_ref(py));
                for (column, field) in input.columns().iter().zip(input.schema().fields()) {
                    let pyobj = self.converter.get_pyobject(py, field, column, i)?;
                    row.push(pyobj);
                }
                let args = PyTuple::new(py, row.drain(..))?;
                let func = if ops.is_valid(i) && ops.value(i) {
                    retract
                } else {
                    &aggregate.accumulate
                };
                state = func.call1(py, args)?;
            }
            let output = self
                .converter
                .build_array(&aggregate.state_field, py, &[state])?;
            Ok(output)
        })?;
        Ok(new_state)
    }

    /// Merge states of an aggregate function.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let states = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    ///
    /// let state = runtime.merge("sum", &states).unwrap();
    /// assert_eq!(&*state, &Int32Array::from(vec![9]));
    /// ```
    pub fn merge(&self, name: &str, states: &dyn Array) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let merge = aggregate.merge.as_ref().context("merge not found")?;
        let output = self.interpreter.with_gil(|py| {
            let mut state = self
                .converter
                .get_pyobject(py, &aggregate.state_field, states, 0)?;
            for i in 1..states.len() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput && states.is_null(i) {
                    continue;
                }
                let state2 = self
                    .converter
                    .get_pyobject(py, &aggregate.state_field, states, i)?;
                let args = PyTuple::new(py, [state, state2])?;
                state = merge.call1(py, args)?;
            }
            let output = self
                .converter
                .build_array(&aggregate.state_field, py, &[state])?;
            Ok(output)
        })?;
        Ok(output)
    }

    /// Get the result of an aggregate function.
    ///
    /// If the `finish` function is not defined, the state is returned as the result.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let states: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(5)]));
    ///
    /// let outputs = runtime.finish("sum", &states).unwrap();
    /// assert_eq!(&outputs, &states);
    /// ```
    pub fn finish(&self, name: &str, states: &ArrayRef) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let Some(finish) = &aggregate.finish else {
            return Ok(states.clone());
        };
        let output = self.interpreter.with_gil(|py| {
            let mut results = Vec::with_capacity(states.len());
            for i in 0..states.len() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput && states.is_null(i) {
                    results.push(py.None());
                    continue;
                }
                let state = self
                    .converter
                    .get_pyobject(py, &aggregate.state_field, states, i)?;
                let args = PyTuple::new(py, [state])?;
                let result = finish.call1(py, args)?;
                results.push(result);
            }
            let output = self
                .converter
                .build_array(&aggregate.output_field, py, &results)?;
            Ok(output)
        })?;
        Ok(output)
    }
}

/// An iterator over the result of a table function.
pub struct RecordBatchIter<'a> {
    interpreter: &'a Interpreter,
    input: &'a RecordBatch,
    function: &'a Function,
    schema: SchemaRef,
    chunk_size: usize,
    // mutable states
    /// Current row index.
    row: usize,
    /// Generator of the current row.
    generator: Option<Py<PyIterator>>,
    converter: &'a pyarrow::Converter,
}

impl RecordBatchIter<'_> {
    /// Get the schema of the output.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.row == self.input.num_rows() {
            return Ok(None);
        }
        let batch = self.interpreter.with_gil(|py| {
            let mut indexes = Int32Builder::with_capacity(self.chunk_size);
            let mut results = Vec::with_capacity(self.input.num_rows());
            let mut errors = vec![];
            let mut row = Vec::with_capacity(self.input.num_columns());
            while self.row < self.input.num_rows() && results.len() < self.chunk_size {
                let generator = if let Some(g) = self.generator.as_ref() {
                    g
                } else {
                    // call the table function to get a generator
                    if self.function.mode == CallMode::ReturnNullOnNullInput
                        && (self.input.columns().iter()).any(|column| column.is_null(self.row))
                    {
                        self.row += 1;
                        continue;
                    }
                    row.clear();
                    for (column, field) in
                        (self.input.columns().iter()).zip(self.input.schema().fields())
                    {
                        let val = self.converter.get_pyobject(py, field, column, self.row)?;
                        row.push(val);
                    }
                    let args = PyTuple::new(py, row.drain(..))?;
                    match self.function.function.bind(py).call1(args) {
                        Ok(result) => {
                            let iter = result.try_iter()?.into();
                            self.generator.insert(iter)
                        }
                        Err(e) => {
                            // append a row with null value and error message
                            indexes.append_value(self.row as i32);
                            results.push(py.None());
                            errors.push((indexes.len(), e.to_string()));
                            self.row += 1;
                            continue;
                        }
                    }
                };
                match generator.bind(py).clone().next() {
                    Some(Ok(value)) => {
                        indexes.append_value(self.row as i32);
                        results.push(value.into());
                    }
                    Some(Err(e)) => {
                        indexes.append_value(self.row as i32);
                        results.push(py.None());
                        errors.push((indexes.len(), e.to_string()));
                        self.row += 1;
                        self.generator = None;
                    }
                    None => {
                        self.row += 1;
                        self.generator = None;
                    }
                }
            }

            if results.is_empty() {
                return Ok(None);
            }
            let indexes = Arc::new(indexes.finish());
            let output = self
                .converter
                .build_array(&self.function.return_field, py, &results)
                .context("failed to build arrow array from return values")?;
            let error = build_error_array(indexes.len(), errors);
            if let Some(error) = error {
                Ok(Some(
                    RecordBatch::try_new(
                        Arc::new(append_error_to_schema(&self.schema)),
                        vec![indexes, output, error],
                    )
                    .unwrap(),
                ))
            } else {
                Ok(Some(
                    RecordBatch::try_new(self.schema.clone(), vec![indexes, output]).unwrap(),
                ))
            }
        })?;
        Ok(batch)
    }
}

impl Iterator for RecordBatchIter<'_> {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next().transpose()
    }
}

impl Drop for RecordBatchIter<'_> {
    fn drop(&mut self) {
        if let Some(generator) = self.generator.take() {
            _ = self.interpreter.with_gil(|_| {
                drop(generator);
                Ok(())
            });
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // `PyObject` must be dropped inside the interpreter
        _ = self.interpreter.with_gil(|_| {
            self.functions.clear();
            self.aggregates.clear();
            Ok(())
        });
    }
}

fn build_error_array(num_rows: usize, errors: Vec<(usize, String)>) -> Option<ArrayRef> {
    if errors.is_empty() {
        return None;
    }
    let data_capacity = errors.iter().map(|(i, _)| i).sum();
    let mut builder = StringBuilder::with_capacity(num_rows, data_capacity);
    for (i, msg) in errors {
        while builder.len() + 1 < i {
            builder.append_null();
        }
        builder.append_value(&msg);
    }
    while builder.len() < num_rows {
        builder.append_null();
    }
    Some(Arc::new(builder.finish()))
}

/// Append an error field to the schema.
fn append_error_to_schema(schema: &Schema) -> Schema {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new("error", DataType::Utf8, true)));
    Schema::new(fields)
}
