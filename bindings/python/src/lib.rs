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

mod asyncio;

use crate::asyncio::*;

use databend_driver::{new_connection, Connection};

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::sync::Arc;
create_exception!(
    databend_client,
    Error,
    PyException,
    "databend_client related errors"
);

#[derive(Clone)]
pub struct Connector {
    pub connector: FusedConnector,
}

pub type FusedConnector = Arc<dyn Connection>;

// For bindings
impl Connector {
    pub fn new_connector(dsn: &str) -> Result<Box<Self>, Error> {
        let conn = new_connection(dsn).unwrap();
        let r = Self {
            connector: FusedConnector::from(conn),
        };
        Ok(Box::new(r))
    }
}

fn build_connector(dsn: &str) -> PyResult<Connector> {
    let conn = Connector::new_connector(dsn).unwrap();
    Ok(*conn)
}

#[pymodule]
fn _databend_driver(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<AsyncDatabendDriver>()?;
    Ok(())
}
