// Copyright 2020 Datafuse Labs.
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

use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;

use backtrace::Backtrace;
use serde::Deserialize;
use serde::Serialize;

/// AnyError is a serializable wrapper `Error`.
///
/// It is can be used to convert other `Error` into a serializable Error for transmission,
/// with most necessary info kept.
#[derive(Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct AnyError {
    typ: Option<String>,
    msg: String,
    source: Option<Box<AnyError>>,
    backtrace: Option<String>,
}

impl Display for AnyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        if let Some(t) = &self.typ {
            write!(f, "{}: ", t)?;
        }

        write!(f, "{}", self.msg)?;

        if let Some(ref s) = self.source {
            write!(f, " source: {}", s)?;
        }

        Ok(())
    }
}

impl std::fmt::Debug for AnyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        <Self as Display>::fmt(self, f)?;

        if let Some(ref b) = self.backtrace {
            write!(f, "\nbacktrace:\n{}", b)?;
        }
        Ok(())
    }
}

impl std::error::Error for AnyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            Some(x) => Some(x.as_ref()),
            None => None,
        }
    }
}

impl From<anyhow::Error> for AnyError {
    fn from(a: anyhow::Error) -> Self {
        AnyError::from_dyn(a.as_ref(), None)
    }
}

impl AnyError {
    /// Convert some `Error` to AnyError.
    ///
    /// - If there is a `source()` in the input error, it is also converted to AnyError, recursively.
    /// - A new backtrace will be built if there is not.
    pub fn new<E>(e: &E) -> Self
    where E: Error + 'static {
        let q: &(dyn Error + 'static) = e;

        let x = q.downcast_ref::<AnyError>();
        let typ = match x {
            Some(ae) => ae.typ.clone(),
            None => Some(std::any::type_name::<E>().to_string()),
        };

        Self::from_dyn(e, typ)
    }

    pub fn from_dyn(e: &(dyn Error + 'static), typ: Option<String>) -> Self {
        let x = e.downcast_ref::<AnyError>();

        return match x {
            Some(ae) => {
                let mut res = ae.clone();
                if res.backtrace.is_none() {
                    res.backtrace = Some(format!("{:?}", Backtrace::new()));
                }
                res
            }
            None => {
                let bt = match e.backtrace() {
                    Some(b) => Some(format!("{:?}", b)),
                    None => Some(format!("{:?}", Backtrace::new())),
                };

                let source = e.source().map(|x| Box::new(AnyError::from_dyn(x, None)));

                Self {
                    typ,
                    msg: e.to_string(),
                    source,
                    backtrace: bt,
                }
            }
        };
    }

    pub fn get_type(&self) -> Option<&str> {
        self.typ.as_ref().map(|x| x as _)
    }

    pub fn backtrace(&self) -> Option<&str> {
        self.backtrace.as_ref().map(|x| x as _)
    }
}
