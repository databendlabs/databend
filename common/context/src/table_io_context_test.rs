// Copyright 2021 Datafuse Labs.
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

use std::fmt::Debug;
use std::sync::Arc;

use common_base::Runtime;
use common_dal::DataAccessor;
use common_dal::DataAccessorBuilder;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::IOContext;
use crate::TableIOContext;

/// A default DataAccessorBuilder impl.
#[derive(Debug)]
pub struct TestDataAccessorBuilder {}

impl DataAccessorBuilder for TestDataAccessorBuilder {
    fn build(&self) -> Result<Arc<dyn DataAccessor>> {
        // we do not use it in unit test
        todo!()
    }
}

#[test]
fn test_table_io_context() -> Result<()> {
    let c = TableIOContext::new(
        Arc::new(Runtime::with_default_worker_threads()?),
        Arc::new(TestDataAccessorBuilder {}),
        3,
        vec![],
        Some(Arc::new("foo".to_string())),
    );

    let s: Arc<String> = c.get_user_data()?.unwrap();
    assert_eq!(Arc::new("foo".to_string()), s);

    let s: Result<Option<Arc<u32>>> = c.get_user_data();
    let e = s.unwrap_err();
    assert_eq!(ErrorCode::InvalidCast("").code(), e.code());

    Ok(())
}
