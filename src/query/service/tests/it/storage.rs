// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio;
use common_base::runtime::catch_unwind;
use common_exception::Result;
use common_storage::DataOperator;

use crate::tests::ConfigBuilder;
use crate::tests::TestGlobalServices;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_data_operator_under_panicking() -> Result<()> {
    let config = ConfigBuilder::create().build();
    let _guard = TestGlobalServices::setup(config.clone()).await?;

    let result = catch_unwind(move || {
        let _op = DataOperator::instance().operator();
        panic!("Ooooooops!");
    });
    assert!(result.is_err());

    // The operator fetched from data operator will always be clear.
    assert!(!DataOperator::instance().operator().is_poisoned());

    Ok(())
}
