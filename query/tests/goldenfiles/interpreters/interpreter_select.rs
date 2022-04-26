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

use common_base::tokio;
use common_exception::Result;
use goldenfile::Mint;

use crate::interpreters::interpreter_goldenfiles;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let mut mint = Mint::new("tests/goldenfiles/data");
    let mut file = mint.new_goldenfile("select.txt").unwrap();

    let ctx = crate::tests::create_query_context().await?;

    let cases = &[
        r#"select number from numbers_mt(10)"#,
        r#"select 1 + 1, 2 + 2, 3 * 3, 4 * 4"#,
    ];

    for case in cases {
        interpreter_goldenfiles(&mut file, ctx.clone(), "SelectInterpreter", case).await?;
    }

    Ok(())
}
