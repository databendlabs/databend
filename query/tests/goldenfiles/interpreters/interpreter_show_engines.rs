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

use common_base::tokio;
use common_exception::Result;
use goldenfile::Mint;

use crate::interpreters::interpreter_goldenfiles;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_engines_interpreter() -> Result<()> {
    let mut mint = Mint::new("tests/goldenfiles/data");
    let mut file = mint.new_goldenfile("show-engines.txt").unwrap();

    let ctx = crate::tests::create_query_context().await?;

    interpreter_goldenfiles(
        &mut file,
        ctx.clone(),
        "ShowEnginesInterpreter",
        r#"show engines"#,
    )
    .await?;

    Ok(())
}
