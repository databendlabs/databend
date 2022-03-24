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

use common_exception::Result;
use common_functions::rdoc::FunctionDocAsset;
use pulldown_cmark::Options;
use pulldown_cmark::Parser;

#[test]
fn test_markdown_iter() {
    let file = FunctionDocAsset::get("01-conditional-functions/if.md").unwrap();
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_HEADING_ATTRIBUTES);
    let data = file.data.as_ref();
    let data = String::from_utf8_lossy(data);
    let parser = Parser::new_ext(data.as_ref(), options);
    for _ in parser {}
}

#[test]
fn test_parse() -> Result<()> {
    let file = FunctionDocAsset::get("05-aggregate-functions/aggregate-max.md").unwrap();

    let data = file.data.as_ref();
    let data = String::from_utf8_lossy(data);
    let (keys, doc) = FunctionDocAsset::parse_doc(data.as_ref());
    assert_eq!(keys, vec!["max"]);
    assert_eq!(doc.syntax, "MAX(expression)\n");
    Ok(())
}

#[test]
fn test_build_docs() -> Result<()> {
    let map = FunctionDocAsset::build_docs();
    let keys = map.keys();
    assert!(keys.len() > 100);
    Ok(())
}
