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

use std::error::Error;
use std::fs::File;
use std::io::Write;

use common_functions::rdoc::FunctionDocAsset;
use common_functions::scalars::FunctionFactory;
use handlebars::no_escape;
use handlebars::to_json;
use handlebars::Handlebars;
use serde_json::value::Map;

#[allow(dead_code)]
pub fn codegen_function_doc() -> Result<(), Box<dyn Error>> {
    let function_factory = FunctionFactory::instance();
    let names = function_factory.registered_names();

    let mut handlebars = Handlebars::new();
    handlebars.register_escape_fn(no_escape);

    handlebars.register_template_file("function", "common/codegen/src/template/function.hbs")?;

    for name in names.iter() {
        if name != "Conditional" {
            continue;
        }
        let doc = FunctionDocAsset::get_doc(name);
        let mut data = Map::new();
        data.insert("name".to_string(), to_json(name));
        data.insert("doc".to_string(), to_json(doc));

        let result = handlebars.render("function", &data)?;
        let path = "docs/doc/03-reference/02-functions/01-conditional-functions/".to_string()
            + name
            + "_new.md";
        let mut file = File::create(&path).expect("open");
        file.write_all(result.as_bytes())?;
    }

    Ok(())
}
