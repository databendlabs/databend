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

use common_functions::scalars::FunctionFactory;
use handlebars::no_escape;
use handlebars::to_json;
use handlebars::Handlebars;
use serde_json::value::Map;

pub fn codegen_function_doc() -> Result<(), Box<dyn Error>> {
    let function_factory = FunctionFactory::instance();
    let names = function_factory.registered_names();
    let features = function_factory.registered_features();

    let mut handlebars = Handlebars::new();
    handlebars.register_escape_fn(no_escape);

    handlebars.register_template_file("function", "common/codegen/src/template/function.hbs")?;

    for (name, feature) in names.iter().zip(features.iter()) {
        if feature.category != "Conditional" {
            continue;
        }
        let mut data = Map::new();
        data.insert("name".to_string(), to_json(name));
        data.insert("features".to_string(), to_json(feature));

        let result = handlebars.render("function", &data)?;
        // println!("{}", result);
        let path = "docs/doc/03-reference/02-functions/01-conditional-functions/".to_string()
            + name
            + "_new.md";
        let mut file = File::create(&path).expect("open");
        file.write_all(result.as_bytes())?;
    }

    Ok(())
}
