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

use bstr::ByteSlice;
use common_expression::types::variant::DEFAULT_JSONB;
use common_expression::types::StringType;
use common_expression::types::VariantType;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_jsonb::parse_value;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, VariantType, _, _>(
        "parse_json",
        FunctionProperty::default(),
        |_| None,
        vectorize_with_builder_1_arg::<StringType, VariantType>(|s, output| {
            if s.trim().is_empty() {
                output.put_slice(DEFAULT_JSONB);
                output.commit_row();
                return Ok(());
            }
            let value = parse_value(s).map_err(|err| {
                format!("unable to parse '{}': {}", &String::from_utf8_lossy(s), err)
            })?;
            let mut buf: Vec<u8> = Vec::new();
            value
                .to_writer(&mut buf)
                .map_err(|_| "unable to encode jsonb".to_string())?;
            output.put_slice(buf.as_slice());
            output.commit_row();

            Ok(())
        }),
    );
}
