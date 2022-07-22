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
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::StringType;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::StringDomain;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_with_writer_1_arg::<StringType, StringType, _, _>(
        "upper",
        FunctionProperty::default(),
        |arg| {
            Some(StringDomain {
                min: run_scalar(&arg.min, upper),
                max: arg.max.as_ref().map(|max| run_scalar(max, upper)),
            })
        },
        |val, writer| {
            upper(val, writer);
            Ok(())
        },
    );
    registry.register_aliases("upper", &["ucase"]);
}

fn upper(val: &[u8], writer: &mut StringColumnBuilder) {
    for (start, end, ch) in val.char_indices() {
        if ch == '\u{FFFD}' {
            // If char is not valid, just copy it.
            writer.put_slice(&val.as_bytes()[start..end]);
        } else if ch.is_ascii() {
            writer.put_u8(ch.to_ascii_uppercase() as u8);
        } else {
            for x in ch.to_uppercase() {
                writer.put_slice(x.encode_utf8(&mut [0; 4]).as_bytes());
            }
        }
    }
    writer.commit_row();
}

fn run_scalar(val: &[u8], f: impl Fn(&[u8], &mut StringColumnBuilder)) -> Vec<u8> {
    let mut builder = StringColumnBuilder::with_capacity(val.len(), 1);
    f(val, &mut builder);
    builder.build_scalar()
}
