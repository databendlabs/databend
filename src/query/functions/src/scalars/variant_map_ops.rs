// Copyright 2021 Datafuse Labs
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

use databend_common_expression::types::VariantType;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use jsonb::concat;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg(
        "map_cat",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, VariantType, VariantType>(
            |map1, map2, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.commit_row();
                        return;
                    }
                }

                if let Err(err) = concat(map1, map2, &mut output.data) {
                    ctx.set_error(output.len(), err.to_string());
                };

                output.commit_row();
            },
        ),
    );
}
