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

use databend_common_expression::types::map::KvColumnBuilder;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::MapType;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg(
        "map_cat",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<
            MapType<GenericType<0>, GenericType<1>>,
            MapType<GenericType<0>, GenericType<1>>,
            MapType<GenericType<0>, GenericType<1>>,
        >(|input_map1, input_map2, output_map, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output_map.len()) {
                    output_map.commit_row();
                    return;
                }
            }

            log::info!("map_cat, input_map1 ...");
            input_map1
                .iter()
                .for_each(|(key, value)| log::info!("K :: {:#?}, V :: {:#?}", key, value));

            log::info!("map_cat, input_map2 ...");
            input_map2
                .iter()
                .for_each(|(key, value)| log::info!("K :: {:#?}, V :: {:#?}", key, value));

            let mut concatenated_map_builder = KvColumnBuilder::from_column(input_map1);
            concatenated_map_builder.append_column(&input_map2);
            let concatenated_map = KvColumnBuilder::build(concatenated_map_builder);

            log::info!("map_cat ...");
            for (key, value) in concatenated_map.iter() {
                log::info!("K :: {:#?}, V :: {:#?}", key, value);
                output_map.put_item((key, value));
            }

            output_map.commit_row();
        }),
    );
}
