// Copyright [2021] [Jorge C Leitao]
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

mod from_thrift;
#[allow(unused_imports)]
pub use from_thrift::*;

mod to_thrift;
#[allow(unused_imports)]
pub use to_thrift::*;

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::schema::io_message::from_message;
    use crate::schema::types::ParquetType;

    fn test_round_trip(message: &str) -> Result<()> {
        let expected_schema = from_message(message)?;
        let thrift_schema = expected_schema.to_thrift();
        let thrift_schema = thrift_schema.into_iter().collect::<Vec<_>>();
        let result_schema = ParquetType::try_from_thrift(&thrift_schema)?;
        assert_eq!(result_schema, expected_schema);
        Ok(())
    }

    #[test]
    fn test_schema_type_thrift_conversion() {
        let message_type = "
    message conversions {
      REQUIRED INT64 id;
      OPTIONAL group int_array_Array (LIST) {
        REPEATED group list {
          OPTIONAL group element (LIST) {
            REPEATED group list {
              OPTIONAL INT32 element;
            }
          }
        }
      }
      OPTIONAL group int_map (MAP) {
        REPEATED group map (MAP_KEY_VALUE) {
          REQUIRED BYTE_ARRAY key (UTF8);
          OPTIONAL INT32 value;
        }
      }
      OPTIONAL group int_Map_Array (LIST) {
        REPEATED group list {
          OPTIONAL group g (MAP) {
            REPEATED group map (MAP_KEY_VALUE) {
              REQUIRED BYTE_ARRAY key (UTF8);
              OPTIONAL group value {
                OPTIONAL group H {
                  OPTIONAL group i (LIST) {
                    REPEATED group list {
                      OPTIONAL DOUBLE element;
                    }
                  }
                }
              }
            }
          }
        }
      }
      OPTIONAL group nested_struct {
        OPTIONAL INT32 A;
        OPTIONAL group b (LIST) {
          REPEATED group list {
            REQUIRED FIXED_LEN_BYTE_ARRAY (16) element;
          }
        }
      }
    }
    ";
        test_round_trip(message_type).unwrap();
    }

    #[test]
    fn test_schema_type_thrift_conversion_decimal() {
        let message_type = "
    message decimals {
      OPTIONAL INT32 field0;
      OPTIONAL INT64 field1 (DECIMAL (18, 2));
      OPTIONAL FIXED_LEN_BYTE_ARRAY (16) field2 (DECIMAL (38, 18));
      OPTIONAL BYTE_ARRAY field3 (DECIMAL (9));
    }
    ";
        test_round_trip(message_type).unwrap();
    }
}
