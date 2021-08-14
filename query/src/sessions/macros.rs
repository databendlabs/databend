// Copyright 2020 Datafuse Labs.
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

// for settings getter setter
macro_rules! apply_getter_setter_settings {
    ($(($NAME: expr, $TYPE: tt, $VALUE:expr, $DESC: expr)),* ) => {
        $(
            paste::paste!{
                pub fn [< get_ $NAME >](&self) -> Result<$TYPE> {
                    self.inner.[<try_get_ $TYPE:lower>]($NAME)
                }

                pub fn [< set_ $NAME >](&self, value: $TYPE) -> Result<()> {
                    self.inner.[<try_update_ $TYPE:lower>]($NAME, value)
                }
            }
        )*
    };
}

macro_rules! apply_initial_settings {
    ($(($NAME: expr, $TYPE: tt, $VALUE:expr, $DESC: expr)),* ) => {

        pub fn initial_settings(&self) -> Result<()> {
            paste::paste! {
                $(
                    self.inner.[<try_set_ $TYPE:lower>]($NAME, $VALUE, $DESC)?;
                )*
            }
            Ok(())
        }
    };
}

macro_rules! apply_parse_value {
    ($VALUE: expr, String) => {
        $VALUE
    };

    ($VALUE: expr, $TYPE: tt) => {
        $VALUE.parse::<$TYPE>().map_err(ErrorCode::from)?
    };
}

macro_rules! apply_update_settings {
    ($(($NAME: expr, $TYPE: tt, $VALUE:expr, $DESC: expr)),* ) => {
        pub fn update_settings(&self, key: &str, value: String) -> Result<()> {
            paste::paste! {
                $(
                    if (key.to_lowercase().as_str() == $NAME) {
                        let v = apply_parse_value!{value, $TYPE};
                        return self.inner.[<try_update_ $TYPE:lower>]($NAME, v);
                    }
                )*
            }
            Err(ErrorCode::UnknownVariable(
                format!("Unknown variable: {:?}", key)
            ))
        }
    };
}

macro_rules! apply_macros {
    ($MACRO_A: ident, $MACRO_B: ident, $MACRO_C: ident, $(($NAME: expr, $TYPE: tt, $VALUE:expr, $DESC: expr)),* ) => {
        $MACRO_A! { $( ($NAME, $TYPE, $VALUE, $DESC) ), * }
        $MACRO_B! { $( ($NAME, $TYPE, $VALUE, $DESC) ), * }
        $MACRO_C! { $( ($NAME, $TYPE, $VALUE, $DESC) ), * }
    };
}
