// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// for settings getter setter
macro_rules! apply_getter_setter_settings {
    ($(($NAME: expr, $TYPE: tt, $VALUE:expr, $DESC: expr)),* ) => {
        $(
            paste::paste!{
                pub fn [< get_ $NAME >](&self) -> FuseQueryResult<$TYPE> {
                    self.settings.[<try_get_ $TYPE:lower>]($NAME)
                }

                pub fn [< set_ $NAME >](&self, value: $TYPE) -> FuseQueryResult<()> {
                    self.settings.[<try_update_ $TYPE:lower>]($NAME, value)
                }
            }
        )*
    };
}

macro_rules! apply_initial_settings {
    ($(($NAME: expr, $TYPE: tt, $VALUE:expr, $DESC: expr)),* ) => {

        pub fn initial_settings(&self) -> FuseQueryResult<()> {
            paste::paste! {
                $(
                    self.settings.[<try_set_ $TYPE:lower>]($NAME, $VALUE, $DESC)?;
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
        $VALUE.parse::<$TYPE>()?
    };
}

macro_rules! apply_update_settings {
    ($(($NAME: expr, $TYPE: tt, $VALUE:expr, $DESC: expr)),* ) => {
        pub fn update_settings(&self, key: &str, value: String) -> FuseQueryResult<()> {
            paste::paste! {
                $(
                    if (key.to_lowercase().as_str() == $NAME) {
                        let v = apply_parse_value!{value, $TYPE};
                        return self.settings.[<try_update_ $TYPE:lower>]($NAME, v);
                    }
                )*
            }
            Err(FuseQueryError::build_internal_error(format!(
                "Unknown variable: {:?}",
                key
            )))
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
