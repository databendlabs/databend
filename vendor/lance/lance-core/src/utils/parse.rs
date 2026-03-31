// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Parse a string into a boolean value.
pub fn str_is_truthy(val: &str) -> bool {
    val.eq_ignore_ascii_case("1")
        | val.eq_ignore_ascii_case("true")
        | val.eq_ignore_ascii_case("on")
        | val.eq_ignore_ascii_case("yes")
        | val.eq_ignore_ascii_case("y")
}
