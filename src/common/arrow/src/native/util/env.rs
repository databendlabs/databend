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

use std::env;

pub const DICT_ENV: &str = "STRAWBOAT_DICT_COMPRESSION";
pub const RLE_ENV: &str = "STRAWBOAT_RLE_COMPRESSION";
pub const FREQ_ENV: &str = "STRAWBOAT_FREQ_COMPRESSION";
pub const BITPACK_ENV: &str = "STRAWBOAT_BITPACK_COMPRESSION";
pub const PATAS_ENV: &str = "STRAWBOAT_PATAS_COMPRESSION";

pub fn check_env(env: &str) -> bool {
    match env::var(env) {
        Ok(v) => v == "1",
        Err(_) => false,
    }
}

pub fn check_dict_env() -> bool {
    check_env(DICT_ENV)
}

pub fn check_rle_env() -> bool {
    check_env(RLE_ENV)
}

pub fn check_freq_env() -> bool {
    check_env(FREQ_ENV)
}

pub fn check_bitpack_env() -> bool {
    check_env(BITPACK_ENV)
}

pub fn check_patas_env() -> bool {
    check_env(PATAS_ENV)
}

pub fn remove_env(env: &str) {
    env::remove_var(env);
}

pub fn remove_all_env() {
    remove_env(DICT_ENV);
    remove_env(RLE_ENV);
    remove_env(FREQ_ENV);
    remove_env(BITPACK_ENV);
    remove_env(PATAS_ENV);
}

pub fn set_dict_env() {
    env::set_var(DICT_ENV, "1");
}

pub fn set_rle_env() {
    env::set_var(RLE_ENV, "1");
}

pub fn set_freq_env() {
    env::set_var(FREQ_ENV, "1");
}

pub fn set_bitpack_env() {
    env::set_var(BITPACK_ENV, "1");
}

pub fn set_patas_env() {
    env::set_var(PATAS_ENV, "1");
}
