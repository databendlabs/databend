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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::Config;

// Obsoleted configuration entries checking
//
// The following code should be removed from the release after the next release.
// Just give user errors without any detail explanation and migration suggestions.
impl Config {
    pub const fn obsoleted_option_keys() -> &'static [&'static str; 1] {
        &["obsoleted-name"]
    }

    pub(crate) fn check_obsoleted(&self) -> Result<()> {
        let check_results = vec![
            // This is a demo.
            Self::check(
                &Some("obsoleted-name"),
                "obsoleted-name",
                "new-name",
                r#"
                    ...
                    name-name = "value"
                  "#,
                "NEW-NAME",
            ),
        ];

        let messages = check_results.into_iter().flatten().collect::<Vec<_>>();
        if !messages.is_empty() {
            let errors = messages.join("\n");
            Err(ErrorCode::InvalidConfig(format!("\n{errors}")))
        } else {
            Ok(())
        }
    }

    fn check<T>(
        target: &Option<T>,
        cli_arg_name: &str,
        alternative_cli_arg: &str,
        alternative_toml_config: &str,
        alternative_env_var: &str,
    ) -> Option<String> {
        target.as_ref().map(|_| {
            format!(
                "
 --------------------------------------------------------------
 *** {cli_arg_name} *** is obsoleted :
 --------------------------------------------------------------
   alternative command-line options : {alternative_cli_arg}
   alternative environment variable : {alternative_env_var}
            alternative toml config : {alternative_toml_config}
 --------------------------------------------------------------
"
            )
        })
    }
}
