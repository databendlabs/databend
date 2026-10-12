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

use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use serde::Deserialize;

use crate::error::DSqlLogicTestError;
use crate::error::Result;

const HOOK_FILE: &str = "hook.toml";

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Hook {
    pub name: String,
    pub prepare: Vec<String>,
    #[serde(default)]
    pub cleanup: Vec<String>,
    #[serde(skip)]
    path: PathBuf,
}

#[derive(Debug, Default)]
pub struct Hooks {
    hooks: Vec<Hook>,
}

impl Hooks {
    /// Discover at most one hook for each direct child suite of `suites`.
    /// Files outside that root deliberately have no hook.
    pub fn discover(suites: &Path, files: &[PathBuf]) -> Result<Self> {
        if !suites.is_dir() {
            return Ok(Self::default());
        }
        let suites = suites.canonicalize()?;
        let mut hooks = Vec::new();
        for entry in std::fs::read_dir(&suites)? {
            let entry = entry?;
            let suite = entry.path();
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let suite = suite.canonicalize()?;
            if !files.iter().any(|file| is_under(file, &suite)) {
                continue;
            }

            let hook_path = suite.join(HOOK_FILE);
            if hook_path.is_file() {
                hooks.push(Hook::load(hook_path)?);
            }
        }
        hooks.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(Self { hooks })
    }

    #[cfg(test)]
    fn from_hooks(hooks: Vec<Hook>) -> Self {
        Self { hooks }
    }

    pub fn prepare(&self, env: &[(&str, String)]) -> Result<usize> {
        for (index, hook) in self.hooks.iter().enumerate() {
            if let Err(error) = run_command(hook, &hook.prepare, env, "prepare") {
                let cleanup_error = self.cleanup(index + 1, env).err();
                return Err(with_cleanup_error(error, cleanup_error));
            }
        }
        Ok(self.hooks.len())
    }

    pub fn cleanup(&self, prepared: usize, env: &[(&str, String)]) -> Result<()> {
        let mut errors = Vec::new();
        for hook in self.hooks[..prepared].iter().rev() {
            if let Err(error) = run_command(hook, &hook.cleanup, env, "cleanup") {
                errors.push(error.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(DSqlLogicTestError::SelfError(errors.join("; ")))
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.hooks.len()
    }
}

impl Hook {
    fn load(path: PathBuf) -> Result<Self> {
        let contents = std::fs::read_to_string(&path)?;
        let mut hook: Hook = toml::from_str(&contents).map_err(|error| {
            DSqlLogicTestError::SelfError(format!("failed to parse {}: {error}", path.display()))
        })?;
        if hook.prepare.is_empty() {
            return Err(DSqlLogicTestError::SelfError(format!(
                "hook {} must define a prepare command",
                path.display()
            )));
        }
        hook.path = path;
        Ok(hook)
    }
}

fn is_under(file: &Path, suite: &Path) -> bool {
    file.canonicalize()
        .ok()
        .and_then(|file| file.strip_prefix(suite).ok().map(Path::to_path_buf))
        .is_some_and(|relative| !relative.as_os_str().is_empty())
}

fn run_command(hook: &Hook, command: &[String], env: &[(&str, String)], phase: &str) -> Result<()> {
    let Some((program, args)) = command.split_first() else {
        return Ok(());
    };
    println!(
        "Running hook '{}' {phase}: {}",
        hook.name,
        command.join(" ")
    );

    let status = Command::new(program)
        .args(args)
        .envs(env.iter().map(|(key, value)| (*key, value)))
        .status()
        .map_err(|error| {
            DSqlLogicTestError::SelfError(format!(
                "failed to run hook '{}' {phase} command: {error}",
                hook.name
            ))
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(DSqlLogicTestError::SelfError(format!(
            "hook '{}' {phase} command exited with {status}",
            hook.name
        )))
    }
}

fn with_cleanup_error(
    error: DSqlLogicTestError,
    cleanup_error: Option<DSqlLogicTestError>,
) -> DSqlLogicTestError {
    match cleanup_error {
        Some(cleanup_error) => DSqlLogicTestError::SelfError(format!(
            "{error}; cleanup after prepare failure also failed: {cleanup_error}"
        )),
        None => error,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    fn write_test(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "statement ok\nselect 1;\n").unwrap();
    }

    #[test]
    fn discovers_only_direct_suite_hooks() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("suites");
        let direct = suites.join("direct");
        let nested = direct.join("nested");
        write_test(&nested.join("case.test"));
        fs::write(
            direct.join(HOOK_FILE),
            "name = 'direct'\nprepare = ['true']\ncleanup = ['true']\n",
        )
        .unwrap();
        fs::write(
            nested.join(HOOK_FILE),
            "name = 'nested'\nprepare = ['false']\ncleanup = ['false']\n",
        )
        .unwrap();

        let hooks = Hooks::discover(&suites, &[nested.join("case.test")]).unwrap();

        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks.hooks[0].name, "direct");
    }

    #[test]
    fn external_files_have_no_hook() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("suites");
        fs::create_dir_all(suites.join("suite")).unwrap();
        fs::write(
            suites.join("suite/hook.toml"),
            "name = 'suite'\nprepare = ['false']\ncleanup = ['false']\n",
        )
        .unwrap();
        let external = temp.path().join("external.test");
        write_test(&external);

        assert_eq!(Hooks::discover(&suites, &[external]).unwrap().len(), 0);
    }

    #[test]
    fn prepare_failure_cleans_up_the_failed_hook_and_previous_hooks() {
        let temp = tempdir().unwrap();
        let marker = temp.path().join("marker");
        let command = |text: &str| {
            vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("printf '{text}' >> '{}'", marker.display()),
            ]
        };
        let hooks = Hooks::from_hooks(vec![
            Hook {
                name: "first".to_string(),
                prepare: command("p1"),
                cleanup: command("c1"),
                path: temp.path().join("first/hook.toml"),
            },
            Hook {
                name: "second".to_string(),
                prepare: vec!["false".to_string()],
                cleanup: command("c2"),
                path: temp.path().join("second/hook.toml"),
            },
        ]);

        let error = hooks.prepare(&[]).unwrap_err();

        assert!(error.to_string().contains("second"));
        assert_eq!(fs::read_to_string(marker).unwrap(), "p1c2c1");
    }

    #[test]
    fn hooks_with_same_commands_are_not_deduplicated() {
        let temp = tempdir().unwrap();
        let marker = temp.path().join("marker");
        let command = |text: &str| {
            vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("printf '{text}' >> '{}'", marker.display()),
            ]
        };
        let hooks = Hooks::from_hooks(vec![
            Hook {
                name: "same".to_string(),
                prepare: command("p"),
                cleanup: command("c"),
                path: temp.path().join("one/hook.toml"),
            },
            Hook {
                name: "same".to_string(),
                prepare: command("p"),
                cleanup: command("c"),
                path: temp.path().join("two/hook.toml"),
            },
        ]);

        assert_eq!(hooks.prepare(&[]).unwrap(), 2);
        hooks.cleanup(2, &[]).unwrap();
        assert_eq!(fs::read_to_string(marker).unwrap(), "ppcc");
    }

    #[test]
    fn cleanup_runs_in_reverse_order() {
        let temp = tempdir().unwrap();
        let marker = temp.path().join("marker");
        let command = |text: &str| {
            vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("printf '{text}' >> '{}'", marker.display()),
            ]
        };
        let hooks = Hooks::from_hooks(vec![
            Hook {
                name: "first".to_string(),
                prepare: command("p1"),
                cleanup: command("c1"),
                path: temp.path().join("first/hook.toml"),
            },
            Hook {
                name: "second".to_string(),
                prepare: command("p2"),
                cleanup: command("c2"),
                path: temp.path().join("second/hook.toml"),
            },
        ]);

        let env = [];
        assert_eq!(hooks.prepare(&env).unwrap(), 2);
        hooks.cleanup(2, &env).unwrap();

        assert_eq!(fs::read_to_string(marker).unwrap(), "p1p2c2c1");
    }
}
