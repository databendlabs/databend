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

use common_base::base::*;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

#[test]
fn test_format_parse() -> Result<()> {
    let cases = vec![
        // Unknown - odd variants.
        ("", None),
        ("databend", None),
        ("dir/file", None),
        ("dir/file.ext", None),
        ("dir.toml.ext", None),
        ("dir.yaml.ext", None),
        ("dir.yml.ext", None),
        ("dir.json.ext", None),
        ("dir.TOML", None),
        ("dir.YAML", None),
        ("dir.YML", None),
        ("dir.JSON", None),
        (".toml", None),
        (".yaml", None),
        (".yml", None),
        (".json", None),
        // TOML
        ("config.toml", Some(Format::Toml)),
        ("/config.toml", Some(Format::Toml)),
        ("/dir/config.toml", Some(Format::Toml)),
        ("config.var.toml", Some(Format::Toml)),
        // YAML
        ("config.yaml", Some(Format::Yaml)),
        ("/config.yaml", Some(Format::Yaml)),
        ("/dir/config.yaml", Some(Format::Yaml)),
        ("config.var.yaml", Some(Format::Yaml)),
        ("config.yml", Some(Format::Yaml)),
        ("/config.yml", Some(Format::Yaml)),
        ("/dir/config.yml", Some(Format::Yaml)),
        ("config.var.yml", Some(Format::Yaml)),
        // JSON
        ("config.json", Some(Format::Json)),
        ("/config.json", Some(Format::Json)),
        ("/dir/config.json", Some(Format::Json)),
        ("config.var.json", Some(Format::Json)),
    ];

    for (input, expected) in cases {
        let output = Format::from_path(std::path::PathBuf::from(input));
        assert_eq!(expected, output.ok(), "{}", input)
    }

    Ok(())
}

#[test]
fn test_load_config() -> Result<()> {
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct TempConfig {
        pub d: SubConfig1,
        pub e: SubConfig2,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct SubConfig1 {
        pub foo: String,
        pub bar: f64,
        pub age: i64,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct SubConfig2 {
        pub foo: String,
        pub bar: f64,
        pub age: i64,
    }

    const SAMPLE_TOML: &str = r#"
            [d]
            foo = "file"
            bar = 1.0
            age = 18
            [e]
            foo = "file"
            bar = 1.0
            age = 36
        "#;

    const SAMPLE_JSON: &str = r#"
        {
            "d": {
                "foo": "file",
                "bar": 1.0,
                "age": 18
            },
            "e": {
                "foo": "file",
                "bar": 1.0,
                "age": 36
            }
        }
        "#;

    const SAMPLE_YAML: &str = r#"
        d:
          foo: file
          bar: 1.0
          age: 18
        e:
          foo: file
          bar: 1.0
          age: 36
        "#;

    let data = vec![
        (Format::Toml, SAMPLE_TOML),
        (Format::Json, SAMPLE_JSON),
        (Format::Yaml, SAMPLE_YAML),
    ];

    let cfg = toml::from_str::<TempConfig>(SAMPLE_TOML).unwrap();
    for (f, content) in data.iter() {
        let cfg_str = f.serialize_config(&cfg).unwrap();
        let new_cfg: TempConfig = f.load_config(&cfg_str).unwrap();
        assert_eq!(cfg, new_cfg);

        let new_cfg: TempConfig = f.load_config(content).unwrap();
        assert_eq!(cfg, new_cfg);
    }

    Ok(())
}
