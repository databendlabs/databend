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

use std::collections::HashMap;
use std::path::Path;

use once_cell::sync::OnceCell;
use pulldown_cmark::Options;
use pulldown_cmark::Parser;
use pulldown_cmark::Tag;
use regex::Regex;
use rust_embed::RustEmbed;

use super::FunctionDocs;

#[derive(RustEmbed)]
#[folder = "../../docs/doc/03-reference/02-functions"]
pub struct FunctionDocAsset;

static DOCS: OnceCell<HashMap<String, FunctionDocs>> = OnceCell::new();
static TITLE_REGEXP: OnceCell<Regex> = OnceCell::new();
static TITLE_ALIAS_REGEXP: OnceCell<Regex> = OnceCell::new();
static CATE_REGEXP: OnceCell<Regex> = OnceCell::new();

impl FunctionDocAsset {
    pub fn get_doc(name: &str) -> FunctionDocs {
        let docs = DOCS.get_or_init(Self::build_docs);
        docs.get(name).cloned().unwrap_or_default()
    }

    pub fn build_docs() -> HashMap<String, FunctionDocs> {
        let mut map = HashMap::new();

        for it in FunctionDocAsset::iter() {
            let path = it.as_ref();
            let path = Path::new(path);
            let extension = path.extension().and_then(|f| f.to_str());

            if let Some("md") = extension {
                if let Some(file) = Self::get(it.as_ref()) {
                    let data = file.data.as_ref();
                    let data = String::from_utf8_lossy(data);
                    let (keys, mut doc) = Self::parse_doc(data.as_ref());

                    let cate_regexp =
                        CATE_REGEXP.get_or_init(|| Regex::new(r"\d+\-(\S+)-functions").unwrap());
                    let cap = cate_regexp.captures(it.as_ref()).unwrap();
                    let cate = cap.get(1).unwrap();
                    doc.category = cate.as_str().to_string();

                    for key in keys {
                        map.insert(key, doc.clone());
                    }
                }
            }
        }

        map
    }

    pub fn parse_doc(data: &str) -> (Vec<String>, FunctionDocs) {
        let mut doc = FunctionDocs {
            markdown: data.to_string(),
            ..Default::default()
        };

        let mut options = Options::empty();
        options.insert(Options::ENABLE_STRIKETHROUGH);
        options.insert(Options::ENABLE_HEADING_ATTRIBUTES);
        let parser = Parser::new_ext(data, options);

        let title_regex =
            TITLE_REGEXP.get_or_init(|| Regex::new(r#"title:\s*(?P<title>\S+)"#).unwrap());
        let title_alias_regexp = TITLE_ALIAS_REGEXP
            .get_or_init(|| Regex::new(r#"title_includes\s*:\s*(?P<title>\S+)"#).unwrap());

        let mut stage = MarkdownStage::Title;
        let mut inside_head = false;

        let mut keys = vec![];
        for event in parser {
            match event {
                pulldown_cmark::Event::Start(Tag::Heading(_, _, _)) => {
                    inside_head = true;
                }

                pulldown_cmark::Event::End(Tag::Heading(_, _, _)) => {
                    inside_head = false;
                    if stage == MarkdownStage::Title {
                        stage = MarkdownStage::Description;
                    }
                }

                pulldown_cmark::Event::Text(txt) => {
                    let txt = txt.as_ref();
                    let txt_string = txt.to_lowercase();
                    if inside_head {
                        // Stage
                        if txt.starts_with("title:") {
                            stage = MarkdownStage::Title;

                            if let Some(caps) = title_regex.captures(txt) {
                                if let Some(title) = caps.get(1) {
                                    let title = title.as_str();
                                    keys.push(title.to_lowercase());
                                }
                            } else if let Some(caps) = title_alias_regexp.captures(txt) {
                                if let Some(aliases) = caps.get(1) {
                                    let aliases = aliases.as_str();
                                    let aliases =
                                        aliases.split(',').map(|s| s.trim()).collect::<Vec<_>>();

                                    for title in aliases {
                                        keys.push(title.to_lowercase());
                                    }
                                }
                            }
                        } else if txt.starts_with("Syntax") {
                            stage = MarkdownStage::Syntax;
                        } else if txt.starts_with("Return Type") {
                            stage = MarkdownStage::ReturnType;
                        } else if txt.starts_with("Example") {
                            stage = MarkdownStage::Example;
                        } else if keys.contains(&txt_string) {
                            stage = MarkdownStage::Description;
                        } else {
                            stage = MarkdownStage::Unknown;
                        }
                        continue;
                    }
                    match stage {
                        MarkdownStage::Description => {
                            doc.description = txt.to_string();
                        }
                        MarkdownStage::Syntax => {
                            doc.syntax = txt.to_string();
                        }
                        MarkdownStage::ReturnType => {
                            doc.return_type = txt.to_string();
                        }
                        MarkdownStage::Example => {
                            doc.example = txt.to_string();
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        (keys, doc)
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum MarkdownStage {
    Title,
    Description,
    Syntax,
    ReturnType,
    Example,
    Unknown,
}
