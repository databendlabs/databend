use common_catalog::table_args::TableArgs;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::StageFilesInfo;

use crate::table_functions::string_value;

#[derive(Clone)]
pub(crate) struct ListStageArgsParsed {
    pub(crate) location: String,
    pub(crate) files_info: StageFilesInfo,
}

impl ListStageArgsParsed {
    pub fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_named("infer_schema")?;

        let mut location = None;
        let mut files_info = StageFilesInfo {
            path: "".to_string(),
            files: None,
            pattern: None,
        };

        for (k, v) in &args {
            match k.to_lowercase().as_str() {
                "location" => {
                    location = Some(string_value(v)?);
                }
                "pattern" => {
                    files_info.pattern = Some(string_value(v)?);
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "unknown param {} for list_stage",
                        k
                    )));
                }
            }
        }

        let location =
            location.ok_or(ErrorCode::BadArguments("list_stage must specify location"))?;

        Ok(Self {
            location,
            files_info,
        })
    }
}
