use std::str::FromStr;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_meta_types::FileFormatOptions;
use common_meta_types::StageFileCompression;
use common_meta_types::StageFileFormatType;
use common_settings::Settings;

trait FileFormatTypeExt {
    fn get_file_format_options_from_setting(
        &self,
        settings: &Settings,
    ) -> Result<FileFormatOptions>;
}

impl FileFormatTypeExt for StageFileFormatType {
    fn get_file_format_options_from_setting(
        &self,
        settings: &Settings,
    ) -> Result<FileFormatOptions> {
        Ok(FileFormatOptions {
            format: self.clone(),
            skip_header: settings.get_format_skip_header()?,
            field_delimiter: settings.get_format_field_delimiter()?,
            record_delimiter: settings.get_format_record_delimiter()?,
            escape: settings.get_format_escape()?,
            compression: StageFileCompression::from_str(&settings.get_format_compression()?)
                .map_err_to_code(
                    ErrorCode::InvalidArgument,
                    || "get_file_format_options_from_setting",
                )?,
        })
    }
}
