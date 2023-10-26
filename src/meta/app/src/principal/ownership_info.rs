use std::fmt;
use std::fmt::Display;

use anyerror::AnyError;
use common_exception::ErrorCode;

use crate::principal::GrantObjectByID;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct OwnershipInfo {
    pub object: GrantObjectByID,
    pub role: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub struct OwnershipInfoSerdeError {
    pub message: String,
    pub source: AnyError,
}

impl Display for OwnershipInfoSerdeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} cause: {}", self.message, self.source)
    }
}

impl TryFrom<Vec<u8>> for OwnershipInfo {
    type Error = OwnershipInfoSerdeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match serde_json::from_slice(&value) {
            Ok(role_info) => Ok(role_info),
            Err(serialize_error) => Err(OwnershipInfoSerdeError {
                message: "Cannot deserialize GrantOwnershipInfo from bytes".to_string(),
                source: AnyError::new(&serialize_error),
            }),
        }
    }
}

impl From<OwnershipInfoSerdeError> for ErrorCode {
    fn from(e: OwnershipInfoSerdeError) -> Self {
        ErrorCode::InvalidReply(e.to_string())
    }
}
