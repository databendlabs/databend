use crate::models;
use models::Credentials;
use models::RequestFile;
use models::LambdaInput;

use poem::error::{BadRequest, Result as PoemResult};
use poem::web::Path;
use poem::web::Json;
use crate::accessor::SharingAccessor;
use crate::models::{PresignFileResponse, SharedTableResponse};


#[poem::handler]
pub async fn presign_files(
    credentials: &Credentials,
    Path((tenant_id, share_name, table_name)): Path<(String, String, String)>,
    Json(request_files): Json<Vec<RequestFile>>,
) -> PoemResult<Json<Vec<PresignFileResponse>>> {
    let requester = credentials.token.clone();
    let input = models::LambdaInput::new(credentials.token.clone(), share_name, requester, table_name, request_files, None);
    println!("input: {:?}", input.clone());
    return match SharingAccessor::get_presigned_files(&input).await {
        Ok(output) => Ok(Json(output)),
        Err(e) => Err(BadRequest(e)),
    }
}
