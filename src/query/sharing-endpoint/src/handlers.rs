use models::Credentials;
use models::RequestFile;
use poem::error::BadRequest;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;

use crate::accessor::SharingAccessor;
use crate::models;
use crate::models::PresignFileResponse;

#[poem::handler]
pub async fn presign_files(
    credentials: &Credentials,
    Path((_tenant_id, share_name, table_name)): Path<(String, String, String)>,
    Json(request_files): Json<Vec<RequestFile>>,
) -> PoemResult<Json<Vec<PresignFileResponse>>> {
    let requester = credentials.token.clone();
    let input = models::LambdaInput::new(
        credentials.token.clone(),
        share_name,
        requester,
        table_name,
        request_files,
        None,
    );
    println!("input: {:?}", input.clone());
    return match SharingAccessor::get_presigned_files(&input).await {
        Ok(output) => Ok(Json(output)),
        Err(e) => Err(BadRequest(e)),
    };
}
