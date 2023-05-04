#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub column: String,
    pub nlists: Option<u64>, // used for IVF index
}
