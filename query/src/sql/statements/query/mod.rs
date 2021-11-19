#[cfg(test)]
mod query_schema_joined_analyzer_test;

#[cfg(test)]
mod query_normalizer_test;

#[cfg(test)]
mod query_qualified_rewriter_test;

mod query_normalizer;
mod query_qualified_rewriter;
mod query_schema_joined;
mod query_schema_joined_analyzer;

pub use query_normalizer::QueryNormalizer;
pub use query_normalizer::QueryNormalizerData;
pub use query_qualified_rewriter::QualifiedRewriter;
pub use query_schema_joined::JoinedColumnDesc;
pub use query_schema_joined::JoinedSchema;
pub use query_schema_joined::JoinedTableDesc;
pub use query_schema_joined_analyzer::JoinedSchemaAnalyzer;
