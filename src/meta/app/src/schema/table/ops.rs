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

//! TableMeta operation methods

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;

use super::TableMeta;
use crate::schema::constraint::Constraint;

impl TableMeta {
    pub fn add_column(
        &mut self,
        field: &TableField,
        comment: &str,
        index: FieldIndex,
    ) -> Result<()> {
        self.fill_field_comments();

        let mut new_schema = self.schema.as_ref().to_owned();
        new_schema.add_column(field, index)?;
        self.schema = Arc::new(new_schema);
        self.field_comments.insert(index, comment.to_owned());
        Ok(())
    }

    pub fn drop_column(&mut self, column: &str) -> Result<()> {
        self.fill_field_comments();

        let mut new_schema = self.schema.as_ref().to_owned();
        let index = new_schema.drop_column(column)?;
        self.field_comments.remove(index);
        self.schema = Arc::new(new_schema);
        Ok(())
    }

    /// To fix the field comments panic.
    pub fn fill_field_comments(&mut self) {
        let num_fields = self.schema.num_fields();
        // If the field comments is confused, fill it with empty string.
        if self.field_comments.len() < num_fields {
            self.field_comments = vec!["".to_string(); num_fields];
        }
    }

    pub fn add_constraint(
        &mut self,
        constraint_name: String,
        constraint: Constraint,
    ) -> Result<()> {
        if self.constraints.contains_key(&constraint_name) {
            return Err(ErrorCode::AlterTableError(format!(
                "constraint {} already exists",
                constraint_name
            )));
        }
        self.constraints.insert(constraint_name, constraint);
        Ok(())
    }

    pub fn drop_constraint(&mut self, constraint_name: &str) -> Result<()> {
        if self.constraints.remove(constraint_name).is_none() {
            return Err(ErrorCode::AlterTableError(format!(
                "constraint {} not exists",
                constraint_name
            )));
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;

    use super::*;

    fn create_test_table_meta() -> TableMeta {
        let schema = TableSchema::new(vec![
            TableField::new("id", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("name", TableDataType::String),
        ]);

        TableMeta {
            schema: Arc::new(schema),
            field_comments: vec!["id comment".to_string(), "name comment".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn test_add_column() {
        let mut meta = create_test_table_meta();
        assert_eq!(meta.schema.num_fields(), 2);

        let new_field = TableField::new("age", TableDataType::Number(NumberDataType::Int32));
        meta.add_column(&new_field, "age comment", 2).unwrap();

        assert_eq!(meta.schema.num_fields(), 3);
        assert_eq!(meta.schema.field(2).name(), "age");
        assert_eq!(meta.field_comments[2], "age comment");
    }

    #[test]
    fn test_drop_column() {
        let mut meta = create_test_table_meta();
        assert_eq!(meta.schema.num_fields(), 2);

        meta.drop_column("name").unwrap();

        assert_eq!(meta.schema.num_fields(), 1);
        assert_eq!(meta.schema.field(0).name(), "id");
        assert_eq!(meta.field_comments.len(), 1);
    }

    #[test]
    fn test_drop_nonexistent_column() {
        let mut meta = create_test_table_meta();
        let result = meta.drop_column("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_fill_field_comments() {
        let schema = TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::String),
            TableField::new("c", TableDataType::Boolean),
        ]);

        let mut meta = TableMeta {
            schema: Arc::new(schema),
            field_comments: vec!["comment a".to_string()],
            ..Default::default()
        };

        assert_eq!(meta.field_comments.len(), 1);

        meta.fill_field_comments();

        assert_eq!(meta.field_comments.len(), 3);
        assert_eq!(meta.field_comments[0], "");
        assert_eq!(meta.field_comments[1], "");
        assert_eq!(meta.field_comments[2], "");
    }

    #[test]
    fn test_add_constraint() {
        let mut meta = create_test_table_meta();
        let constraint = Constraint::Check("id > 0".to_string());

        meta.add_constraint("check_id".to_string(), constraint.clone())
            .unwrap();

        assert_eq!(meta.constraints.len(), 1);
        assert!(meta.constraints.contains_key("check_id"));
    }

    #[test]
    fn test_add_duplicate_constraint() {
        let mut meta = create_test_table_meta();
        let constraint = Constraint::Check("id > 0".to_string());

        meta.add_constraint("check_id".to_string(), constraint.clone())
            .unwrap();
        let result = meta.add_constraint("check_id".to_string(), constraint);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::ALTER_TABLE_ERROR);
    }

    #[test]
    fn test_drop_constraint() {
        let mut meta = create_test_table_meta();
        let constraint = Constraint::Check("id > 0".to_string());

        meta.add_constraint("check_id".to_string(), constraint)
            .unwrap();
        assert_eq!(meta.constraints.len(), 1);

        meta.drop_constraint("check_id").unwrap();
        assert_eq!(meta.constraints.len(), 0);
    }

    #[test]
    fn test_drop_nonexistent_constraint() {
        let mut meta = create_test_table_meta();
        let result = meta.drop_constraint("nonexistent");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::ALTER_TABLE_ERROR);
    }
}
