// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::utils::try_insert_field;
use super::*;

/// Creates a field id to field map.
pub fn index_by_id(r#struct: &StructType) -> Result<HashMap<i32, NestedFieldRef>> {
    struct IndexById(HashMap<i32, NestedFieldRef>);

    impl SchemaVisitor for IndexById {
        type T = ();

        fn schema(&mut self, _schema: &Schema, _value: ()) -> Result<()> {
            Ok(())
        }

        fn field(&mut self, field: &NestedFieldRef, _value: ()) -> Result<()> {
            try_insert_field(&mut self.0, field.id, field.clone())
        }

        fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
            Ok(())
        }

        fn list(&mut self, list: &ListType, _value: Self::T) -> Result<Self::T> {
            try_insert_field(
                &mut self.0,
                list.element_field.id,
                list.element_field.clone(),
            )
        }

        fn map(&mut self, map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
            try_insert_field(&mut self.0, map.key_field.id, map.key_field.clone())?;
            try_insert_field(&mut self.0, map.value_field.id, map.value_field.clone())
        }

        fn primitive(&mut self, _: &PrimitiveType) -> Result<Self::T> {
            Ok(())
        }
    }

    let mut index = IndexById(HashMap::new());
    visit_struct(r#struct, &mut index)?;
    Ok(index.0)
}

/// Creates a field id to parent field id map.
pub fn index_parents(r#struct: &StructType) -> Result<HashMap<i32, i32>> {
    struct IndexByParent {
        parents: Vec<i32>,
        result: HashMap<i32, i32>,
    }

    impl SchemaVisitor for IndexByParent {
        type T = ();

        fn before_struct_field(&mut self, field: &NestedFieldRef) -> Result<()> {
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
            self.parents.push(field.id);
            Ok(())
        }

        fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
            self.parents.push(field.id);
            Ok(())
        }

        fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
            self.parents.push(field.id);
            Ok(())
        }

        fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
            if let Some(parent) = self.parents.last().copied() {
                self.result.insert(field.id, parent);
            }
            self.parents.push(field.id);
            Ok(())
        }

        fn after_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
            self.parents.pop();
            Ok(())
        }

        fn schema(&mut self, _schema: &Schema, _value: Self::T) -> Result<Self::T> {
            Ok(())
        }

        fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
            Ok(())
        }

        fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
            Ok(())
        }

        fn list(&mut self, _list: &ListType, _value: Self::T) -> Result<Self::T> {
            Ok(())
        }

        fn map(&mut self, _map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
            Ok(())
        }

        fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
            Ok(())
        }
    }

    let mut index = IndexByParent {
        parents: vec![],
        result: HashMap::new(),
    };
    visit_struct(r#struct, &mut index)?;
    Ok(index.result)
}

#[derive(Default)]
pub struct IndexByName {
    // Maybe radix tree is better here?
    name_to_id: HashMap<String, i32>,
    short_name_to_id: HashMap<String, i32>,

    field_names: Vec<String>,
    short_field_names: Vec<String>,
}

impl IndexByName {
    fn add_field(&mut self, name: &str, field_id: i32) -> Result<()> {
        let full_name = self
            .field_names
            .iter()
            .map(String::as_str)
            .chain(vec![name])
            .join(".");
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}"
                ),
            ));
        } else {
            self.name_to_id.insert(full_name, field_id);
        }

        let full_short_name = self
            .short_field_names
            .iter()
            .map(String::as_str)
            .chain(vec![name])
            .join(".");
        self.short_name_to_id
            .entry(full_short_name)
            .or_insert_with(|| field_id);
        Ok(())
    }

    /// Returns two indexes: full name to field id, and id to full name.
    ///
    /// In the first index, short names are returned.
    /// In second index, short names are not returned.
    pub fn indexes(mut self) -> (HashMap<String, i32>, HashMap<i32, String>) {
        self.short_name_to_id.reserve(self.name_to_id.len());
        for (name, id) in &self.name_to_id {
            self.short_name_to_id.insert(name.clone(), *id);
        }

        let id_to_name = self.name_to_id.into_iter().map(|e| (e.1, e.0)).collect();
        (self.short_name_to_id, id_to_name)
    }
}

impl SchemaVisitor for IndexByName {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.to_string());
        self.short_field_names.push(field.name.to_string());
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        self.short_field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.clone());
        if !field.field_type.is_struct() {
            self.short_field_names.push(field.name.to_string());
        }

        Ok(())
    }

    fn after_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        if !field.field_type.is_struct() {
            self.short_field_names.pop();
        }

        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.before_struct_field(field)
    }

    fn after_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.after_struct_field(field)
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.to_string());
        if !field.field_type.is_struct() {
            self.short_field_names.push(field.name.to_string());
        }
        Ok(())
    }

    fn after_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        if !field.field_type.is_struct() {
            self.short_field_names.pop();
        }

        Ok(())
    }

    fn schema(&mut self, _schema: &Schema, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn field(&mut self, field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
        self.add_field(field.name.as_str(), field.id)
    }

    fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
        Ok(())
    }

    fn list(&mut self, list: &ListType, _value: Self::T) -> Result<Self::T> {
        self.add_field(LIST_FIELD_NAME, list.element_field.id)
    }

    fn map(&mut self, map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
        self.add_field(MAP_KEY_FIELD_NAME, map.key_field.id)?;
        self.add_field(MAP_VALUE_FIELD_NAME, map.value_field.id)
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::schema::tests::table_schema_nested;

    #[test]
    fn test_index_parent() {
        let schema = table_schema_nested();
        let result = index_parents(&schema.r#struct).unwrap();
        assert_eq!(result.get(&5).unwrap(), &4);
        assert_eq!(result.get(&7).unwrap(), &6);
        assert_eq!(result.get(&8).unwrap(), &6);
        assert_eq!(result.get(&9).unwrap(), &8);
        assert_eq!(result.get(&10).unwrap(), &8);
        assert_eq!(result.get(&12).unwrap(), &11);
        assert_eq!(result.get(&13).unwrap(), &12);
        assert_eq!(result.get(&14).unwrap(), &12);
        assert_eq!(result.get(&16).unwrap(), &15);
        assert_eq!(result.get(&17).unwrap(), &15);
    }
}
