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
// limitations under the License.pub use data_type::*;

use common_datavalues::prelude::DataField as OldDataField;
use common_datavalues::prelude::DataType as OldDataType;

use crate::from_arrow_field;
use crate::prelude::*;
use crate::DataField;
use crate::MutableColumn;

impl From<OldDataField> for DataField {
    fn from(old: OldDataField) -> Self {
        let arrow_field = old.to_arrow();
        let new_type = from_arrow_field(&arrow_field);

        DataField::new(old.name(), new_type)
    }
}

impl From<DataField> for OldDataField {
    fn from(field: DataField) -> OldDataField {
        let arrow_field = field.to_arrow();

        From::from(&arrow_field)
    }
}

pub fn create_mutable_builder(datatype: OldDataType) -> Box<dyn MutableColumn> {
    let f = OldDataField::new("xx", datatype, false);
    let f: DataField = f.into();

    match f.data_type().data_type_id().to_physical_type() {
        PhysicalTypeID::Primitive(t) => with_match_physical_primitive_type!(t, |$T| {
            return Box::new(<<$T as Scalar>::ColumnType as ScalarColumn>::Builder::with_capacity(1024))
        }),
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod test {
    use common_datavalues::prelude::DataField as OldDataField;
    use common_datavalues::prelude::DataType as OldDataType;

    use crate::DataField;

    #[test]
    fn test_convert_field() {
        let old_f = OldDataField::new("name", OldDataType::Date32, false);
        let new_f = DataField::from(old_f);
        assert!(new_f.data_type().name() == "Date32");
        let old_f = OldDataField::from(new_f);
        assert!(old_f.data_type() == &OldDataType::Date32);

        let old_f = OldDataField::new("name", OldDataType::Date16, false);
        let new_f = DataField::from(old_f);
        assert!(new_f.data_type().name() == "Date");

        let old_f = OldDataField::from(new_f);
        assert!(old_f.data_type() == &OldDataType::Date16);
    }
}
