pub mod fieldtype;

use std::sync::{Arc, Mutex};

use fieldtype::{FieldType, ConvertFieldType};

pub struct Field<'a> {
    name: &'a str,
    field_type: FieldType,
    data: Vec<u8>
}

impl<'a> Field<'a> {
    pub fn new<T: ConvertFieldType<'a, T>>(name: &'a str, data: T) -> Option<Field<'a>> {
        let field_type = data.get_type();
        let data = data.serialize();

        match data {
            Some(data) => {
                Some(Self {
                    name,
                    field_type,
                    data
                })
            }
            None => None,
        }
    }

    pub fn get_key(&self) -> &'a str {
        self.name
    }

    pub fn get_value<T: ConvertFieldType<'a, T>>(&'a self) -> Option<<T as ConvertFieldType<'a, T>>::Output> {
        T::deserialize(&self.data)
    }

    pub fn get_type(&self) -> &FieldType {
        &self.field_type
    }
}