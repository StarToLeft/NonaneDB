pub mod field;
use field::Field;

pub struct Document<'a> {
    fields: Vec<Field<'a>> 
}

impl<'a> Document<'a> {
    pub fn new(fields: Vec<Field<'a>>) -> Self {
        Self {
            fields,
        }
    }

    pub fn read_field(&self, key: &str) -> Option<&Field<'a>> {
        for f in self.fields.iter() {
            if f.get_key() == key {
                return Some(f);
            }
        }

        None
    }
}

pub trait DocumentConvert {
    type ConvertFrom;

    fn convert_to(&self) -> Option<Document>;
    fn convert_from(doc: &Document) -> Option<Self::ConvertFrom>;
}