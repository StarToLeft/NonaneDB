use super::document::field::descriptor::FieldDescriptor;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BucketDescription {
    pub(crate) field_description: Vec<FieldDescriptor>,
}

pub trait BucketDesriptor {
    fn get_description(&self) -> BucketDescription;
}