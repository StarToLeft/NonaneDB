pub struct BucketDescription {
    
}

pub trait BucketDesriptor {
    fn get_description(&self) -> BucketDescription;
}