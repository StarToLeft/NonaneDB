use std::sync::{atomic::AtomicBool, Arc};

#[derive(Clone, Debug)]
pub struct Reader<'a> {
    name: &'a str,
    will_write: Arc<AtomicBool>,
}

impl<'a> Reader<'a> {
    pub fn new(name: &'a str, will_write: Arc<AtomicBool>) -> Self {
        Self { name, will_write }
    }
}

unsafe impl<'a> Send for Reader<'a> {}
unsafe impl<'a> Sync for Reader<'a> {}
