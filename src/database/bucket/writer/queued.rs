use std::{sync::Arc, thread::JoinHandle};

#[derive(Debug, Clone)]
pub struct WriterThread {
    pub(crate) join_handle: Arc<JoinHandle<QueuedWriter>>,
}

pub struct QueuedWriter {

}

impl QueuedWriter {
    pub fn new() -> (QueuedWriter) {
        QueuedWriter {}
    }
}