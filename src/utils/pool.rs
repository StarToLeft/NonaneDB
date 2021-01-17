use parking_lot::Mutex;
use std::mem::ManuallyDrop;

pub type Stack<T> = Vec<T>;

pub struct Pool<T> {
    stack: Mutex<Stack<T>>,
}

impl<T> Pool<T> {
    pub fn new<F: Fn() -> T>(cap: usize, init: F) -> Pool<T> {
        let mut stack = Stack::new();
        (0..cap).for_each(|_| stack.push(init()));

        Pool {
            stack: Mutex::new(stack),
        }
    }

    pub fn pull(&self) -> Ref<T> {
        self.stack
            .lock()
            .pop()
            .map(|data| Ref::new(self, data))
            .expect("Failed to pull from pool")
    }

    pub fn attach(&self, t: T) {
        self.stack.lock().push(t)
    }
}

pub struct Ref<'a, T> {
    pool: &'a Pool<T>,
    data: ManuallyDrop<T>,
}

impl<'a, T> Ref<'a, T> {
    pub fn new(pool: &'a Pool<T>, t: T) -> Self {
        Self {
            pool,
            data: ManuallyDrop::new(t),
        }
    }

    pub fn as_ref(&self) -> &T {
        &self.data
    }

    pub fn as_mut_ref(&mut self) -> &mut T {
        &mut self.data
    }

    unsafe fn take(&mut self) -> T {
        ManuallyDrop::take(&mut self.data)
    }
}

impl<'a, T> Drop for Ref<'a, T> {
    fn drop(&mut self) {
        unsafe { self.pool.attach(self.take()) }
    }
}
