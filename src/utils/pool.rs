use parking_lot::Mutex;
use std::mem::{forget, ManuallyDrop};
use std::ops::{Deref, DerefMut};

pub type Stack<T> = Vec<T>;

pub struct Pool<T> {
    objects: Mutex<Stack<T>>,
}

impl<T> Pool<T> {
    #[inline]
    pub fn new<F>(cap: usize, init: F) -> Pool<T>
    where
        F: Fn() -> T,
    {
        let mut objects = Stack::new();

        for _ in 0..cap {
            objects.push(init());
        }

        Pool {
            objects: Mutex::new(objects),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.objects.lock().is_empty()
    }

    pub fn pull(&self) -> Reusable<T> {
        self.objects
            .lock()
            .pop()
            .map(|data| Reusable::new(self, data))
            .expect("Failed to pull from pool")
    }

    pub fn attach(&self, t: T) {
        self.objects.lock().push(t)
    }
}

pub struct Reusable<'a, T> {
    pool: &'a Pool<T>,
    data: ManuallyDrop<T>,
}

impl<'a, T> Reusable<'a, T> {
    pub fn new(pool: &'a Pool<T>, t: T) -> Self {
        Self {
            pool,
            data: ManuallyDrop::new(t),
        }
    }

    pub fn detach(mut self) -> (&'a Pool<T>, T) {
        let ret = unsafe { (self.pool, self.take()) };
        forget(self);
        ret
    }

    pub fn as_ref(&self) -> &T {
        &self.data
    }

    unsafe fn take(&mut self) -> T {
        ManuallyDrop::take(&mut self.data)
    }
}

impl<'a, T> Deref for Reusable<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T> DerefMut for Reusable<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<'a, T> Drop for Reusable<'a, T> {
    fn drop(&mut self) {
        unsafe { self.pool.attach(self.take()) }
    }
}
