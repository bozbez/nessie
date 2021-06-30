use std::alloc::Allocator;

pub trait CloneIn<A: Allocator> {
    fn clone_in(&self, alloc: A) -> Self where Self: Sized;
}

impl<A: Allocator + Clone, T: CloneIn<A>> CloneIn<A> for (T, T) {
    fn clone_in(&self, alloc: A) -> Self {
        (self.0.clone_in(alloc.clone()), self.1.clone_in(alloc))
    }
}

