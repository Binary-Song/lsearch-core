use std::alloc::{GlobalAlloc, Layout, System};

struct MyAlloc;

unsafe impl GlobalAlloc for MyAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        System.alloc(layout) // delegate to system malloc
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

// this must appear in exactly ONE crate
#[global_allocator]
static A: MyAlloc = MyAlloc;