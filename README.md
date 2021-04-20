# sharify

[![Documentation](https://docs.rs/sharify/badge.svg)](https://docs.rs/sharify)
[![Crates.io](https://img.shields.io/crates/v/sharify)](https://crates.io/crates/sharify)

Back Rust types with shared memory and send them cheaply between processes. 

For example

```rust
use sharify::SharedMut;
use std::{iter, sync::mpsc::channel, thread};

// Create a slice backed by shared memory.
let mut shared_slice: SharedMut<[u64]> = SharedMut::new(&(0, 1_000_000))?;

// Write some data to it.
for (src, dst) in
    iter::successors(Some(0), |&p| Some(p + 1))
    .zip(shared_slice.as_view_mut().iter_mut())
{
    *dst = src;
}

// The shared slice can be sent between processes cheaply without copying the data. What is shown here for threads works equally well for processes, e.g. using the ipc_channel crate.
let (tx, rx) = channel::<SharedMut<[u64]>>();
let handle = thread::spawn(move || {
    let shared_slice = rx.recv().unwrap();
    // Get a view into the shared memory
    let view: &[u64] = shared_slice.as_view();
    assert_eq!(view.len(), 1_000_000);
    assert!(iter::successors(Some(0), |&p| Some(p + 1))
        .zip(view.iter())
        .all(|(a, &b)| a == b));
});
tx.send(shared_slice)?;
handle.join().unwrap();
```

See the [docs](https://docs.rs/sharify) for details on how to do this for your own types. Requires at least Rust `1.51.0` due to the use of [const generics](https://github.com/rust-lang/rust/pull/79135).