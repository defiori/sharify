#![warn(clippy::all)]

//! This crate allows backing types with shared memory to send them cheaply
//! between processes. Here's an example of doing so with a slice:
//!
//! ```
//! use sharify::SharedMut;
//! use std::{iter, sync::mpsc::channel, thread};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a slice backed by shared memory.
//! let mut shared_slice: SharedMut<[u64]> = SharedMut::new(&(0, 1_000_000))?;
//! // Write some data to it.
//! for (src, dst) in
//!     iter::successors(Some(0), |&p| Some(p + 1))
//!     .zip(shared_slice.as_view_mut().iter_mut())
//! {
//!     *dst = src;
//! }
//! // The shared slice can be sent between processes cheaply without copying the
//! // data. What is shown here for threads works equally well for processes,
//! // e.g. using the ipc_channel crate.
//! let (tx, rx) = channel::<SharedMut<[u64]>>();
//! let handle = thread::spawn(move || {
//!     let shared_slice = rx.recv().unwrap();
//!     // Get a view into the shared memory
//!     let view: &[u64] = shared_slice.as_view();
//!     assert_eq!(view.len(), 1_000_000);
//!     assert!(iter::successors(Some(0), |&p| Some(p + 1))
//!         .zip(view.iter())
//!         .all(|(a, &b)| a == b));
//! });
//! tx.send(shared_slice)?;
//! handle.join().unwrap();
//! # Ok(())
//! # }
//! ```
//!
//! The [`Shared`] and [`SharedMut`] structs wrap types to be backed by shared
//! memory. They handle cheap serialization / deserialization by only
//! serializing the metadata required to recreate the struct on the
//! deserialization side. As a result, [`Shared`] and [`SharedMut`] can be used
//! with inter-process channels (e.g. the [ipc-channel](https://crates.io/crates/ipc-channel) crate) the
//! same way that the wrapped types are used with Rust's
//! [builtin](std::sync::mpsc::channel) or [crossbeam](https://crates.io/crates/crossbeam-channel) inter-thread channels without copying the
//! underlying data.
//!
//! Memory is managed through reference counts in the underlying shared memory.
//! The wrappers behave as follows:
//!
//! <table>
//! <tr>
//! <th>
//! </th>
//! <th>
//! Mutability
//! </th>
//! <th>
//!
//! Trait bounds on `T`
//!
//! </th>
//! <th>
//! Ownership
//! </th>
//! <th>
//! Shared memory freed when...
//! </th>
//! </tr>
//!
//!
//! <tr>
//! <td>
//!
//! [`Shared<T>`]
//!
//! </td>
//! <td>
//! Immutable
//! </td>
//! <td>
//!
//! [`ShmemBacked`] + [`ShmemView`]
//!
//! </td>
//! <td>
//!
//! Multiple ownership tracked with refcount, implements [`Clone`] to create
//! another instance backed by the same shared memory.
//!
//! </td>
//! <td>
//!
//! ...an instance with exclusive ownership of the shared memory drops **and**
//! the [*serialization count*](#-safety-and-the-serialization-count) is 0.
//!
//! </td>
//! </tr>
//!
//!
//! <tr>
//! <td>
//!
//! [`SharedMut<T>`]
//!
//! </td>
//! <td>
//! Mutable
//! </td>
//! <td>
//!
//! [`ShmemBacked`] + [`ShmemView`] + [`ShmemViewMut`]
//!
//! </td>
//! <td>
//!
//! Exclusive ownership, but implements [`TryInto<Shared>`].
//!
//! </td>
//! <td>
//!
//! ...an instance drops without serialization.
//!
//! </td>
//! </tr>
//!
//!
//! </table>
//!
//!
//! # ⚠️ Safety and the serialization count
//!
//! When serializing a [`Shared`]/[`SharedMut`] to send it between processes
//! the underlying shared memory must not be freed. However, calling
//! [`Shared::into_serialized`]/[`SharedMut::into_serialized`] consumes
//! the `Self` instance acting as the memory RAII guard. As
//! a result, not deserializing at the other end can **leak the shared memory**.
//! While this is not inherently [`unsafe`](https://doc.rust-lang.org/stable/nomicon/what-unsafe-does.html),
//! it must be kept in mind when serializing.
//!
//! [`Shared`]s keep count of how many instances accessing the same shared
//! memory have been serialized without a matching deserialization. Only when
//! this *serialization count* is 0, i.e. there are
//! no 'dangling' serializations, will the shared memory be freed when an
//! instance with exclusive ownership drops. This is necessary so that the
//! shared memory persists when a [`Shared`] with exclusive access is
//! serialized/deserialized while sent between processes.
//!
//! The downside is that this approach only allows usage patterns where each
//! serialization is paired with exactly one deserialization. If multiple
//! receivers deserialize a [`Shared`] from a single serialization and drop, the
//! shared memory may be freed before other receivers attempt to deserialize a
//! different serialization. See `tests/serialization_count.rs` for an example
//! of this situation. The opposite scenario is also bad - serializing the same
//! [`Shared`] instance multiple times through [`serde::Serialize::serialize`]
//! without matching deserializations will likely leak memory.
//!
//!
//! # [`SharedMut`] and [`serde::Serialize`]
//!
//! A [`SharedMut`] represents unique ownership of the underlying shared memory.
//! Because each serialization expects a matching deserialization, serializing
//! should consume `Self` so that only one memory access exists in either
//! instance or serialized form. [`serde::Serialize::serialize`], however, takes
//! a `&Self` argument, which leaves the [`SharedMut`] intact. As a workaround
//! to provide integration with [`serde`], calls to
//! [`serde::Serialize::serialize`] invalidate `Self` through interior
//! mutability. Any future use of `Self` produces a panic. This enforces
//! the intended usage of dropping a [`SharedMut`] immediately after the
//! serialization call.
//!
//!
//! # Backing custom types with shared memory
//!
//! To be wrappable in [`Shared`], a type must implement
//! the [`ShmemBacked`] and [`ShmemView`] traits. [`SharedMut`]s
//! have an additional [`ShmemViewMut`] trait bound. See the example below for
//! how to back a custom type with shared memory.
//!
//! ```
//! use sharify::{Shared, ShmemBacked, ShmemView};
//! use std::{sync::mpsc::channel, thread};
//!
//! // Holds a stack of images in contiguous memory.
//! struct ImageStack {
//!     data: Vec<u8>,
//!     shape: [u16; 2],
//! }
//!
//! // To back `ImageStack` with shared memory, it needs to implement `ShmemBacked`.
//! unsafe impl ShmemBacked for ImageStack {
//!     // Constructor arguments, (shape, n_images, init value).
//!     type NewArg = ([u16; 2], usize, u8);
//!     // Information required to create a view of an `ImageStack` from raw memory.
//!     type MetaData = [u16; 2];
//!
//!     fn required_memory_arg((shape, n_images, _init): &Self::NewArg) -> usize {
//!         shape.iter().product::<u16>() as usize * n_images
//!     }
//!     fn required_memory_src(src: &Self) -> usize {
//!         src.data.len()
//!     }
//!     fn new(data: &mut [u8], (shape, _n_images, init): &Self::NewArg) -> Self::MetaData {
//!         data.fill(*init);
//!         *shape
//!     }
//!     fn new_from_src(data: &mut [u8], src: &Self) -> Self::MetaData {
//!         data.copy_from_slice(&src.data);
//!         src.shape
//!     }
//! }
//!
//! // Create a referential struct as a view into the memory.
//! struct ImageStackView<'a> {
//!     data: &'a [u8],
//!     shape: [u16; 2],
//! }
//!
//! // The view must implement `ShmemView`.
//! impl<'a> ShmemView<'a> for ImageStack {
//!     type View = ImageStackView<'a>;
//!     fn view(data: &'a [u8], shape: &'a <Self as ShmemBacked>::MetaData) -> Self::View {
//!         ImageStackView {
//!             data,
//!             shape: *shape,
//!         }
//!     }
//! }
//!
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Existing stack with its data in a `Vec`.
//! let stack = ImageStack {
//!     data: vec![0; 640 * 640 * 100],
//!     shape: [640, 640],
//! };
//! // Copy the stack into shared memory.
//! let shared_stack: Shared<ImageStack> = Shared::new_from_inner(&stack)?;
//! // The `data` field is now backed by shared memory so the stack can be sent
//! // between processes cheaply. What is shown here for threads works equally
//! // well for processes, e.g. using the ipc_channel crate.
//! let (tx, rx) = channel::<Shared<ImageStack>>();
//! let handle = thread::spawn(move || {
//!     let shared_stack = rx.recv().unwrap();
//!     // Get a view into the shared memory.
//!     let view: ImageStackView = shared_stack.as_view();
//!     assert!(view.data.iter().all(|&x| x == 0));
//!     assert_eq!(view.shape, [640, 640]);
//! });
//! tx.send(shared_stack)?;
//! handle.join().unwrap();
//! # Ok(())
//! # }
//! ```
//!
//! # [`ndarray`] integration
//! By default the `shared_ndarray` feature is enabled, which implements [`ShmemBacked`]
//! for [`ndarray::Array`] and is useful for cheaply sending large arrays between
//! processes.
//!

use raw_sync::locks::{LockInit, Mutex};
use serde::{de, de::DeserializeOwned, ser, Deserialize, Deserializer, Serialize, Serializer};
use std::cell::UnsafeCell;
use std::convert::{From, Into, TryFrom, TryInto};
use std::ops::{Deref, DerefMut};

#[allow(dead_code)]
mod shared_memory;
use shared_memory::{Shmem, ShmemConf, ShmemError};

#[allow(dead_code)]
mod memory;
use memory::{is_aligned, ALIGNMENT};

#[derive(Debug)]
pub enum Error {
    UnalignedMemory,
    Mutex(String),
    Shmem(ShmemError),
    Serialization(String),
    Deserialization(String),
    InvalidSharedMut,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnalignedMemory => write!(f, "Encountered unaligned memory."),
            Self::Shmem(e) => e.fmt(f),
            Self::Mutex(s) | Self::Serialization(s) | Self::Deserialization(s) => {
                write!(f, "{}", s)
            }
            Self::InvalidSharedMut => write!(f, "Trying to use a `SharedMut` previously invalidated with a call to `serde::Serialize::serialize`.")
        }
    }
}

impl std::error::Error for Error {}

/// Implemented for types which can be wrapped in a [`Shared`] or [`SharedMut`]
/// for cheap sharing across processes.
///
/// This trait is closely
/// connected to the [`ShmemView`] and [`ShmemViewMut`] traits. See the
/// crate-level documentation for how to implement this trait on custom types.
///
/// # Safety
///
/// This trait is unsafe because care must be taken to return the correct memory
/// size and metadata. The [`ShmemView`]/[`ShmemViewMut`] traits rely on valid
/// metadata to create view types from raw memory, usually using `unsafe`
/// operations. Incorrect metadata can produce invalid view types.
//
// IDEA: Generic associated types should allow constraining lifetime parameters
// on generic return types, obviating the need for separate
// [`ShmemView`]/[`ShmemViewMut`] traits. https://github.com/rust-lang/rfcs/blob/master/text/1598-generic_associated_types.md
pub unsafe trait ShmemBacked {
    /// The type of user-defined arguments passed to
    /// [`Self::required_memory_arg`] and [`Self::new`]. The
    /// [`new`](SharedMut::new) function on the [`Shared`]/[`SharedMut`]
    /// wrappers accepts an argument of this type to construct a shared
    /// memory-backed version of `Self`.
    type NewArg: ?Sized;
    /// The information required to construct
    /// [`ShmemView::View`]/[`ShmemViewMut::View`] types from raw memory.
    type MetaData: Serialize + DeserializeOwned + Clone;

    /// Returns the memory size in bytes required by a shared memory-backed
    /// version of `Self` created with the user-defined constructor argument
    /// `arg`.
    fn required_memory_arg(arg: &Self::NewArg) -> usize;
    /// Returns the required shared memory size in bytes
    /// to hold a copy of `src`.
    fn required_memory_src(src: &Self) -> usize;
    /// Fills the shared memory at `data` with bytes according to the
    /// user-defined constructor argument `arg`  and returns the metadata
    /// required to create a [`ShmemView::View`]/[`ShmemViewMut::View`] from
    /// `data`.
    fn new(data: &mut [u8], arg: &Self::NewArg) -> Self::MetaData;
    /// Fills the shared memory at `data` with a copy of `src` and returns the
    /// metadata required to create a [`ShmemView::View`]/[`ShmemViewMut::View`]
    /// from `data`.
    fn new_from_src(data: &mut [u8], src: &Self) -> Self::MetaData;
}

/// An immutable view into shared memory.
pub trait ShmemView<'a>: ShmemBacked {
    type View;
    /// Creates a [`Self::View`] into the shared memory at `data` based on the information
    /// in `metadata`.
    fn view(data: &'a [u8], metadata: &'a <Self as ShmemBacked>::MetaData) -> Self::View;
}

/// An mutable view into shared memory.
pub trait ShmemViewMut<'a>: ShmemBacked {
    type View;
    /// Creates a [`Self::View`] into the shared memory at `data` based on the information
    /// in `metadata`.
    fn view_mut(
        data: &'a mut [u8],
        metadata: &'a mut <Self as ShmemBacked>::MetaData,
    ) -> Self::View;
}

unsafe impl ShmemBacked for str {
    type NewArg = str;
    // str size in bytes, only used for asserts
    type MetaData = usize;

    fn required_memory_arg(src: &Self::NewArg) -> usize {
        src.len()
    }

    fn required_memory_src(src: &Self) -> usize {
        src.len()
    }

    /// Creates a string slice at `data` with the contents of `src`.
    fn new(data: &mut [u8], src: &Self::NewArg) -> Self::MetaData {
        assert_eq!(data.len(), Self::required_memory_arg(src));
        data.copy_from_slice(src.as_bytes());
        data.len()
    }

    /// Creates a string slice at `data` with the contents of `src`.
    fn new_from_src(data: &mut [u8], src: &Self) -> Self::MetaData {
        assert_eq!(data.len(), Self::required_memory_src(src));
        data.copy_from_slice(src.as_bytes());
        data.len()
    }
}

impl<'a> ShmemView<'a> for str {
    type View = &'a str;

    fn view(data: &'a [u8], metadata: &'a <Self as ShmemBacked>::MetaData) -> Self::View {
        assert_eq!(data.len(), *metadata);
        unsafe { std::str::from_utf8_unchecked(data) }
    }
}

impl<'a> ShmemViewMut<'a> for str {
    type View = &'a mut str;

    fn view_mut(
        data: &'a mut [u8],
        metadata: &'a mut <Self as ShmemBacked>::MetaData,
    ) -> Self::View {
        assert_eq!(data.len(), *metadata);
        unsafe { std::str::from_utf8_unchecked_mut(data) }
    }
}

unsafe impl<T> ShmemBacked for [T]
where
    T: Copy,
{
    type NewArg = (T, usize);
    // slice size in elements of T
    type MetaData = usize;

    fn required_memory_arg((_, len): &Self::NewArg) -> usize {
        *len * std::mem::size_of::<T>()
    }

    fn required_memory_src(src: &Self) -> usize {
        src.len() * std::mem::size_of::<T>()
    }

    /// Creates a slice at `data` of length `len` filled with `init`.
    fn new(data: &mut [u8], &(init, len): &Self::NewArg) -> Self::MetaData {
        assert_eq!(data.len(), Self::required_memory_arg(&(init, len)));
        let data_typed =
            unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut T, len) };
        for elem in data_typed.iter_mut() {
            *elem = init;
        }
        len
    }

    /// Creates a slice at `data` with the contents of `src`.
    fn new_from_src(data: &mut [u8], src: &Self) -> Self::MetaData {
        assert_eq!(data.len(), Self::required_memory_src(src));
        let data_typed =
            unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut T, src.len()) };
        data_typed.copy_from_slice(src);
        data_typed.len()
    }
}

impl<'a, T> ShmemView<'a> for [T]
where
    T: Copy + 'a,
{
    type View = &'a [T];

    fn view(data: &'a [u8], metadata: &'a <Self as ShmemBacked>::MetaData) -> Self::View {
        unsafe { std::slice::from_raw_parts(data.as_ptr() as *const T, *metadata) }
    }
}

impl<'a, T> ShmemViewMut<'a> for [T]
where
    T: Copy + 'a,
{
    type View = &'a mut [T];

    fn view_mut(
        data: &'a mut [u8],
        metadata: &'a mut <Self as ShmemBacked>::MetaData,
    ) -> Self::View {
        unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut T, *metadata) }
    }
}

use num_traits::{ops::checked::CheckedAdd, sign::Unsigned, NumOps};

trait AccessCounter: Copy + PartialOrd + Eq + NumOps + Unsigned + CheckedAdd {}

impl<T: Copy + PartialOrd + Eq + NumOps + Unsigned + CheckedAdd> AccessCounter for T {}

struct ShmemBase<A: AccessCounter, P: DropBehaviour, const N: usize> {
    tag: P,
    shmem: Shmem,
    access_counter_type: std::marker::PhantomData<A>,
    // In bytes
    counter_offset: usize,
    // In bytes
    data_offset: usize,
    // In bytes
    data_size: usize,
    free_shmem: bool,
}

impl<A: AccessCounter, P: DropBehaviour, const N: usize> ShmemBase<A, P, N> {
    /// Creates uninitialized shared memory to hold data of size `data_size`.
    /// The region at the start of the memory segment contains a mutex and N
    /// access counters of type `A` protected by the mutex. Use
    /// [`mutex_cond_write`] to read / change the access counter. The behaviour
    /// on `Drop` depends on the `DropBehaviour` impl on the `tag` field.
    fn new(access_counter: &[A; N], data_size: usize, tag: P) -> Result<Self, Error> {
        // `size_of` seems to check for `*mut u8` (or `usize`??) aligment of the
        // internal lock on *nix, see https://github.com/elast0ny/raw_sync-rs/blob/506c542600de9d79439ab4be5297e760bd428a2c/src/locks/unix.rs
        // Since below we're using the aligment checks for arrow which require at least
        // 8-byte alignment, we should be ok passing `None` here, bypassing the
        // check. This assumes a pointer size of no more than 8 bytes (i.e. no
        // larger than 64-bit systems).
        let lock_size = Mutex::size_of(None);
        // Ensure that the start of the memory region for user data follows arrow
        // alignment.
        let reserved_size =
            ((lock_size + std::mem::size_of::<A>() * N) / ALIGNMENT + 1) * ALIGNMENT;
        // Create shared memory.
        let shmem = ShmemConf::new()
            .size(reserved_size + data_size)
            .create()
            .map_err(Error::Shmem)?;
        // Create a mutex.
        let (mutex, _) = unsafe {
            Mutex::new(shmem.as_ptr(), shmem.as_ptr().add(lock_size))
                .map_err(|e| Error::Mutex(format!("{}", e)))?
        };
        // Initialize the sentinel value.
        {
            let lock = mutex.lock().map_err(|e| Error::Mutex(format!("{}", e)))?;
            unsafe {
                let counter_ptr = std::slice::from_raw_parts_mut(*lock as *mut A, N);
                counter_ptr.copy_from_slice(access_counter);
            }
        }
        // Prevents the mutex destructor from running, so we can use the same mutex for
        // serialization/deserialization later
        std::mem::forget(mutex);
        // TODO: Should we guarantee this?
        let aligned = unsafe { is_aligned(shmem.as_ptr().add(reserved_size), ALIGNMENT) };
        if !aligned {
            Err(Error::UnalignedMemory)
        } else {
            Ok(Self {
                tag,
                shmem,
                access_counter_type: std::marker::PhantomData,
                counter_offset: lock_size,
                data_offset: reserved_size,
                data_size,
                free_shmem: true,
            })
        }
    }

    /// Uses the mutex at `shmem.as_ptr()` to write the value returned by
    /// `write` to the sentinel location. If `write` returns `None`, nothing
    /// is written. Returns the previously held sentinel value.
    ///
    /// This function allows interior mutability through a non-mutable
    /// reference. This is necessary to allow implementing
    /// [`serde::Serialize`] for [`Shared`]/[`SharedMut`].
    /// ([`serde::Serialize::serialize`] takes a non-mutable reference.)
    fn mutex_write(&self, write: fn(&[A; N]) -> Option<[A; N]>) -> Result<[A; N], Error> {
        let (mutex, _) = unsafe {
            let counter_ptr = self.shmem.as_ptr().add(self.counter_offset);
            Mutex::from_existing(self.shmem.as_ptr(), counter_ptr)
                .map_err(|e| Error::Mutex(format!("{}", e)))?
        };
        let counter_values = {
            let lock = mutex.lock().map_err(|e| Error::Mutex(format!("{}", e)))?;
            let counter_ptr = unsafe { std::slice::from_raw_parts_mut(*lock as *mut A, N) };
            let mut old = [A::zero(); N];
            old.copy_from_slice(counter_ptr);
            if let Some(new) = write(&old) {
                counter_ptr.copy_from_slice(&new);
            }
            old
        };
        // Prevents the mutex destructor from running, so we can use the same mutex for
        // serialization/deserialization later
        std::mem::forget(mutex);
        Ok(counter_values)
    }

    /// If `free` is `true`, `ShmemBase` frees the shared memory & mutex on
    /// drop depending on the `DropBehaviour` impl on the `tag` field.
    fn free_on_drop(&mut self, free: bool) {
        self.free_shmem = free;
    }

    /// Returns a pointer to the data segment of the underlying shared memory.
    fn data_ptr(&self) -> *const u8 {
        unsafe { self.shmem.as_ptr().add(self.data_offset) }
    }

    /// Returns a mutable pointer to the data segment of the underlying shared
    /// memory.
    fn data_ptr_mut(&mut self) -> *mut u8 {
        unsafe { self.shmem.as_ptr().add(self.data_offset) }
    }

    fn to_wire_format<T>(&self, metadata: T) -> WireFormat<T> {
        WireFormat {
            tag: self.tag.clone().into(),
            os_id: String::from(self.shmem.get_os_id()),
            mem_size: self.shmem.len(),
            counter_offset: self.counter_offset,
            data_offset: self.data_offset,
            data_size: self.data_size,
            meta: metadata,
        }
    }

    /// Serializes a `ShmemBase` into the wire format **without** freeing the
    /// shared memory & mutex.
    fn into_wire_format<T>(mut self, metadata: T) -> WireFormat<T> {
        // Prevents the shared memory & mutex from being freed on drop
        self.free_shmem = false;
        WireFormat {
            tag: self.tag.clone().into(),
            os_id: String::from(self.shmem.get_os_id()),
            mem_size: self.shmem.len(),
            counter_offset: self.counter_offset,
            data_offset: self.data_offset,
            data_size: self.data_size,
            meta: metadata,
        }
    }

    /// Creates a `ShmemBase` which **does not** free the shared memory & mutex
    /// on drop.
    fn from_wire_format<T>(wire_format: WireFormat<T>) -> Result<(Self, T), Error> {
        let WireFormat {
            tag,
            os_id,
            mem_size,
            counter_offset,
            data_offset,
            data_size,
            meta,
        } = wire_format;
        let shmem = ShmemConf::new()
            .os_id(os_id)
            .size(mem_size)
            .open()
            .map_err(Error::Shmem)?;
        Ok((
            Self {
                tag: tag.try_into()?,
                shmem,
                access_counter_type: std::marker::PhantomData,
                counter_offset,
                data_offset,
                data_size,
                free_shmem: false,
            },
            meta,
        ))
    }
}

impl<A: AccessCounter, P: DropBehaviour, const N: usize> Drop for ShmemBase<A, P, N> {
    fn drop(&mut self) {
        P::called_on_drop(self);
    }
}

trait DropBehaviour: Clone + TryFrom<Tag, Error = Error> + Into<Tag> {
    fn called_on_drop<A: AccessCounter, P: DropBehaviour, const N: usize>(
        base: &mut ShmemBase<A, P, N>,
    );
}

impl<A: AccessCounter> Clone for ShmemBase<A, SharedTag, 2> {
    fn clone(&self) -> Self {
        let shmem = ShmemConf::new()
            .os_id(self.shmem.get_os_id())
            .size(self.shmem.len())
            .open()
            .unwrap();
        let new = Self {
            tag: SharedTag(),
            shmem,
            access_counter_type: std::marker::PhantomData,
            counter_offset: self.counter_offset,
            data_offset: self.data_offset,
            data_size: self.data_size,
            free_shmem: true,
        };
        // Increment the access counter, keep the serialization counter
        let write: fn(&[A; 2]) -> Option<[A; 2]> = |old| {
            let mut new = [A::zero(); 2];
            if let Some(new_acc_count) = old[0].checked_add(&A::one()) {
                new[0] = new_acc_count;
                new[1] = old[1];
                Some(new)
            } else {
                panic!("Can't have more than A::MAX `Shared`s with simultaneous access.")
            }
        };
        new.mutex_write(write).unwrap();
        new
    }
}

impl<A: AccessCounter> From<ShmemBase<A, SharedMutTag, 2>> for ShmemBase<A, SharedTag, 2> {
    fn from(mut shared_mut: ShmemBase<A, SharedMutTag, 2>) -> Self {
        // Don't free the shared memory on drop.
        shared_mut.free_on_drop(false);
        // Need to create new `Shmem` because `shared_mut` implements `Drop`, so we
        // can't move out of it. Option<shmem>?
        let shmem = ShmemConf::new()
            .os_id(shared_mut.shmem.get_os_id())
            .size(shared_mut.shmem.len())
            .open()
            .unwrap();
        let new = Self {
            tag: SharedTag(),
            shmem,
            access_counter_type: std::marker::PhantomData,
            counter_offset: shared_mut.counter_offset,
            data_offset: shared_mut.data_offset,
            data_size: shared_mut.data_size,
            free_shmem: true,
        };
        // Set the access counter to 1 and the serialization counter to 0
        new.mutex_write(|_| Some([A::one(), A::zero()])).unwrap();
        new
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct SharedTag();

impl TryFrom<Tag> for SharedTag {
    type Error = Error;
    fn try_from(value: Tag) -> Result<Self, Self::Error> {
        match value {
            Tag::Shared(tag) => Ok(tag),
            Tag::SharedMut(_) => Err(Error::Deserialization(String::from(
                "Can't deserialize a `Shared` from a `SharedMut` serialization.",
            ))),
        }
    }
}

impl DropBehaviour for SharedTag {
    fn called_on_drop<A: AccessCounter, P: DropBehaviour, const N: usize>(
        base: &mut ShmemBase<A, P, N>,
    ) {
        // `free_shmem` is **only** true when `base` drops outside of a serialization
        // function.
        if base.free_shmem {
            // Decrement the access counter, keep the serialization counter
            let write: fn(&[A; N]) -> Option<[A; N]> = |old| {
                let mut new = [A::zero(); N];
                new[0] = old[0] - A::one();
                new[1] = old[1];
                Some(new)
            };
            let counter_value = base.mutex_write(write).unwrap();
            // Only free the shared memory if the access counter was 1 and the serialization
            // counter was 0 (exclusive access).
            if (counter_value[0] == A::one()) && (counter_value[1] == A::zero()) {
                // Drops the mutex in shared memory, cleaning up any additional (kernel?)
                // memory. This seems to be unnessecary on *nix, see
                // https://stackoverflow.com/questions/39822987/pthread-mutexattr-process-shared-memory-leak
                unsafe {
                    let counter_ptr = base.shmem.as_ptr().add(base.counter_offset);
                    Mutex::from_existing(base.shmem.as_ptr(), counter_ptr).unwrap();
                }
                base.shmem.set_owner(true);
            } else {
                base.shmem.set_owner(false);
            }
        } else {
            // Prevents 'shmem' from freeing the shared memory when dropped.
            // Windows behavior should be fixed with https://github.com/elast0ny/shared_memory-rs/pull/59
            base.shmem.set_owner(false);
        }
    }
}

/// Wrapper type for immutable access to shared memory from multiple processes.
pub struct Shared<T>
where
    T: ShmemBacked + ?Sized,
{
    metadata: <T as ShmemBacked>::MetaData,
    shmem: ShmemBase<u64, SharedTag, 2>,
}

impl<T> Shared<T>
where
    T: ShmemBacked + for<'a> ShmemView<'a> + ?Sized,
{
    pub fn new(arg: &<T as ShmemBacked>::NewArg) -> Result<Self, Error> {
        let size = T::required_memory_arg(&arg);
        // The strategy is to use a pair of u64s as counters: the first one keeps track
        // of the number of `Shared`s accessing the shared memory (access
        // counter), the second one tracks the number of serialized instances
        // (serialization counter).
        //
        // The counters live at the start of the shared memory region. The access
        // counter is incremented when calling `new`, `deserialize` or `clone`
        // and decremented when dropping or calling
        // `into_serialized`. The serialization
        // counter is incremented when calling
        // `into_serialized`/`serde::Serialize::serialize`, and decremented when
        // calling `deserialize`.
        //
        // Shared memory is only freed if the access count is 1 and the serialization
        // count is 0 (exclusive access) on drop and we're not serializing.
        let mut shmem = ShmemBase::new(&[1_u64, 0], size, SharedTag())?;
        let metadata = unsafe {
            let data = std::slice::from_raw_parts_mut(shmem.data_ptr_mut(), shmem.data_size);
            T::new(data, arg)
        };
        Ok(Shared { metadata, shmem })
    }

    pub fn new_from_inner(arg: &T) -> Result<Self, Error> {
        let size = T::required_memory_src(&arg);
        let mut shmem = ShmemBase::new(&[1_u64, 0], size, SharedTag())?;
        let inner = unsafe {
            let data = std::slice::from_raw_parts_mut(shmem.data_ptr_mut(), shmem.data_size);
            T::new_from_src(data, arg)
        };
        Ok(Shared {
            metadata: inner,
            shmem,
        })
    }

    #[allow(clippy::clippy::needless_lifetimes)]
    pub fn as_view<'a>(&'a self) -> <T as ShmemView<'a>>::View {
        let data =
            unsafe { std::slice::from_raw_parts(self.shmem.data_ptr(), self.shmem.data_size) };
        T::view(data, &self.metadata)
    }

    #[cfg(test)]
    pub fn counts(&self) -> Result<[u64; 2], Error> {
        self.shmem.mutex_write(|_| None)
    }

    pub fn into_serialized<S: Serializer>(self, serializer: S) -> Result<S::Ok, S::Error> {
        // Decrements the access counter and increments the serialized counter (see
        // [`Self::new`]).
        let write: fn(&[u64; 2]) -> Option<[u64; 2]> = |old| {
            let mut new = [0_u64, 0];
            if let Some(new_ser_count) = old[1].checked_add(1) {
                new[0] = old[0] - 1;
                new[1] = new_ser_count;
                Some(new)
            } else {
                panic!("Can't have more than A::MAX serialized `Shared`s.")
            }
        };
        self.shmem.mutex_write(write).map_err(ser::Error::custom)?;
        let wire_format = self.shmem.into_wire_format(self.metadata);
        wire_format.serialize(serializer)
    }
}

impl<T> Serialize for Shared<T>
where
    T: ShmemBacked + ?Sized,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Increments the serialized counter (see [`Self::new`]).
        let write: fn(&[u64; 2]) -> Option<[u64; 2]> = |old| {
            let mut new = [0_u64, 0];
            if let Some(new_ser_count) = old[1].checked_add(1) {
                new[0] = old[0];
                new[1] = new_ser_count;
                Some(new)
            } else {
                panic!("Can't have more than A::MAX serialized `Shared`s.")
            }
        };
        self.shmem.mutex_write(write).map_err(ser::Error::custom)?;
        let wire_format = self.shmem.to_wire_format(&self.metadata);
        wire_format.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Shared<T>
where
    T: ShmemBacked + ?Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire_format = WireFormat::deserialize(deserializer)?;
        // The created `shmem` does not free the shared memory, see
        // (`SharedBase::from_wire_format`).
        let (mut shmem, metadata) = ShmemBase::<u64, SharedTag, 2>::from_wire_format(wire_format)
            .map_err(de::Error::custom)?;
        // Increments the access counter and decrements the serialized counter (see
        // [`Self::new`]).
        let write: fn(&[u64; 2]) -> Option<[u64; 2]> = |old| {
            let mut new = [0_u64, 0];
            if let Some(new_acc_count) = old[0].checked_add(1) {
                new[0] = new_acc_count;
                new[1] = old[1].saturating_sub(1);
                Some(new)
            } else {
                None
            }
        };
        if shmem.mutex_write(write).map_err(de::Error::custom)?[0] < u64::MAX {
            // `shmem` does not free the shared memory in case of an error.
            // In the `Ok` case it should do so.
            shmem.free_on_drop(true);
            Ok(Shared { metadata, shmem })
        } else {
            Err(de::Error::custom(
                "Can't have more than u64::MAX `Shared`s with simultaneous access.",
            ))
        }
    }
}

unsafe impl<T> Send for Shared<T>
where
    T: ShmemBacked + ?Sized,
    T::MetaData: Send,
{
}
// TODO: Is the mutex around the counters enough to make a `Shared` `Sync`?
// unsafe impl<T> Sync for Shared<T> where T: ShmemBacked + ?Sized, T::MetaData:
// Sync {}

impl<'a, T> From<&'a T> for Shared<T>
where
    T: ShmemBacked + for<'b> ShmemView<'b> + ?Sized,
{
    fn from(src: &'a T) -> Self {
        Self::new_from_inner(src).unwrap()
    }
}

impl<T> Clone for Shared<T>
where
    T: ShmemBacked + ?Sized,
{
    /// Creates another `Shared` instance backed by the same shared memory. No
    /// data is copied.
    fn clone(&self) -> Self {
        let Shared {
            shmem, metadata, ..
        } = self;
        let shmem = shmem.clone();
        Self {
            metadata: metadata.clone(),
            shmem,
        }
    }
}

impl<T> TryFrom<SharedMut<T>> for Shared<T>
where
    T: ShmemBacked + ?Sized,
{
    type Error = Error;

    /// This method fails if the `SharedMut` has previously been
    /// invalidated with a call to `serde::Serialize::serialize`.
    fn try_from(shared_mut: SharedMut<T>) -> Result<Self, Self::Error> {
        let SharedMut { shmem, metadata } = shared_mut;
        if let Some(shmem) = shmem.into_inner() {
            let shmem: ShmemBase<_, SharedTag, 2> = shmem.into();
            Ok(Self { metadata, shmem })
        } else {
            Err(Error::InvalidSharedMut)
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct SharedMutTag();

impl TryFrom<Tag> for SharedMutTag {
    type Error = Error;
    fn try_from(value: Tag) -> Result<Self, Self::Error> {
        match value {
            Tag::SharedMut(tag) => Ok(tag),
            Tag::Shared(_) => Err(Error::Deserialization(String::from(
                "Can't deserialize a `SharedMut` from a `Shared` serialization.",
            ))),
        }
    }
}

impl DropBehaviour for SharedMutTag {
    fn called_on_drop<A: AccessCounter, P: DropBehaviour, const N: usize>(
        base: &mut ShmemBase<A, P, N>,
    ) {
        if base.free_shmem {
            // Drops the mutex in shared memory, cleaning up any additional (kernel?)
            // memory. This seems to be unnessecary on *nix, see
            // https://stackoverflow.com/questions/39822987/pthread-mutexattr-process-shared-memory-leak
            unsafe {
                let counter_ptr = base.shmem.as_ptr().add(base.counter_offset);
                Mutex::from_existing(base.shmem.as_ptr(), counter_ptr).unwrap();
            }
            base.shmem.set_owner(true);
        } else {
            // Prevents 'shmem' from freeing the shared memory when dropped.
            // Windows behavior should be fixed with https://github.com/elast0ny/shared_memory-rs/pull/59
            base.shmem.set_owner(false);
        }
    }
}

///  Safe mutable access to shared memory from multiple processes through unique ownership.
pub struct SharedMut<T>
where
    T: ShmemBacked + ?Sized,
{
    metadata: <T as ShmemBacked>::MetaData,
    shmem: UnsafeCell<Option<ShmemBase<u64, SharedMutTag, 2>>>,
}

impl<T> SharedMut<T>
where
    T: ShmemBacked + for<'a> ShmemView<'a> + for<'a> ShmemViewMut<'a> + ?Sized,
{
    pub fn new(arg: &<T as ShmemBacked>::NewArg) -> Result<Self, Error> {
        let size = T::required_memory_arg(&arg);
        // The strategy is to reserve a u64 at the start of the memory segment which is
        // set to 1 when a `SharedMut` instance exists in any process and set to
        // 0 otherwise. This lets us throw an error if attempting to deserialize
        // a `SharedMut` backed by shared memory which is already accessed from
        // somewhere else.
        //
        // The `access_counter` array has two elements to make the memory behind
        // `SharedMut` compatible with `Shared` for easy conversion with
        // `From<SharedMut> for Shared`, the second element is not used.
        let mut shmem = ShmemBase::new(&[1_u64, 0], size, SharedMutTag())?;
        let metadata = unsafe {
            let data = std::slice::from_raw_parts_mut(shmem.data_ptr_mut(), shmem.data_size);
            T::new(data, arg)
        };
        Ok(SharedMut {
            metadata,
            shmem: UnsafeCell::new(Some(shmem)),
        })
    }

    pub fn new_from_inner(arg: &T) -> Result<Self, Error> {
        let size = T::required_memory_src(&arg);
        let mut shmem = ShmemBase::new(&[1_u64, 0], size, SharedMutTag())?;
        let metadata = unsafe {
            let data = std::slice::from_raw_parts_mut(shmem.data_ptr_mut(), shmem.data_size);
            T::new_from_src(data, arg)
        };
        Ok(SharedMut {
            metadata,
            shmem: UnsafeCell::new(Some(shmem)),
        })
    }

    #[allow(clippy::clippy::needless_lifetimes)]
    pub fn as_view_mut<'a>(&'a mut self) -> <T as ShmemViewMut<'a>>::View {
        let shmem =
            self.shmem.get_mut().as_mut().expect(
                "`SharedMut` must not be used after a call to `serde::Serialize::serialize`.",
            );
        let data = unsafe { std::slice::from_raw_parts_mut(shmem.data_ptr_mut(), shmem.data_size) };
        T::view_mut(data, &mut self.metadata)
    }
}

impl<T> SharedMut<T>
where
    T: ShmemBacked + for<'a> ShmemView<'a> + ?Sized,
{
    #[allow(clippy::clippy::needless_lifetimes)]
    pub fn as_view<'a>(&'a self) -> <T as ShmemView<'a>>::View {
        let data = unsafe {
            let shmem: &mut _ = &mut *self.shmem.get();
            let shmem = shmem.as_mut().expect(
                "`SharedMut` must not be used after a call to `serde::Serialize::serialize`.",
            );
            std::slice::from_raw_parts(shmem.data_ptr(), shmem.data_size)
        };
        T::view(data, &self.metadata)
    }

    /// Allows directly modifying the metadata used to create the view types
    /// from raw memory. See the [`ShmemBacked`]/[`ShmemView`]/[`ShmemViewMut`]
    /// traits for details.
    ///
    /// # Safety
    /// [`ShmemView::view`]/[`ShmemViewMut::view_mut`] use metadata to create
    /// views from raw memory, usually through the use of `unsafe`
    /// operations. Directly changing the metadata is almost always a bad
    /// idea unless the changes come from a view into the same memory.
    pub unsafe fn metadata_mut(&mut self) -> &mut <T as ShmemBacked>::MetaData {
        &mut self.metadata
    }

    #[cfg(test)]
    pub fn counts(&mut self) -> Result<[u64; 1], Error> {
        let shmem =
            self.shmem.get_mut().as_mut().expect(
                "`SharedMut` must not be used after a call to `serde::Serialize::serialize`.",
            );
        let mut access_count = [0];
        let counts = shmem.mutex_write(|_| None)?;
        access_count[0] = counts[0];
        Ok(access_count)
    }

    pub fn into_serialized<S: Serializer>(self, serializer: S) -> Result<S::Ok, S::Error> {
        let shmem = self
            .shmem
            .into_inner()
            .expect("`SharedMut` must not be used after a call to `serde::Serialize::serialize`.");
        // Sets the access counter (see [`Self::new`]) to 0, indicating we're done
        // accessing the memory segment.
        let write: fn(&[u64; 2]) -> Option<[u64; 2]> = |old| {
            debug_assert_eq!(old[0], 1);
            Some([0_u64, 0])
        };
        shmem.mutex_write(write).map_err(ser::Error::custom)?;
        let wire_format = shmem.into_wire_format(self.metadata);
        wire_format.serialize(serializer)
    }
}

impl<T> Serialize for SharedMut<T>
where
    T: ShmemBacked + ?Sized,
{
    /// # Safety
    /// This function can only be called **once**. It makes use of interior
    /// mutability to invalidate `Self`, any future use leads to a panic.
    /// This is a workaround to provide an implementation of
    /// [`serde::Serialize`] even though `serialize` takes an immutable
    /// reference to `Self`.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let shmem: &mut _ = unsafe { &mut *self.shmem.get() };
        let shmem = shmem
            .take()
            .expect("`SharedMut` must not be used after a call to `serde::Serialize::serialize`.");
        // Sets the access counter (see [`Self::new`]) to 0, indicating we're done
        // accessing the memory segment.
        let write: fn(&[u64; 2]) -> Option<[u64; 2]> = |old| {
            debug_assert_eq!(old[0], 1);
            Some([0_u64, 0])
        };
        shmem.mutex_write(write).map_err(ser::Error::custom)?;
        let wire_format = shmem.into_wire_format(self.metadata.clone());
        wire_format.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for SharedMut<T>
where
    T: ShmemBacked + ?Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire_format = WireFormat::deserialize(deserializer)?;
        // The created `shmem` does not free the shared memory, see
        // (`SharedBase::from_wire_format`).
        let (mut shmem, metadata) =
            ShmemBase::<u64, SharedMutTag, 2>::from_wire_format(wire_format)
                .map_err(de::Error::custom)?;
        // If the access counter is 0 (no other `SharedMut`s accessing this data exist),
        // set the value to 1 and create a `Self` instance. If not throw an error.
        let write: fn(&[u64; 2]) -> Option<[u64; 2]> = |old| {
            debug_assert!(old[0] <= 1);
            if old[0] == 0 {
                Some([1, 0])
            } else {
                None
            }
        };
        if shmem.mutex_write(write).map_err(de::Error::custom)?[0] == 0 {
            // `shmem` does not free the shared memory in case of an error.
            // In the `Ok` case it should do so.
            shmem.free_on_drop(true);
            Ok(SharedMut {
                metadata,
                shmem: UnsafeCell::new(Some(shmem)),
            })
        } else {
            Err(de::Error::custom("A shared memory region can only be accessed by one `SharedMut` instance at any time. Note that the existing instance may live in a different process."))
        }
    }
}

unsafe impl<T> Send for SharedMut<T>
where
    T: ShmemBacked + ?Sized,
    T::MetaData: Send,
{
}
// A `SharedMut` is **not** `Sync` because the `serde::Serialize` impl relies on
// interior mutability through an immutable reference with an `UnsafeCell`.

impl<'a, T> From<&'a T> for SharedMut<T>
where
    T: ShmemBacked + for<'b> ShmemView<'b> + for<'b> ShmemViewMut<'b> + ?Sized,
{
    fn from(src: &'a T) -> Self {
        Self::new_from_inner(src).unwrap()
    }
}

#[derive(Serialize, Deserialize)]
struct WireFormat<T> {
    tag: Tag,
    os_id: String,
    mem_size: usize,
    counter_offset: usize,
    data_offset: usize,
    data_size: usize,
    meta: T,
}

#[derive(Serialize, Deserialize)]
enum Tag {
    Shared(SharedTag),
    SharedMut(SharedMutTag),
}

impl From<SharedTag> for Tag {
    fn from(shared: SharedTag) -> Self {
        Tag::Shared(shared)
    }
}

impl From<SharedMutTag> for Tag {
    fn from(shared: SharedMutTag) -> Self {
        Tag::SharedMut(shared)
    }
}

/// A `str` in shared memory.
pub type SharedStr = Shared<str>;

impl Deref for SharedStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_view()
    }
}

/// A mutable `str` in shared memory.
pub type SharedStrMut = SharedMut<str>;

impl Deref for SharedStrMut {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_view()
    }
}

impl DerefMut for SharedStrMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_view_mut()
    }
}

/// A `slice` in shared memory.
pub type SharedSlice<T> = Shared<[T]>;

impl<T: Copy + 'static> Deref for SharedSlice<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_view()
    }
}

/// A mutable `slice` in shared memory.
pub type SharedSliceMut<T> = SharedMut<[T]>;

impl<T: Copy + 'static> Deref for SharedSliceMut<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_view()
    }
}

impl<T: Copy + 'static> DerefMut for SharedSliceMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_view_mut()
    }
}

#[cfg(feature = "shared_ndarray")]
pub use sharify_ndarray::{SharedArray, SharedArrayMut};

#[cfg(feature = "shared_ndarray")]
pub mod sharify_ndarray {
    use super::*;
    use ndarray::{Array, ArrayView, ArrayViewMut, Dimension};

    /// An immutable [`ndarray::Array`] whose data lives in shared memory.
    pub type SharedArray<T, D> = Shared<Array<T, D>>;
    /// A mutable [`ndarray::Array`] whose data lives in shared memory.
    pub type SharedArrayMut<T, D> = SharedMut<Array<T, D>>;

    unsafe impl<'a, T, D> ShmemBacked for Array<T, D>
    where
        T: Copy,
        D: Dimension + Serialize + DeserializeOwned,
    {
        type NewArg = (T, D);
        type MetaData = (Vec<usize>, Vec<isize>);

        fn required_memory_arg((_, dim): &Self::NewArg) -> usize {
            dim.size() * std::mem::size_of::<T>()
        }

        fn required_memory_src(src: &Self) -> usize {
            src.len() * std::mem::size_of::<T>()
        }

        fn new(data: &mut [u8], (init, dim): &Self::NewArg) -> Self::MetaData {
            let data =
                unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut T, dim.size()) };
            for element in data.iter_mut() {
                *element = *init;
            }
            let view = ArrayView::from_shape(dim.clone(), data).unwrap();
            let shape = Vec::from(view.shape());
            let strides = Vec::from(view.strides());
            (shape, strides)
        }

        fn new_from_src(data: &mut [u8], src: &Self) -> Self::MetaData {
            let data =
                unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut T, src.len()) };
            let mut view = ArrayViewMut::from_shape(src.raw_dim(), data).unwrap();
            for (src, dst) in src.iter().zip(view.iter_mut()) {
                *dst = *src;
            }
            let shape = Vec::from(view.shape());
            let strides = Vec::from(view.strides());
            (shape, strides)
        }
    }

    impl<'a, T, D> ShmemView<'a> for Array<T, D>
    where
        T: Copy + Default + 'a,
        D: Dimension + Serialize + DeserializeOwned,
    {
        type View = ArrayView<'a, T, D>;

        fn view(
            data: &'a [u8],
            (shape, strides): &'a <Self as ShmemBacked>::MetaData,
        ) -> Self::View {
            use ndarray::ShapeBuilder;
            debug_assert!(shape.iter().product::<usize>() <= data.len());
            let data = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const T, shape.iter().product())
            };
            let mut shape_dim = D::zeros(shape.len());
            for (src, dst) in shape.iter().zip(shape_dim.as_array_view_mut().iter_mut()) {
                *dst = *src;
            }
            let mut strides_dim = D::zeros(strides.len());
            for (src, dst) in strides
                .iter()
                .zip(strides_dim.as_array_view_mut().iter_mut())
            {
                // Conversion for negative strides, see https://github.com/rust-ndarray/ndarray/pull/948/files
                // and https://github.com/rust-ndarray/ndarray/pull/948
                *dst = *src as usize;
            }
            ArrayView::from_shape(shape_dim.strides(strides_dim), data).unwrap()
        }
    }

    impl<'a, T, D> ShmemViewMut<'a> for Array<T, D>
    where
        T: Copy + Default + 'a,
        D: Dimension + Serialize + DeserializeOwned,
    {
        type View = ArrayViewMut<'a, T, D>;

        fn view_mut(
            data: &'a mut [u8],
            (shape, strides): &'a mut <Self as ShmemBacked>::MetaData,
        ) -> Self::View {
            use ndarray::ShapeBuilder;
            debug_assert!(shape.iter().product::<usize>() <= data.len());
            let data = unsafe {
                std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut T, shape.iter().product())
            };
            let mut shape_dim = D::zeros(shape.len());
            for (src, dst) in shape.iter().zip(shape_dim.as_array_view_mut().iter_mut()) {
                *dst = *src;
            }
            let mut strides_dim = D::zeros(strides.len());
            for (src, dst) in strides
                .iter()
                .zip(strides_dim.as_array_view_mut().iter_mut())
            {
                // Conversion for negative strides, see https://github.com/rust-ndarray/ndarray/pull/948/files
                // and https://github.com/rust-ndarray/ndarray/pull/948
                *dst = *src as usize;
            }
            ArrayViewMut::from_shape(shape_dim.strides(strides_dim), data).unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{self, de, options};
    use rand::prelude::*;
    use std::thread;

    fn serialize_shared<T>(shared: Shared<T>) -> Vec<u8>
    where
        T: ShmemBacked + for<'a> ShmemView<'a> + ?Sized,
    {
        let mut bytes = Vec::new();
        let mut serializer = bincode::Serializer::new(&mut bytes, options());
        shared.into_serialized(&mut serializer).unwrap();
        bytes
    }

    fn serialize_shared_mut<T>(shared: SharedMut<T>) -> Vec<u8>
    where
        T: ShmemBacked + for<'a> ShmemView<'a> + ?Sized,
    {
        let mut bytes = Vec::new();
        let mut serializer = bincode::Serializer::new(&mut bytes, options());
        shared.into_serialized(&mut serializer).unwrap();
        bytes
    }

    fn deserialize<S: DeserializeOwned>(bytes: &[u8]) -> Result<S, String> {
        let mut deserializer = de::Deserializer::from_slice(bytes, options());
        S::deserialize(&mut deserializer).map_err(|e| format!("{}", e))
    }

    fn serialization_roundtrip_shared<T>(shared: Shared<T>) -> Shared<T>
    where
        T: ShmemBacked + for<'a> ShmemView<'a> + ?Sized,
    {
        let mut bytes = Vec::new();
        let mut serializer = bincode::Serializer::new(&mut bytes, options());
        shared.into_serialized(&mut serializer).unwrap();
        let mut deserializer = de::Deserializer::from_slice(bytes.as_slice(), options());
        Shared::<T>::deserialize(&mut deserializer).unwrap()
    }

    fn serialization_roundtrip_shared_mut<T>(shared: SharedMut<T>) -> SharedMut<T>
    where
        T: ShmemBacked + for<'a> ShmemView<'a> + ?Sized,
    {
        let mut bytes = Vec::new();
        let mut serializer = bincode::Serializer::new(&mut bytes, options());
        shared.into_serialized(&mut serializer).unwrap();
        let mut deserializer = de::Deserializer::from_slice(bytes.as_slice(), options());
        SharedMut::<T>::deserialize(&mut deserializer).unwrap()
    }

    fn slice_check_src_shared<T>(slice: &[T])
    where
        T: Copy + PartialEq + std::fmt::Debug + ?Sized + 'static,
    {
        let shared = Shared::<[T]>::from(slice);
        let roundtrip = serialization_roundtrip_shared(shared);
        assert_eq!(roundtrip.as_view(), slice);
    }

    fn slice_check_src_shared_mut<T>(slice: &[T])
    where
        T: Copy + PartialEq + std::fmt::Debug + ?Sized + 'static,
    {
        let shared = SharedMut::<[T]>::from(slice);
        let roundtrip = serialization_roundtrip_shared_mut(shared);
        assert_eq!(roundtrip.as_view(), slice);
    }

    #[test]
    fn shared_str() {
        let s = "sharify_test";
        let shared: Shared<str> = Shared::new(s).unwrap();
        let roundtrip = serialization_roundtrip_shared(shared);
        assert_eq!(roundtrip.as_view(), s);
    }

    #[test]
    fn shared_mut_str() {
        let s = "sharify_test";
        let shared: SharedMut<str> = SharedMut::new(s).unwrap();
        let roundtrip = serialization_roundtrip_shared_mut(shared);
        assert_eq!(roundtrip.as_view(), s);
    }

    enum Slice {
        Usize(&'static [usize]),
        U8(&'static [u8]),
        U64(&'static [u16]),
        I16(&'static [i16]),
        F64(&'static [f64]),
    }

    impl Slice {
        fn create_slices() -> Vec<Slice> {
            vec![
                Slice::Usize(&[1, 2, 3, 4, 5]),
                Slice::U8(&[1, 2, 3, 4, 5]),
                Slice::U64(&[1, 2, 3, 4, 5]),
                Slice::I16(&[1, 2, 3, 4, 5]),
                Slice::F64(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            ]
        }
    }

    #[test]
    fn shared_slice() {
        let slice: &[usize] = &[0_usize, 0, 0, 0, 0];
        let shared: Shared<[usize]> = Shared::new(&(0_usize, 5)).unwrap();
        let roundtrip = serialization_roundtrip_shared(shared);
        assert_eq!(roundtrip.as_view(), slice);
    }

    #[test]
    fn shared_mut_slice() {
        let slice: &mut [usize] = &mut [1_usize, 2, 3, 4, 5];
        let mut shared: SharedMut<[usize]> = SharedMut::new(&(0_usize, 5)).unwrap();
        shared.deref_mut().copy_from_slice(slice);
        let roundtrip = serialization_roundtrip_shared_mut(shared);
        assert_eq!(roundtrip.as_view(), slice);
    }

    #[test]
    fn shared_slice_from_src() {
        let slices = Slice::create_slices();
        for s in slices {
            match s {
                Slice::Usize(s) => slice_check_src_shared(s),
                Slice::U8(s) => slice_check_src_shared(s),
                Slice::U64(s) => slice_check_src_shared(s),
                Slice::I16(s) => slice_check_src_shared(s),
                Slice::F64(s) => slice_check_src_shared(s),
            }
        }
    }

    #[test]
    fn shared_mut_slice_from_src() {
        let slices = Slice::create_slices();
        for s in slices {
            match s {
                Slice::Usize(s) => slice_check_src_shared_mut(s),
                Slice::U8(s) => slice_check_src_shared_mut(s),
                Slice::U64(s) => slice_check_src_shared_mut(s),
                Slice::I16(s) => slice_check_src_shared_mut(s),
                Slice::F64(s) => slice_check_src_shared_mut(s),
            }
        }
    }

    #[test]
    fn shared_memory() {
        let shared: Shared<[usize]> = Shared::new(&(0_usize, 5)).unwrap();
        assert_eq!(shared.counts().unwrap(), [1, 0]);
        let bytes = serialize_shared(shared);
        let deser: Shared<[usize]> = deserialize(bytes.as_slice()).unwrap();
        assert_eq!(deser.counts().unwrap(), [1, 0]);
        // Should be able to create more than one `Shared`
        let mut instances = Vec::new();
        for i in 1..=10 {
            let inst: Shared<[usize]> = deserialize(bytes.as_slice()).unwrap();
            assert_eq!(deser.counts().unwrap(), [1 + i, 0]);
            instances.push(inst);
        }
        // Data should only be freed when all `Shared`s have been dropped.
        assert_eq!(&[0_usize, 0, 0, 0, 0], deser.deref());
        std::mem::drop(instances);
        assert_eq!(deser.counts().unwrap(), [1, 0]);
        assert_eq!(&[0_usize, 0, 0, 0, 0], deser.deref());
        std::mem::drop(deser);
        assert!(deserialize::<Shared::<[usize]>>(bytes.as_slice()).is_err());
    }

    #[test]
    fn shared_mut_memory() {
        let mut shared: SharedMut<[usize]> = SharedMut::new(&(0_usize, 5)).unwrap();
        assert_eq!(shared.counts().unwrap(), [1]);
        let bytes = serialize_shared_mut(shared);
        let mut deser: SharedMut<[usize]> = deserialize(bytes.as_slice()).unwrap();
        assert_eq!(deser.counts().unwrap(), [1]);
        // Should not be able to create more than one `SharedMut`
        assert!(deserialize::<SharedMut::<[usize]>>(bytes.as_slice()).is_err());
        // ...but a deserialization error must leave the data intact.
        assert_eq!(&[0_usize, 0, 0, 0, 0], deser.deref());
        assert_eq!(deser.counts().unwrap(), [1]);
        // Data should only be freed on drop.
        std::mem::drop(deser);
        assert!(deserialize::<SharedMut::<[usize]>>(bytes.as_slice()).is_err());
    }

    #[test]
    fn cross_serialization_from_shared() {
        let shared: Shared<[usize]> = Shared::new(&(0_usize, 5)).unwrap();
        assert_eq!(&[0_usize, 0, 0, 0, 0], shared.deref());
        let bytes = serialize_shared(shared);
        // Serializing into a `SharedMut` is not allowed
        assert!(deserialize::<SharedMut::<[usize]>>(bytes.as_slice()).is_err());
        // ...but should be able to deserialize back into a `Shared`
        let shared: Shared<[usize]> = deserialize(bytes.as_slice()).unwrap();
        assert_eq!(shared.counts().unwrap(), [1, 0]);
        assert_eq!(&[0_usize, 0, 0, 0, 0], shared.deref());
    }

    #[test]
    fn cross_serialization_from_shared_mut() {
        let shared: SharedMut<[usize]> = SharedMut::new(&(0_usize, 5)).unwrap();
        assert_eq!(&[0_usize, 0, 0, 0, 0], shared.deref());
        let bytes = serialize_shared_mut(shared);
        // Serializing into a `Shared` is not allowed
        assert!(deserialize::<Shared::<[usize]>>(bytes.as_slice()).is_err());
        // ...but should be able to deserialize back into a `SharedMut`
        let mut shared: SharedMut<[usize]> = deserialize(bytes.as_slice()).unwrap();
        assert_eq!(shared.counts().unwrap(), [1]);
        assert_eq!(&[0_usize, 0, 0, 0, 0], shared.deref());
    }

    #[test]
    fn shared_mut_into_shared() {
        let mut shared_mut: SharedMut<[usize]> = SharedMut::new(&(0_usize, 5)).unwrap();
        shared_mut.deref_mut().copy_from_slice(&[1, 2, 3, 4, 5]);
        let shared: Shared<[usize]> = shared_mut.try_into().unwrap();
        assert_eq!(shared.counts().unwrap(), [1, 0]);
        assert_eq!(&[1, 2, 3, 4, 5], shared.deref());
    }

    #[test]
    fn shared_clone() {
        let shared = Shared::from(&[1_usize, 2, 3, 4, 5] as &[_]);
        let mut container = Vec::new();
        for i in 0..100 {
            let bytes = serialize_shared(shared.clone());
            container.push((bytes, shared.clone()));
            assert_eq!(shared.counts().unwrap(), [2 + i, 1 + i]);
        }
        assert_eq!(shared.counts().unwrap(), [101, 100]);
        for (i, (bytes, cl)) in container.into_iter().enumerate() {
            let mut _deser: Shared<[usize]> = deserialize(bytes.as_slice()).unwrap();
            assert_eq!(&[1_usize, 2, 3, 4, 5], cl.deref());
            assert_eq!(shared.counts().unwrap(), [102 - i as u64, 99 - i as u64]);
        }
        assert_eq!(shared.counts().unwrap(), [1, 0]);
        assert_eq!(&[1_usize, 2, 3, 4, 5], shared.deref());
    }

    #[test]
    fn races() {
        let shared = Shared::from(&[1_usize, 2, 3, 4, 5] as &[_]);
        let mut handles = Vec::new();
        for _ in 0..50 {
            let (send, recv) = std::sync::mpsc::sync_channel(0);
            let bytes_send = serialize_shared(shared.clone());
            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();
                recv.recv().unwrap();
                let mut shared: Shared<[usize]> = deserialize(bytes_send.as_slice()).unwrap();
                assert_eq!(&[1_usize, 2, 3, 4, 5], shared.deref());
                for _ in 0..1000 {
                    thread::sleep(std::time::Duration::from_millis(rng.gen_range(0..=5)));
                    let tmp = serialize_shared(shared);
                    thread::sleep(std::time::Duration::from_millis(rng.gen_range(0..=5)));
                    shared = deserialize(tmp.as_slice()).unwrap();
                    assert_eq!(&[1_usize, 2, 3, 4, 5], shared.deref());
                }
            });
            handles.push((handle, send));
        }
        thread::sleep(std::time::Duration::from_millis(100));
        for (_, send) in handles.iter() {
            send.send(()).unwrap();
        }
        for (handle, _) in handles {
            handle.join().unwrap();
        }
        assert_eq!(&[1_usize, 2, 3, 4, 5], shared.deref());
        assert_eq!(shared.counts().unwrap(), [1, 0]);
    }

    #[cfg(feature = "shared_ndarray")]
    mod ndarray_tests {
        use super::*;
        use ndarray::{Array, Axis, IxDyn};

        #[test]
        fn shared_ndarray() {
            let shared: SharedArray<u64, IxDyn> = Shared::new(&(0, IxDyn(&[3, 2]))).unwrap();
            let shared = serialization_roundtrip_shared(shared);
            assert_eq!(&[0; 6], shared.as_view().as_slice().unwrap());
        }

        #[test]
        fn shared_mut_ndarray() {
            let mut shared: SharedArrayMut<f64, IxDyn> =
                SharedMut::new(&(0.0, IxDyn(&[3, 2]))).unwrap();
            let slice: &[f64] = &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
            for (&x, element) in slice.iter().zip(shared.as_view_mut().iter_mut()) {
                *element = x;
            }
            let roundtrip = serialization_roundtrip_shared_mut(shared);
            assert_eq!(slice, roundtrip.as_view().as_slice().unwrap());
        }

        #[test]
        fn shared_mut_array_layout() {
            let mut array: SharedArrayMut<f64, ndarray::IxDyn> =
                SharedArrayMut::new(&(0.0, ndarray::IxDyn(&[100, 200, 300]))).unwrap();
            assert_eq!(array.as_view().strides(), &[200 * 300, 300, 1]);
            {
                let mut view = array.as_view_mut();
                assert!(view.is_standard_layout());
                view.swap_axes(0, 1);
                assert_eq!(view.strides(), &[300, 200 * 300, 1]);
                unsafe {
                    *array.metadata_mut() = (Vec::from(view.shape()), Vec::from(view.strides()));
                }
            }
            assert_eq!(array.as_view().shape(), &[200, 100, 300]);
            assert_eq!(array.as_view().strides(), &[300, 200 * 300, 1]);
            assert!(!array.as_view().is_standard_layout());
            let bytes = serialize_shared_mut(array);
            let deser: SharedArrayMut<f64, ndarray::IxDyn> = deserialize(bytes.as_slice()).unwrap();
            assert_eq!(deser.as_view().shape(), &[200, 100, 300]);
            assert_eq!(deser.as_view().strides(), &[300, 200 * 300, 1]);
            assert!(!deser.as_view().is_standard_layout());
        }

        #[test]
        fn shared_ndarray_from_src() {
            let mut src = Array::from_elem((100, 200, 300), 0_u64);
            src.invert_axis(Axis(1));
            let src_shape = Vec::from(src.shape());
            let array = Shared::new_from_inner(&src).unwrap();
            assert_eq!(src_shape, array.as_view().shape());
            assert!(src
                .iter()
                .zip(array.as_view().iter())
                .all(|(src, dst)| src == dst))
        }
    }
}
