mod bounded;

// BoundedChannel is used in tests
#[allow(unused_imports)]
pub use bounded::{BoundedChannel, BoundedReceiver, BoundedSender, SendError, create_bounded_channel};