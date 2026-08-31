//! One module per endpoint. Anything used by a single handler lives in that handler's file;
//! only what is genuinely shared sits here.

mod health;
mod receive;
mod received;

pub use health::health;
pub use receive::receive;
pub use received::received;
