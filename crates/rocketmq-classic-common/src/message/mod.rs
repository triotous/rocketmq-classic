pub mod constants;
mod properties;
mod types;

pub use properties::{decode_properties, encode_properties};
pub use types::{
    Message, MessageExt, MessageProperties, MessagePropertyError, MessageQueue,
    MessageQueueCompareError, NAME_VALUE_SEPARATOR, PROPERTY_SEPARATOR,
};
