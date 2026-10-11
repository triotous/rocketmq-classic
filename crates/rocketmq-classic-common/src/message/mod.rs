pub mod constants;
pub mod decoder;
mod properties;
mod types;

pub use decoder::{
    BLANK_MAGIC_CODE, DecodeError, DecodeLimits, DecodeOptions, MESSAGE_MAGIC_CODE_V1,
    MESSAGE_MAGIC_CODE_V2, decode_many, decode_one,
};
pub use properties::{decode_properties, encode_properties};
pub use types::{
    Message, MessageExt, MessageProperties, MessagePropertyError, MessageQueue,
    MessageQueueCompareError, NAME_VALUE_SEPARATOR, PROPERTY_SEPARATOR,
};
