pub mod constants;
pub mod decoder;
mod host_address;
mod properties;
mod types;

pub use decoder::{
    BLANK_MAGIC_CODE, DecodeError, DecodeOptions, MESSAGE_MAGIC_CODE_V1, MESSAGE_MAGIC_CODE_V2,
    decode_many, decode_one,
};
pub use host_address::{MessageHostAddress, MessageHostAddressBytesError, MessageHostAddressError};
pub use properties::{decode_properties, encode_properties};
pub use types::{
    Message, MessageExt, MessageProperties, MessagePropertyError, MessagePropertyParseError,
    MessageQueue, MessageQueueCompareError, MessageSysFlagParseError, NAME_VALUE_SEPARATOR,
    PROPERTY_SEPARATOR, TopicFilterType,
};
