pub mod constants;
pub mod decoder;
mod host_address;
mod message_id;
mod properties;
mod types;

pub use decoder::{
    BLANK_MAGIC_CODE, DecodeError, DecodeOptions, MESSAGE_MAGIC_CODE_V1, MESSAGE_MAGIC_CODE_V2,
    append_crc32_property, decode_many, decode_one, decode_record_properties,
};
pub use host_address::{MessageHostAddress, MessageHostAddressBytesError, MessageHostAddressError};
pub use message_id::{
    MessageId, MessageIdError, create_message_id, create_message_id_from_host_bytes,
    decode_message_id,
};
pub use properties::{decode_properties, encode_properties};
pub use types::{
    Message, MessageExt, MessageKeyCollectionError, MessageProperties, MessagePropertyError,
    MessagePropertyParseError, MessageQueue, MessageQueueCompareError, MessageSysFlagParseError,
    NAME_VALUE_SEPARATOR, PROPERTY_SEPARATOR, TopicFilterType,
};
