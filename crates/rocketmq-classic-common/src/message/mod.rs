mod compact;
pub mod constants;
pub mod decoder;
mod encoder;
mod host_address;
mod message_id;
mod properties;
mod types;

pub use compact::{
    CompactMessageError, count_inner_messages, decode_compact_message, decode_compact_messages,
    decode_inner_batch, encode_compact_message, encode_compact_messages,
};
pub use decoder::{
    BLANK_MAGIC_CODE, DecodeError, DecodeOptions, MESSAGE_MAGIC_CODE_V1, MESSAGE_MAGIC_CODE_V2,
    append_crc32_property, decode_many, decode_one, decode_record_properties,
};
pub use encoder::{MessageExtEncodeError, encode_message_ext, encode_message_ext_uniquely};
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
