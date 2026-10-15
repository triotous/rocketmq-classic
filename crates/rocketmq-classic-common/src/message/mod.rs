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
    BLANK_MAGIC_CODE, CHARSET_UTF8, DecodeError, DecodeOptions, MESSAGE_FLAG_POSITION,
    MESSAGE_MAGIC_CODE, MESSAGE_MAGIC_CODE_POSITION, MESSAGE_MAGIC_CODE_V1, MESSAGE_MAGIC_CODE_V2,
    MESSAGE_PHYSIC_OFFSET_POSITION, MESSAGE_STORE_TIMESTAMP_POSITION, MessageVersion,
    MessageVersionError, PHY_POS_POSITION, QUEUE_OFFSET_POSITION, SYSFLAG_POSITION,
    append_crc32_property, client_decode, client_decodes, client_decodes_with_body, decode_many,
    decode_many_as_client, decode_one, decode_one_as_client, decode_record_properties,
};
pub use encoder::{MessageExtEncodeError, encode_message_ext, encode_message_ext_uniquely};
pub use host_address::{MessageHostAddress, MessageHostAddressBytesError, MessageHostAddressError};
pub use message_id::{
    MessageId, MessageIdError, create_message_id, create_message_id_from_host_bytes,
    decode_message_id,
};
pub use properties::{
    decode_properties, decode_properties_nullable, encode_nullable_property_entries,
    encode_properties, encode_properties_nullable,
};
pub use types::{
    Message, MessageExt, MessageKeyCollectionError, MessageProperties, MessagePropertyError,
    MessagePropertyParseError, MessageQueue, MessageQueueCompareError, MessageSysFlagParseError,
    NAME_VALUE_SEPARATOR, PROPERTY_SEPARATOR, TopicFilterType,
};
