use rocketmq_classic::{
    ConsumerServiceState, DEFAULT_CONSUMER_GROUP, DEFAULT_PRODUCER_GROUP, EndTransactionContext,
    EndTransactionHook, Message, MessageBatch, NullablePublishInfoQueueFilters,
    OrderlyListenerFuture, PopResult, PopStatus, Producer, ProducerConfig,
    ProducerPublishQueueList, ProducerQueueIndex, ProducerServiceState, PublishInfoQueueFilter,
    QueryResult, SharedMessageProperties, TopicPublishInfo, TopicPublishInfoEntryHandle,
    TopicPublishInfoEntrySetView, TopicPublishInfoKeySetView, TopicPublishInfoTable,
    TopicPublishInfoTableError, TopicPublishInfoValuesView, TopicRouteData, TopicRouteDataHandle,
};

fn assert_public_type<T: ?Sized>() {}

#[test]
fn phase_owned_public_types_are_available_from_the_facade_crate() {
    assert_eq!(DEFAULT_CONSUMER_GROUP, "DEFAULT_CONSUMER");
    assert_eq!(DEFAULT_PRODUCER_GROUP, "DEFAULT_PRODUCER");

    let producer = Producer::new(ProducerConfig::new("facade-api-probe").unwrap());
    let _: Option<ProducerServiceState> = producer.service_state();
    let _: TopicPublishInfoTable = producer.get_topic_publish_info_table();
    let batch =
        MessageBatch::generate_from_list(vec![Message::new("orders", b"body".to_vec())]).unwrap();
    assert_eq!(batch.topic(), "orders");
    assert!(!batch.encode().unwrap().is_empty());
    assert_public_type::<MessageBatch>();

    let shared_properties =
        SharedMessageProperties::from_properties(std::collections::BTreeMap::new());
    let mut message = Message::new("shared-properties", b"body".to_vec());
    message.set_properties_shared(Some(shared_properties.clone()));
    shared_properties.insert(Some("key".to_owned()), Some("value".to_owned()));
    assert_eq!(message.get_property("key").as_deref(), Some("value"));
    assert_public_type::<SharedMessageProperties>();

    assert_public_type::<ConsumerServiceState>();
    assert_public_type::<OrderlyListenerFuture>();
    assert_public_type::<PopResult>();
    assert_public_type::<PopStatus>();
    assert_public_type::<EndTransactionContext<'static>>();
    assert_public_type::<dyn EndTransactionHook>();
    assert_public_type::<NullablePublishInfoQueueFilters<'static>>();
    assert_public_type::<ProducerPublishQueueList>();
    assert_public_type::<ProducerQueueIndex>();
    assert_public_type::<PublishInfoQueueFilter<'static>>();
    assert_public_type::<QueryResult>();
    assert_public_type::<TopicPublishInfo>();
    assert_public_type::<TopicPublishInfoEntryHandle>();
    assert_public_type::<TopicPublishInfoEntrySetView>();
    assert_public_type::<TopicPublishInfoKeySetView>();
    assert_public_type::<TopicPublishInfoTableError>();
    assert_public_type::<TopicPublishInfoValuesView>();
    assert_public_type::<TopicRouteData>();
    assert_public_type::<TopicRouteDataHandle>();
}
