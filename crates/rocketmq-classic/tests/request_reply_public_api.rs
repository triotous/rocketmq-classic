use rocketmq_classic::{Message, ReplyMessageError, RequestCallback, create_reply_message};

#[test]
fn request_reply_api_is_available_from_the_facade_crate() {
    let _: RequestCallback = Box::new(|_| {});

    let mut request = Message::new("orders", b"request".to_vec());
    request
        .set_system_property("CLUSTER", "cluster-east")
        .unwrap();
    request
        .set_system_property("CORRELATION_ID", "correlation-01")
        .unwrap();
    let reply = create_reply_message(Some(&request), b"reply".to_vec()).unwrap();

    assert_eq!(reply.topic(), "cluster-east_REPLY_TOPIC");
    assert_eq!(
        reply.get_property("CORRELATION_ID").as_deref(),
        Some("correlation-01")
    );
    assert!(matches!(
        create_reply_message(None, b"reply".to_vec()),
        Err(ReplyMessageError::NullRequest)
    ));
}
