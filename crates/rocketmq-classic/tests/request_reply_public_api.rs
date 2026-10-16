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

#[test]
fn pinned_java_request_reply_callback_surface_is_inventoried() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/producer/request-reply.json"
    ))
    .unwrap();

    assert_eq!(
        fixture["publicRequestCallbackMethods"],
        serde_json::json!([
            "void onException(java.lang.Throwable)",
            "void onSuccess(org.apache.rocketmq.common.message.Message)"
        ])
    );
    assert_eq!(
        fixture["publicRequestFutureHolderMethods"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        fixture["publicRequestResponseFutureMethods"]
            .as_array()
            .unwrap()
            .len(),
        18
    );
}

#[test]
fn pinned_java_request_future_mutation_and_holder_table_behavior_is_recorded() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/producer/request-reply.json"
    ))
    .unwrap();

    let future = &fixture["requestResponseFutureBehavior"];
    assert_eq!(future["correlationId"], "future-fixture-01");
    assert_eq!(future["initialLatchCount"], 1);
    assert_eq!(future["replacementLatchInitialCount"], 2);
    assert_eq!(future["putResponseMessageCount"], 1);
    assert_eq!(future["directLatchCountDownCount"], 0);
    assert_eq!(future["negativeTimeoutIsExpired"], true);
    assert_eq!(future["responseSetterCanClear"], true);
    assert_eq!(future["sendRequestOkSetter"], false);
    assert_eq!(future["causeSameInstance"], true);
    assert_eq!(
        future["callbackEvents"],
        serde_json::json!([
            "exception:java.lang.IllegalStateException:fixture-cause",
            "success:future-response"
        ])
    );
    assert_eq!(future["nullCallbackExecution"], "completed");

    let holder = &fixture["requestFutureHolderBehavior"];
    assert_eq!(holder["singletonInstance"], true);
    assert_eq!(holder["tableSameInstance"], true);
    assert_eq!(holder["insertVisibleThroughSecondView"], true);
    assert_eq!(holder["removedSameInstance"], true);
    assert_eq!(holder["sizeAfterInsert"], 1);
    assert_eq!(holder["sizeAfterRemove"], 0);
}
