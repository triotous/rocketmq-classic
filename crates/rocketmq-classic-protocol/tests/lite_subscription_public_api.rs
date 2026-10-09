use std::collections::HashSet;

use rocketmq_classic_protocol::body::{
    LiteSubscription, LiteSubscriptionAction, LiteSubscriptionCtlRequestBody, LiteSubscriptionDTO,
    OffsetOption, OffsetOptionType,
};

#[test]
fn lite_subscription_java_accessors_are_public_and_chain_like_java() {
    let mut subscription = LiteSubscription::new();
    subscription
        .set_group(Some("workers".into()))
        .set_topic(Some("orders".into()))
        .set_lite_topic_set(["lite-a"]);
    assert_eq!(subscription.group(), Some("workers"));
    assert_eq!(subscription.topic(), Some("orders"));
    assert!(subscription.add_lite_topic("lite-b"));
    assert!(subscription.remove_lite_topic("lite-b"));

    let mut dto = LiteSubscriptionDTO::default();
    dto.set_action(Some(LiteSubscriptionAction::CompleteAdd))
        .set_client_id(Some("client".into()))
        .set_group(Some("workers".into()))
        .set_topic(Some("orders".into()))
        .set_lite_topic_set(Some(std::collections::BTreeSet::from([Some(
            "lite-a".into(),
        )])))
        .set_version(9);
    dto.set_offset_option(Some(OffsetOption::new(
        Some(OffsetOptionType::Policy),
        OffsetOption::POLICY_MIN_VALUE,
    )));
    assert_eq!(dto.action(), Some(LiteSubscriptionAction::CompleteAdd));
    assert_eq!(dto.client_id(), Some("client"));
    assert_eq!(dto.version(), 9);

    let mut control = LiteSubscriptionCtlRequestBody::default();
    control.set_subscription_set(Some(vec![Some(dto)]));
    assert_eq!(control.subscription_set().unwrap().len(), 1);

    let equal_options = HashSet::from([
        OffsetOption::new(Some(OffsetOptionType::TailN), 23),
        OffsetOption::new(Some(OffsetOptionType::TailN), 23),
    ]);
    assert_eq!(equal_options.len(), 1);
}
