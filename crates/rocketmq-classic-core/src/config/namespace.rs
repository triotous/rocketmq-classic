//! Private implementation of the pinned NamespaceUtil/endpoint helpers.

fn system(resource: &str) -> bool {
    resource.starts_with("rmq_sys_")
        || resource.starts_with("CID_RMQ_SYS_")
        || matches!(
            resource,
            "TBW102"
                | "SCHEDULE_TOPIC_XXXX"
                | "BenchmarkTest"
                | "RMQ_SYS_TRANS_HALF_TOPIC"
                | "RMQ_SYS_TRACE_TOPIC"
                | "RMQ_SYS_TRANS_OP_HALF_TOPIC"
                | "TRANS_CHECK_MAX_TIME_TOPIC"
                | "SELF_TEST_TOPIC"
                | "OFFSET_MOVED_EVENT"
                | "CHECKPOINT_TOPIC"
                | "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC"
                | "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC"
        )
}

fn prefix(resource: &str) -> (&str, &str) {
    for prefix in ["%RETRY%", "%DLQ%"] {
        if let Some(rest) = resource.strip_prefix(prefix) {
            return (prefix, rest);
        }
    }
    ("", resource)
}

pub(super) fn wrap(namespace: Option<&str>, resource: Option<&str>) -> Option<String> {
    let resource = resource?;
    let Some(namespace) = namespace.filter(|value| !value.is_empty()) else {
        return Some(resource.into());
    };
    let (prefix, rest) = prefix(resource);
    let marker = format!("{namespace}%");
    Some(
        if resource.is_empty() || system(resource) || rest.starts_with(&marker) {
            resource.into()
        } else {
            format!("{prefix}{marker}{rest}")
        },
    )
}

pub(crate) fn unwrap(namespace: Option<&str>, resource: Option<&str>) -> Option<String> {
    let resource = resource?;
    let Some(namespace) = namespace.filter(|value| !value.is_empty()) else {
        return Some(resource.into());
    };
    let (prefix, rest) = prefix(resource);
    if !system(resource) && rest.starts_with(&format!("{namespace}%")) {
        // Java's no-argument remover removes through the first '%' only.
        if let Some((before, after)) = rest.split_once('%') {
            if !before.is_empty() {
                return Some(format!("{prefix}{after}"));
            }
        }
    }
    Some(resource.into())
}

fn line_terminator(value: char) -> bool {
    matches!(value, '\n' | '\r' | '\u{0085}' | '\u{2028}' | '\u{2029}')
}

pub(super) fn address(raw: &str) -> String {
    let trimmed = raw.trim_matches(|value: char| value <= '\u{0020}');
    if trimmed.starts_with("http://") && !trimmed.chars().any(line_terminator) {
        // Validation trims, but Java substring extraction uses the raw string.
        raw.rsplit('/').next().unwrap_or_default().into()
    } else {
        raw.into()
    }
}

pub(super) fn instance_endpoint(raw: &str) -> bool {
    let word = |value: &str| {
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    };
    let host = match raw.split_once("://") {
        Some((scheme, host)) if word(scheme) => host,
        _ => raw,
    };
    let Some((instance, _)) = host.split_once('.') else {
        return false;
    };
    let Some(suffix) = instance.strip_prefix("MQ_INST_") else {
        return false;
    };
    word(suffix)
        && suffix
            .bytes()
            .enumerate()
            .any(|(index, byte)| byte == b'_' && index > 0 && index + 1 < suffix.len())
        && !raw.chars().any(line_terminator)
}

pub(super) fn instance(raw: &str) -> Result<&str, super::ConfigError> {
    let start = raw.rfind('/').map_or(0, |index| index + 1);
    let end = raw
        .find('.')
        .ok_or(super::ConfigError::InvalidInstanceEndpoint)?;
    raw.get(start..end)
        .ok_or(super::ConfigError::InvalidInstanceEndpoint)
}
