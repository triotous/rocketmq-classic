use std::{
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::BytesMut;
use rocketmq_classic_common::{
    compression::decompress_limited,
    message::{DecodeLimits, DecodeOptions, decode_many, decode_one, decode_properties},
    sysflag::message::CompressionType,
};
use rocketmq_classic_protocol::{
    codes::SerializeType,
    header::{CommandHeader, ProofHeader},
};
use rocketmq_classic_remoting::{
    codec::{frame, json_header, rocketmq_header},
    command::RemotingCommand,
};

const MAX_SEED_BYTES: usize = 1024 * 1024;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_DECOMPRESSED_BYTES: usize = 4096;
const MAX_FRAME_SEEDS: usize = 256;
const MUTATIONS_PER_SEED: usize = 16;

#[derive(Debug, PartialEq, Eq)]
pub struct SmokeSummary {
    pub frame_seeds: usize,
    pub message_seeds: usize,
    pub mutated_inputs: usize,
    pub frame_decodes: usize,
    pub message_decodes: usize,
}

pub fn run_fixed_seed_mutations() -> SmokeSummary {
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures");
    let frame_seeds = read_frame_seeds(&fixture_root.join("remoting"));
    let message_seeds = read_message_seeds(&fixture_root.join("messages/records.json"));
    assert!(
        !frame_seeds.is_empty(),
        "remoting corpus has no frame seeds"
    );
    assert!(
        !message_seeds.is_empty(),
        "message corpus has no record seeds"
    );

    let mut frame_decodes = 0;
    let mut message_decodes = 0;
    let mut mutated_inputs = 0;
    for (seed_index, seed) in frame_seeds.iter().chain(&message_seeds).enumerate() {
        for mutation_index in 0..MUTATIONS_PER_SEED {
            let input = mutate(seed, seed_index, mutation_index);
            assert!(input.len() <= MAX_SEED_BYTES);
            catch_unwind(AssertUnwindSafe(|| {
                let (frames, messages) = exercise_trust_boundaries(&input);
                frame_decodes += frames;
                message_decodes += messages;
            }))
            .unwrap_or_else(|_| {
                panic!("decoder panicked for fixture seed {seed_index}, mutation {mutation_index}")
            });
            mutated_inputs += 1;
        }
    }

    unknown_request_codes_round_trip();
    assert!(frame_decodes > 0, "smoke corpus decoded no Remoting frames");
    assert!(
        message_decodes > 0,
        "smoke corpus decoded no message records"
    );
    SmokeSummary {
        frame_seeds: frame_seeds.len(),
        message_seeds: message_seeds.len(),
        mutated_inputs,
        frame_decodes,
        message_decodes,
    }
}

fn read_frame_seeds(directory: &Path) -> Vec<Vec<u8>> {
    let mut paths: Vec<_> = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", directory.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "bin"))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .take(MAX_FRAME_SEEDS)
        .filter_map(|path| {
            let bytes = fs::read(&path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            (bytes.len() <= MAX_SEED_BYTES).then_some(bytes)
        })
        .collect()
}

fn read_message_seeds(path: &Path) -> Vec<Vec<u8>> {
    let fixture: serde_json::Value = serde_json::from_slice(
        &fs::read(path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display())),
    )
    .expect("message records fixture is valid JSON");
    fixture["cases"]
        .as_array()
        .expect("message records fixture has cases")
        .iter()
        .map(|case| {
            STANDARD
                .decode(
                    case["recordBase64"]
                        .as_str()
                        .expect("recordBase64 is a string"),
                )
                .expect("message record fixture uses valid Base64")
        })
        .collect()
}

fn mutate(seed: &[u8], seed_index: usize, mutation_index: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64
        ^ (seed_index as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ (mutation_index as u64).wrapping_mul(0x94d0_49bb_1331_11eb);
    let mut bytes = seed.to_vec();
    match mutation_index % 5 {
        0 if !bytes.is_empty() => {
            let index = next_u32(&mut state) as usize % bytes.len();
            bytes[index] ^= (next_u32(&mut state) as u8) | 1;
        }
        1 => {
            let length = if bytes.is_empty() {
                0
            } else {
                next_u32(&mut state) as usize % (bytes.len() + 1)
            };
            bytes.truncate(length);
        }
        2 if bytes.len() < MAX_SEED_BYTES => {
            let index = next_u32(&mut state) as usize % (bytes.len() + 1);
            bytes.insert(index, next_u32(&mut state) as u8);
        }
        3 if bytes.len() >= 4 => {
            let invalid_prefix = [0_u32, 1, 3, i32::MAX as u32 + 1, u32::MAX];
            let value = invalid_prefix[next_u32(&mut state) as usize % invalid_prefix.len()];
            bytes[..4].copy_from_slice(&value.to_be_bytes());
        }
        _ if !bytes.is_empty() => {
            for _ in 0..3 {
                let index = next_u32(&mut state) as usize % bytes.len();
                bytes[index] ^= (next_u32(&mut state) as u8) | 1;
            }
        }
        _ => {}
    }
    bytes
}

fn next_u32(state: &mut u64) -> u32 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    (*state >> 16) as u32
}

fn exercise_trust_boundaries(input: &[u8]) -> (usize, usize) {
    let bounded = &input[..input.len().min(MAX_SEED_BYTES)];
    let text = String::from_utf8_lossy(&bounded[..bounded.len().min(MAX_MESSAGE_BYTES)]);
    let _properties = decode_properties(&text);
    let _json_header = json_header::decode(&bounded[..bounded.len().min(MAX_FRAME_BYTES)]);
    let _private_header = rocketmq_header::decode(&bounded[..bounded.len().min(MAX_FRAME_BYTES)]);

    let mut frame_input = BytesMut::from(bounded);
    let mut frame_decodes = 0;
    for _ in 0..4 {
        match frame::decode_next(&mut frame_input, MAX_FRAME_BYTES) {
            Ok(Some(command)) => {
                frame_decodes += 1;
                assert!(
                    command
                        .body
                        .as_ref()
                        .is_none_or(|body| body.len() <= MAX_FRAME_BYTES)
                );
                if let Some(fields) = command.ext_fields.as_ref() {
                    let _typed_header = ProofHeader::decode_fields(fields);
                }
            }
            Ok(None) | Err(_) => break,
        }
    }

    let limits = DecodeLimits {
        max_record_bytes: MAX_MESSAGE_BYTES,
        max_decompressed_body_bytes: MAX_DECOMPRESSED_BYTES,
        max_total_decoded_bytes: MAX_MESSAGE_BYTES,
    };
    let options = DecodeOptions::default();
    let mut message_decodes = 0;
    if let Ok((message, consumed)) = decode_one(bounded, options, limits) {
        message_decodes += 1;
        assert!(consumed <= bounded.len());
        assert!(message.message().body().len() <= MAX_DECOMPRESSED_BYTES);
    }
    if let Ok(messages) = decode_many(bounded, options, limits) {
        message_decodes += messages.len();
        assert!(
            messages
                .iter()
                .all(|message| { message.message().body().len() <= MAX_DECOMPRESSED_BYTES })
        );
    }

    for compression in [
        CompressionType::Zlib,
        CompressionType::Lz4,
        CompressionType::Zstd,
    ] {
        let _ = decompress_limited(compression, bounded, MAX_DECOMPRESSED_BYTES);
    }
    (frame_decodes, message_decodes)
}

fn unknown_request_codes_round_trip() {
    for (code, serialization) in [
        (987_654, SerializeType::Json),
        (12_345, SerializeType::RocketMq),
    ] {
        let command = RemotingCommand {
            code,
            serialization,
            ..RemotingCommand::default()
        };
        let encoded = frame::encode(&command, MAX_FRAME_BYTES).expect("unknown-code frame encodes");
        let mut input = BytesMut::from(encoded.as_slice());
        let decoded = frame::decode_next(&mut input, MAX_FRAME_BYTES)
            .expect("unknown-code frame decodes")
            .expect("unknown-code frame is complete");
        assert_eq!(decoded.code, code);
        assert!(input.is_empty());
    }
}
