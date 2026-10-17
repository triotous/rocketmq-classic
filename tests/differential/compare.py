#!/usr/bin/env python3
"""Compare normalized JSON results without discarding meaningful array order."""

import json
import re
import sys
from pathlib import Path


def normalize(value, unordered_paths=frozenset(), path="$", ignored=frozenset()):
    if isinstance(value, dict):
        return {
            key: normalize(item, unordered_paths, f"{path}.{key}", ignored)
            for key, item in value.items()
            if key not in ignored
        }
    if isinstance(value, list):
        items = [normalize(item, unordered_paths, f"{path}[]", ignored) for item in value]
        if path in unordered_paths:
            return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
        return items
    return value


def compare(left, right, unordered_paths=frozenset(), ignored=frozenset()):
    left_normal = normalize(left, unordered_paths, ignored=ignored)
    right_normal = normalize(right, unordered_paths, ignored=ignored)
    return left_normal == right_normal


def pending_phase19_source_case_ids():
    pending = set()
    for plan_path in Path("plan/2026/09").glob("*-phase-*.md"):
        if plan_path.name.endswith(".zh-tw.md"):
            continue
        match = re.search(r"phase-(\d+)-", plan_path.name)
        if not match:
            continue
        phase = int(match.group(1))
        if phase > 17:
            continue
        plan = plan_path.read_text()
        if phase <= 11:
            # These early plans record their pending live gates as unchecked
            # validation rows rather than in a dedicated Phase 19 section.
            pending.update(re.findall(r"(?m)^- \[ \] \*\*(P\d+-V\d+)\*\*", plan))
            continue
        handoff = re.search(
            r"(?ms)^### [^\n]*Phase 19 live-test handoff[^\n]*\n(.*?)(?=^## |\Z)",
            plan,
        )
        if handoff:
            pending.update(
                re.findall(r"(?m)^- \[ \] \*\*(P\d+-V\d+)\*\*", handoff.group(1))
            )
    return pending


def validate_manifest(manifest):
    if manifest.get("java_client_version") != "5.5.1" or manifest.get("server_version") != "5.3.4":
        raise ValueError("manifest client/server versions do not match pinned baselines")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("manifest scenarios must be a non-empty array")
    ids = set()
    for scenario in scenarios:
        required = {
            "id",
            "phase",
            "execution_phase",
            "profile",
            "inputs",
            "assertions",
            "client_directions",
            "cleanup",
            "normalization",
            "live_status",
        }
        if not isinstance(scenario, dict) or not required.issubset(scenario):
            raise ValueError("scenario is missing required fields")
        if scenario["id"] in ids:
            raise ValueError(f"duplicate scenario id: {scenario['id']}")
        ids.add(scenario["id"])
        assertions = scenario["assertions"]
        if not isinstance(assertions, list) or not assertions or not all(
            isinstance(assertion, str) and assertion.strip()
            for assertion in assertions
        ):
            raise ValueError(f"scenario has no explicit assertions: {scenario['id']}")
        checks = scenario["inputs"].get("checks", [])
        missing_assertions = set(checks) - set(assertions)
        if missing_assertions:
            missing = ", ".join(sorted(missing_assertions))
            raise ValueError(
                f"scenario assertions omit input checks for {scenario['id']}: {missing}"
            )
        directions = scenario["client_directions"]
        if (
            not isinstance(directions, list)
            or not directions
            or not all(isinstance(direction, str) and direction.strip() for direction in directions)
            or not any("java" in direction.lower() for direction in directions)
            or not any("rust" in direction.lower() for direction in directions)
        ):
            raise ValueError(
                f"scenario must declare Java and Rust client directions: {scenario['id']}"
            )
        cleanup = scenario["cleanup"]
        cleanup_fields = {"topics", "consumer_groups", "offsets_with_groups", "notes"}
        if not isinstance(cleanup, dict) or not cleanup_fields.issubset(cleanup):
            raise ValueError(f"scenario is missing cleanup instructions: {scenario['id']}")
        if (
            not isinstance(cleanup["topics"], list)
            or not isinstance(cleanup["consumer_groups"], list)
            or cleanup["offsets_with_groups"] is not True
            or not isinstance(cleanup["notes"], str)
            or not cleanup["notes"].strip()
        ):
            raise ValueError(f"scenario has invalid cleanup instructions: {scenario['id']}")
        input_resources = set()
        for name in ("topic", "group", "consumer_group", "producer_group"):
            value = scenario["inputs"].get(name)
            if isinstance(value, str):
                input_resources.add(value)
        for name in ("producer_groups", "topics", "groups"):
            value = scenario["inputs"].get(name, [])
            if isinstance(value, list):
                input_resources.update(item for item in value if isinstance(item, str))
        cleanup_resources = set(cleanup["topics"]) | set(cleanup["consumer_groups"])
        if not input_resources.issubset(cleanup_resources):
            missing = ", ".join(sorted(input_resources - cleanup_resources))
            raise ValueError(
                f"scenario cleanup omits input resources for {scenario['id']}: {missing}"
            )
        if not all(
            isinstance(resource, str) and resource.startswith("compat_")
            for resource in cleanup_resources
        ):
            raise ValueError(
                f"scenario cleanup includes a non-isolated resource: {scenario['id']}"
            )
        live_status = scenario["live_status"]
        if live_status not in {"PENDING_PHASE_19", "PASS"}:
            raise ValueError(f"unsupported live status for {scenario['id']}: {live_status}")
        if live_status == "PASS":
            if scenario["execution_phase"] != 19:
                raise ValueError(f"only Phase 19 scenarios may carry live PASS: {scenario['id']}")
            evidence = scenario.get("evidence")
            if not isinstance(evidence, str) or not evidence:
                raise ValueError(f"Phase 19 PASS requires evidence: {scenario['id']}")
            evidence_root = Path("docs/compatibility/evidence/phase-19").resolve()
            evidence_path = Path(evidence).resolve()
            if evidence_root not in evidence_path.parents or not evidence_path.is_dir():
                raise ValueError(f"Phase 19 evidence path is missing or outside the evidence root: {scenario['id']}")
            metadata_path = evidence_path / "metadata.txt"
            result_path = evidence_path / "result.txt"
            if not metadata_path.is_file() or not result_path.is_file():
                raise ValueError(f"Phase 19 evidence is incomplete: {scenario['id']}")
            if "exit_status=0" not in metadata_path.read_text() or f"PASS {scenario['id']}:" not in result_path.read_text():
                raise ValueError(f"Phase 19 evidence does not record a successful run: {scenario['id']}")
            if scenario["id"] == "ROUTE-001-get-existing-topic":
                java_rows = evidence_path / "java-route.tsv"
                rust_rows = evidence_path / "rust-route.tsv"
                if not java_rows.is_file() or not rust_rows.is_file() or java_rows.read_bytes() != rust_rows.read_bytes():
                    raise ValueError(f"Phase 19 route evidence does not match: {scenario['id']}")
            if scenario["id"] == "ROUTE-002-topic-not-exist":
                java_rows = evidence_path / "java-route.tsv"
                rust_rows = evidence_path / "rust-route.tsv"
                expected = f"NO_ROUTE\t{scenario['inputs']['topic']}\n".encode()
                if (
                    not java_rows.is_file()
                    or not rust_rows.is_file()
                    or java_rows.read_bytes() != expected
                    or rust_rows.read_bytes() != expected
                ):
                    raise ValueError(f"Phase 19 missing-route evidence does not match: {scenario['id']}")
            if scenario["id"] == "PROD-001-sync-send":
                topic = scenario["inputs"].get("topic")
                body = scenario["inputs"].get("body")
                if not isinstance(topic, str) or not isinstance(body, str):
                    raise ValueError("synchronous-send evidence requires topic and body inputs")
                expected_body = body.encode().hex()

                def one_tsv_row(name, columns):
                    path = evidence_path / name
                    if not path.is_file():
                        raise ValueError(f"synchronous-send evidence is missing {name}")
                    rows = [line.split("\t") for line in path.read_text().splitlines()]
                    if len(rows) != 1 or len(rows[0]) != columns:
                        raise ValueError(f"synchronous-send evidence has invalid rows in {name}")
                    return rows[0]

                java_send = one_tsv_row("java-send.tsv", 7)
                rust_send = one_tsv_row("rust-send.tsv", 7)
                for language, row in (("Java", java_send), ("Rust", rust_send)):
                    if (
                        row[0] != "SEND"
                        or row[1] != "SEND_OK"
                        or row[2] != topic
                        or row[3] != "0"
                        or not row[4].isdecimal()
                        or not re.fullmatch(r"[0-9a-fA-F]+", row[5])
                        or len(row[5]) not in {32, 56}
                        or not row[6].isdecimal()
                        or int(row[5][-16:], 16) != int(row[6])
                    ):
                        raise ValueError(f"{language} synchronous-send result is invalid")
                if java_send[1:4] != rust_send[1:4]:
                    raise ValueError("Java and Rust synchronous-send status/queue results differ")
                for name, sent in (
                    ("rust-read-java.tsv", java_send),
                    ("java-read-rust.tsv", rust_send),
                ):
                    row = one_tsv_row(name, 5)
                    if (
                        row[0] != "MESSAGE"
                        or row[1] != topic
                        or row[2] != "0"
                        or not row[3].isdecimal()
                        or row[3] != sent[4]
                        or row[4] != expected_body
                    ):
                        raise ValueError(f"cross-language readback does not match {name}")
    phase19_ids = {
        scenario["id"]
        for scenario in scenarios
        if scenario["execution_phase"] == 19
    }
    phase19_source_ids = {
        scenario.get("source_case_id", scenario["id"])
        for scenario in scenarios
        if scenario["execution_phase"] == 19
    }
    covered_source_ids = phase19_ids | phase19_source_ids
    missing_source_ids = {
        source_id
        for source_id in pending_phase19_source_case_ids()
        if source_id not in covered_source_ids
        and not any(case_id.startswith(f"{source_id}-") for case_id in phase19_ids)
    }
    if missing_source_ids:
        missing = ", ".join(sorted(missing_source_ids))
        raise ValueError(f"manifest missing Phase 19 source case IDs: {missing}")
    for source_id in pending_phase19_source_case_ids():
        source_phase = int(source_id.split("-")[0][1:])
        source_cases = [
            scenario
            for scenario in scenarios
            if scenario.get("source_case_id", scenario["id"]) == source_id
            or scenario["id"] == source_id
            or scenario["id"].startswith(f"{source_id}-")
        ]
        misplaced = [
            scenario["id"]
            for scenario in source_cases
            if scenario["phase"] != source_phase or scenario["execution_phase"] != 19
        ]
        if misplaced:
            cases = ", ".join(sorted(misplaced))
            raise ValueError(f"Phase 19 source case {source_id} is assigned to the wrong phase: {cases}")
    return len(scenarios)


def self_test():
    assert compare({"a": 1, "b": [1, 2]}, {"b": [1, 2], "a": 1})
    assert not compare({"queue": [1, 2]}, {"queue": [2, 1]})
    assert compare({"queue": [1, 2]}, {"queue": [2, 1]}, frozenset({"$.queue"}))
    assert not compare({"code": 10}, {"code": 11})
    assert compare({"opaque": 1}, {"opaque": 2}, ignored=frozenset({"opaque"}))
    manifest = {
        "java_client_version": "5.5.1",
        "server_version": "5.3.4",
        "scenarios": [
            {
                "id": source_id,
                "phase": int(source_id.split("-")[0][1:]),
                "execution_phase": 19,
                "profile": "basic",
                "inputs": {},
                "assertions": ["java-rust-result-equality"],
                "client_directions": [
                    "java-5.5.1-client->server-5.3.4",
                    "rust-client->server-5.3.4",
                ],
                "cleanup": {
                    "topics": [],
                    "consumer_groups": [],
                    "offsets_with_groups": True,
                    "notes": "Delete only resources owned by this scenario.",
                },
                "normalization": [],
                "live_status": "PENDING_PHASE_19",
            }
            for source_id in sorted(pending_phase19_source_case_ids())
        ],
    }
    assert validate_manifest(manifest) == len(pending_phase19_source_case_ids())
    invalid = {
        **manifest,
        "scenarios": [{**manifest["scenarios"][0], "live_status": "PASS"}],
    }
    try:
        validate_manifest(invalid)
        raise AssertionError("pending scenario incorrectly accepted as PASS")
    except ValueError as error:
        assert "requires evidence" in str(error)
    invalid_evidence_path = {
        **manifest,
        "scenarios": [
            {
                **manifest["scenarios"][0],
                "live_status": "PASS",
                "evidence": "../../README.md",
            }
        ],
    }
    try:
        validate_manifest(invalid_evidence_path)
        raise AssertionError("manifest accepted evidence outside the Phase 19 evidence directory")
    except ValueError as error:
        assert "evidence path" in str(error)
    checked_in = json.loads(Path("tests/differential/scenarios.yaml").read_text())
    validate_manifest(checked_in)
    phase6 = [case for case in checked_in["scenarios"] if case["id"] == "P6-V04"]
    assert len(phase6) == 1, "Phase 6 live handoff case must retain its original ID"
    assert phase6[0]["phase"] == 6 and phase6[0]["execution_phase"] == 19
    assert phase6[0]["live_status"] == "PENDING_PHASE_19"
    assert phase6[0]["inputs"]["checks"] == [
        "heartbeat-subscription-change", "unregister", "remaining-shared-user-functional"]
    missing_handoff = {
        **checked_in,
        "scenarios": [case for case in checked_in["scenarios"] if case["id"] != "P11-V04"],
    }
    try:
        validate_manifest(missing_handoff)
        raise AssertionError("manifest accepted an omitted Phase 11 live handoff")
    except ValueError as error:
        assert "P11-V04" in str(error)
    missing_late_handoff = {
        **checked_in,
        "scenarios": [case for case in checked_in["scenarios"] if case["id"] != "P17-V02"],
    }
    try:
        validate_manifest(missing_late_handoff)
        raise AssertionError("manifest accepted an omitted Phase 17 live handoff")
    except ValueError as error:
        assert "P17-V02" in str(error)
    missing_assertion = {
        **checked_in,
        "scenarios": [
            {**case, "assertions": []} if case["id"] == "P17-V02" else case
            for case in checked_in["scenarios"]
        ],
    }
    try:
        validate_manifest(missing_assertion)
        raise AssertionError("manifest accepted a handoff without assertions")
    except ValueError as error:
        assert "P17-V02" in str(error)
    missing_rust_direction = {
        **checked_in,
        "scenarios": [
            {
                **case,
                "client_directions": ["java-5.5.1 client -> server-5.3.4"],
            }
            if case["id"] == "P17-V02"
            else case
            for case in checked_in["scenarios"]
        ],
    }
    try:
        validate_manifest(missing_rust_direction)
        raise AssertionError("manifest accepted a handoff without a Rust direction")
    except ValueError as error:
        assert "P17-V02" in str(error)
    wrong_source_phase = {
        **checked_in,
        "scenarios": [
            {**case, "phase": 10} if case["id"] == "P11-V04" else case
            for case in checked_in["scenarios"]
        ],
    }
    try:
        validate_manifest(wrong_source_phase)
        raise AssertionError("manifest accepted a Phase 11 case assigned to Phase 10")
    except ValueError as error:
        assert "P11-V04" in str(error)
    print("Differential comparator and manifest self-test passed")


def main(argv):
    if argv == ["--self-test"]:
        self_test()
        return 0
    if argv == ["--validate-manifest"]:
        count = validate_manifest(json.loads(Path("tests/differential/scenarios.yaml").read_text()))
        print(f"Validated {count} scenarios")
        return 0
    if len(argv) != 2:
        raise SystemExit("usage: compare.py --self-test | --validate-manifest | LEFT.json RIGHT.json")
    left, right = (json.loads(Path(name).read_text()) for name in argv)
    if compare(left, right):
        print("match")
        return 0
    print("results differ", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
