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
        match = re.search(r"phase-(\d+)-", plan_path.name)
        if not match or int(match.group(1)) > 11:
            continue
        pending.update(re.findall(r"(?m)^- \[ \] \*\*(P\d+-V\d+)\*\*", plan_path.read_text()))
    return pending


def validate_manifest(manifest):
    if manifest.get("java_client_version") != "5.5.1" or manifest.get("server_version") != "5.3.4":
        raise ValueError("manifest client/server versions do not match pinned baselines")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("manifest scenarios must be a non-empty array")
    ids = set()
    for scenario in scenarios:
        required = {"id", "phase", "execution_phase", "profile", "inputs", "normalization", "live_status"}
        if not isinstance(scenario, dict) or not required.issubset(scenario):
            raise ValueError("scenario is missing required fields")
        if scenario["id"] in ids:
            raise ValueError(f"duplicate scenario id: {scenario['id']}")
        ids.add(scenario["id"])
        if scenario["execution_phase"] == 19 and scenario["live_status"] == "PASS":
            raise ValueError(f"unexecuted Phase 19 scenario cannot be PASS: {scenario['id']}")
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
    except ValueError:
        pass
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
