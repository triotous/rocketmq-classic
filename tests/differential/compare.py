#!/usr/bin/env python3
"""Compare normalized JSON results without discarding meaningful array order."""

import json
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
    return len(scenarios)


def self_test():
    assert compare({"a": 1, "b": [1, 2]}, {"b": [1, 2], "a": 1})
    assert not compare({"queue": [1, 2]}, {"queue": [2, 1]})
    assert compare({"queue": [1, 2]}, {"queue": [2, 1]}, frozenset({"$.queue"}))
    assert not compare({"code": 10}, {"code": 11})
    assert compare({"opaque": 1}, {"opaque": 2}, ignored=frozenset({"opaque"}))
    manifest = {"java_client_version": "5.5.1", "server_version": "5.3.4", "scenarios": [
        {"id": "live", "phase": 0, "execution_phase": 19, "profile": "basic", "inputs": {},
         "normalization": [], "live_status": "PENDING_PHASE_19"}]}
    assert validate_manifest(manifest) == 1
    invalid = {**manifest, "scenarios": [{**manifest["scenarios"][0], "live_status": "PASS"}]}
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
