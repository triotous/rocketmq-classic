#!/usr/bin/env python3
"""Check workspace package metadata and the phase-defined crate direction."""

import json
import subprocess

metadata = json.loads(
    subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"], text=True
    )
)
packages = {package["name"]: package for package in metadata["packages"]}
expected = {
    "rocketmq-classic-common",
    "rocketmq-classic-consumer",
    "rocketmq-classic-core",
    "rocketmq-classic",
    "rocketmq-classic-protocol",
    "rocketmq-classic-producer",
    "rocketmq-classic-remoting",
}
assert set(packages) == expected, f"unexpected workspace packages: {set(packages)}"

common_deps = {dependency["name"] for dependency in packages["rocketmq-classic-common"]["dependencies"]}
protocol_deps = {dependency["name"] for dependency in packages["rocketmq-classic-protocol"]["dependencies"]}
remoting_deps = {dependency["name"] for dependency in packages["rocketmq-classic-remoting"]["dependencies"]}
assert "rocketmq-classic-protocol" not in common_deps, "common must not depend on protocol"
assert "rocketmq-classic-common" in protocol_deps, "protocol must depend on common"
assert "rocketmq-classic-remoting" not in protocol_deps, "protocol must not depend on remoting"
assert "rocketmq-classic-protocol" in remoting_deps, "remoting must depend on protocol"
assert all(package["publish"] == [] for package in packages.values())
print("workspace package metadata and dependency direction PASS")
