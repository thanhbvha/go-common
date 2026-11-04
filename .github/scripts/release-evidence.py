#!/usr/bin/env python3
"""Capture local CI evidence. Digests detect corruption, not authenticity.

Only artifacts downloaded from the trusted release-evidence workflow run are
release authority; a locally constructed manifest is never an attestation.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

GATES = ("quality", "security", "fuzz", "databases", "messaging", "mongo-replica",
         "mongo-secure", "brokers", "faults", "soak")
REQUIRED_TESTS = {
    "databases": ("TestPostgresProduction", "TestMySQLContract"),
    "messaging": ("TestPostgresContract", "TestPostgresEventBus", "TestCommitUnknownReconciliation",
                  "TestMongoDBMessagingContract", "TestMongoDBMessagingEventBus", "TestMongoDBMessagingCommitResponseLoss",
                  "TestMongoDBMessagingAmbiguousClaim", "TestMongoDBMessagingTransactionRetries",
                  "TestMySQLMessagingContract", "TestMySQLMessagingEventBus", "TestMySQLMessagingCommitUnknown",
                  "TestMySQLMessagingSnapshotRegression", "TestMySQLMessagingLeaseWaitRegression",
                  "TestMessagingExample", "TestMessagingExampleMongoDB", "TestMessagingExampleMySQL"),
    "mongo-replica": ("TestMongoDBProductionIntegration", "TestMongoDBMessagingContract",
                      "TestMongoDBMessagingCommitResponseLoss", "TestMongoDBMessagingPrimaryStepdown",
                      "TestMongoDBMessagingAmbiguousClaim", "TestMongoDBMessagingTransactionRetries",
                      "TestMessagingExampleMongoDB", "TestMessagingExampleMongoDBStandaloneRejected"),
    "mongo-secure": ("TestMongoDBProductionIntegration", "TestMongoDBMessagingContract",
                     "TestMongoDBMessagingCommitResponseLoss", "TestMongoDBMessagingAmbiguousClaim",
                     "TestMongoDBMessagingTransactionRetries", "TestMessagingExampleMongoDB"),
    "brokers": ("TestKafkaSettlementBrokerFaultWindows", "TestRedisSentinelFailoverIntegration"),
    "faults": ("TestPublishResponseLossLeavesAcceptedSource",
               "TestTransferResponseLossIsIdempotent",
               "TestAckResponseLossDoesNotRestoreOrStealSource",
               "TestNATSAckWaitExpiryRedeliversWithAttempt",
               "TestNATSMaxAckPendingSaturatesAndResumes",
               "TestNATSDisconnectReconnectResumesConsumer",
               "TestReferenceRedisRepeatedOutageRecovery"),
    "soak": ("TestProductionSoak",),
}


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def scan(path):
    """Reject empty/truncated/wrong-mode streams before convert can accept them."""
    raw = path.read_text(encoding="utf-8")
    decoder = json.JSONDecoder()
    messages = []
    while raw.strip():
        message, end = decoder.raw_decode(raw.lstrip())
        if not isinstance(message, dict):
            raise ValueError("invalid scan message")
        messages.append(message)
        raw = raw.lstrip()[end:]
    configs = [m["config"] for m in messages if "config" in m]
    if len(configs) != 1 or configs[0].get("scan_mode") != "source" or configs[0].get("scan_level") != "symbol":
        raise ValueError("source/symbol scan configuration required")
    for message in messages:
        if "finding" in message:
            finding = message["finding"]
            if not finding.get("osv") or not finding.get("trace"):
                raise ValueError("malformed vulnerability finding")
            if any(frame.get("function") for frame in finding["trace"]):
                raise ValueError("reachable vulnerability: " + finding["osv"])


def verify(root, revision):
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("full Git revision required")
    for gate in GATES:
        record = json.loads((root / (gate + ".json")).read_text())
        log = root / (gate + ".txt")
        if record.get("revision") != revision or record.get("gate") != gate:
            raise ValueError("mismatched revision/gate: " + gate)
        if record.get("exit_code") != 0 or record.get("sha256") != digest(log):
            raise ValueError("failed gate or corrupt log: " + gate)
        if not record.get("command"):
            raise ValueError("missing command: " + gate)
        if gate == "security":
            report = root.parent / "govulncheck.json"
            if record.get("scan_sha256") != digest(report):
                raise ValueError("missing or corrupt vulnerability scan")
            scan(report)
        # Verbose integration tests must not silently downgrade a drill to SKIP.
        if gate in ("databases", "messaging", "mongo-replica", "mongo-secure", "brokers", "faults", "soak"):
            text = log.read_text(encoding="utf-8")
            if re.search(r"--- SKIP:|\"Action\"\s*:\s*\"skip\"|^\s*SKIP\b", text, re.M):
                raise ValueError("skipped required test: " + gate)
            if not re.search(r"^ok\s+github.com/thanhbvha/go-common", text, re.M):
                raise ValueError("missing Go test completion: " + gate)
            for test in REQUIRED_TESTS.get(gate, ()):
                passes = len(re.findall(r"^--- PASS: " + re.escape(test) + r" \(", text, re.M))
                if passes < (5 if gate == "faults" else 1):
                    raise ValueError("missing required test completion: " + test)
        if gate == "soak" and not re.search(r"duration=1h0m0s requests=\d+ failures=0\b", text):
            raise ValueError("missing successful 60-minute soak: " + gate)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "verify", "scan"))
    parser.add_argument("target")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.action == "scan":
        scan(Path(args.target))
        return 0
    root = Path("artifacts/release")
    if args.action == "verify":
        verify(root, args.target)
        print("All release gates verified on " + args.target)
        return 0
    if args.target not in GATES:
        raise ValueError("unknown gate")
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise ValueError("command required")
    root.mkdir(parents=True, exist_ok=True)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    # Tracked changes make the evidence irreproducible for this revision.
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--"], check=True, stdout=subprocess.DEVNULL)
    log = root / (args.target + ".txt")
    with log.open("wb") as output:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in process.stdout:
            output.write(line)
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
        code = process.wait()
    record = dict(gate=args.target, revision=revision, exit_code=code,
                  sha256=digest(log), command=command,
                  run_id=os.getenv("GITHUB_RUN_ID"), attempt=os.getenv("GITHUB_RUN_ATTEMPT"))
    report = root.parent / "govulncheck.json"
    if args.target == "security" and report.is_file():
        record["scan_sha256"] = digest(report)
    (root / (args.target + ".json")).write_text(json.dumps(record, indent=2) + "\n")
    return code if code >= 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print("Evidence rejected: " + str(error), file=sys.stderr)
        sys.exit(1)
