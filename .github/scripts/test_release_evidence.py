import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("evidence", Path(__file__).with_name("release-evidence.py"))
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)


class EvidenceTests(unittest.TestCase):
    def test_scan(self):
        config = {"config": {"scan_mode": "source", "scan_level": "symbol"}}
        module = {"finding": {"osv": "GO-fixture", "trace": [{"module": "fixture"}]}}
        reachable = {"finding": {"osv": "GO-fixture", "trace": [{"function": "Unsafe"}]}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "scan.json"
            for messages, valid in [([config], True), ([config, module], True),
                                    ([config, reachable], False), ([], False),
                                    ([config, config], False), ([module], False),
                                    ([{"config": {"scan_mode": "binary"}}], False),
                                    ([config, {"finding": {}}], False)]:
                with self.subTest(messages=messages):
                    path.write_text("\n".join(json.dumps(m) for m in messages))
                    if valid:
                        evidence.scan(path)
                    else:
                        with self.assertRaises(ValueError):
                            evidence.scan(path)
            path.write_text(json.dumps(config) + '{"finding":')
            with self.assertRaises(ValueError):
                evidence.scan(path)

    def fixture(self, root):
        (root.parent / "govulncheck.json").write_text(json.dumps(
            {"config": {"scan_mode": "source", "scan_level": "symbol"}}))
        for gate in evidence.GATES:
            log = root / (gate + ".txt")
            text = "ok  github.com/thanhbvha/go-common/v2/fixture 1.0s\n"
            for test in evidence.REQUIRED_TESTS.get(gate, ()):
                text += ("--- PASS: " + test + " (1.00s)\n") * (5 if gate == "faults" else 1)
            if gate == "soak":
                text += "duration=1h0m0s requests=720000 failures=0\n"
            log.write_text(text)
            self.record(root, gate)

    def record(self, root, gate, **changes):
        record = dict(gate=gate, revision="a" * 40, exit_code=0,
                      sha256=evidence.digest(root / (gate + ".txt")), command=["fixture"])
        record.update(changes)
        if gate == "security":
            record["scan_sha256"] = evidence.digest(root.parent / "govulncheck.json")
        (root / (gate + ".json")).write_text(json.dumps(record))

    def test_complete_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "release"
            root.mkdir()
            self.fixture(root)
            evidence.verify(root, "a" * 40)

    def test_reject_invalid_evidence(self):
        for fault in ("missing", "revision", "failure", "tamper", "skip", "empty", "short-soak", "missing-test", "scan-tamper"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "release"
                root.mkdir()
                self.fixture(root)
                if fault == "missing":
                    (root / "brokers.json").unlink()
                elif fault == "revision":
                    self.record(root, "brokers", revision="b" * 40)
                elif fault == "failure":
                    self.record(root, "brokers", exit_code=1)
                elif fault == "scan-tamper":
                    (root.parent / "govulncheck.json").write_text("{}")
                else:
                    gate = "soak" if fault == "short-soak" else "brokers"
                    log = root / (gate + ".txt")
                    if fault == "empty":
                        log.write_text("")
                    elif fault == "short-soak":
                        log.write_text(log.read_text().replace("1h0m0s", "1m0s"))
                    elif fault == "missing-test":
                        log.write_text("ok  github.com/thanhbvha/go-common/v2/fixture 1.0s\n")
                    else:
                        log.write_text(log.read_text() + "--- SKIP: required-drill\n")
                    if fault != "tamper":
                        self.record(root, gate)
                with self.assertRaises((ValueError, OSError)):
                    evidence.verify(root, "a" * 40)


if __name__ == "__main__":
    unittest.main()
