import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('performance', ROOT / 'benchmarks/performance.py')
perf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(perf)


class PerformanceTests(unittest.TestCase):
    def test_benchmark_inventory_and_metrics(self):
        valid = 'BenchmarkExample-4 10 2.5 ns/op 16 B/op 1 allocs/op\n'
        rows = perf.parse_benchmarks(valid)
        perf.validate_inventory(rows, ['BenchmarkExample'], '4', 1)
        for output in ('', '--- SKIP: BenchmarkExample\n' + valid,
                       'BenchmarkExample-4 0 2 ns/op 0 B/op 0 allocs/op',
                       'BenchmarkExample-4 10 NaN ns/op 0 B/op 0 allocs/op',
                       'BenchmarkExample-4 10 2 ns/op'):
            with self.subTest(output=output), self.assertRaises(ValueError):
                perf.parse_benchmarks(output)
        for bad in ([], rows * 2):
            with self.assertRaises(ValueError):
                perf.validate_inventory(bad, ['BenchmarkExample'], '4', 1)

    def test_load_rejects_missing_failed_and_undrained_work(self):
        valid = dict(name='example', completed=10, errors=0, duplicates=0, missing=0,
                     throughput_per_second=1, p50_ms=1, p95_ms=2, p99_ms=3,
                     server={key: 0 for key in ("heap_before_bytes","heap_after_bytes","peak_heap_bytes","allocated_bytes","allocations","goroutines_before","goroutines_after","peak_goroutines","gc_cycles")},
                     client={key: 0 for key in ("heap_before_bytes","heap_after_bytes","peak_heap_bytes","allocated_bytes","allocations","goroutines_before","goroutines_after","peak_goroutines","gc_cycles")})
        perf.validate_load([valid])
        for data in ([], [dict(valid, errors=1)], [dict(valid, completed=0)],
                     [dict(valid, duplicates=1)], [dict(valid, pending=1)],
                     [dict(valid, p99_ms=float('inf'))], [{}]):
            with self.subTest(data=data), self.assertRaises(ValueError):
                perf.validate_load(data)

    def test_websocket_requires_observed_backlog_and_real_receipts(self):
        resources = {key: 0 for key in ("heap_before_bytes","heap_after_bytes","peak_heap_bytes","allocated_bytes","allocations","goroutines_before","goroutines_after","peak_goroutines","gc_cycles")}
        row = dict(name='websocket-fanout', completed=10, errors=0, duplicates=0, missing=0,
                   throughput_per_second=1, p50_ms=1, p95_ms=2, p99_ms=3,
                   server=resources, client=resources, pending=0, in_flight=0, delayed=0, dlq=0,
                   backlog_measured=True, broker_drain_seconds=.1,
                   sent_operations=12, expected_receipts=120, received=120)
        perf.validate_load([row])
        for field in ('backlog_measured','broker_drain_seconds','pending','sent_operations','expected_receipts','received'):
            invalid = row.copy()
            del invalid[field]
            with self.subTest(missing=field), self.assertRaises(ValueError):
                perf.validate_load([invalid])
        for change in (dict(backlog_measured=False), dict(received=119), dict(expected_receipts=12, received=12),
                       dict(sent_operations=True), dict(broker_drain_seconds=16), dict(in_flight=1)):
            with self.subTest(change=change), self.assertRaises(ValueError):
                perf.validate_load([row | change])

    def test_command_errors_and_timeouts_leave_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            with self.assertRaises(RuntimeError):
                perf.command([sys.executable, '-c', 'import sys; print("failure",file=sys.stderr); sys.exit(7)'], path, 'failed')
            self.assertEqual(json.loads((path / 'failed.command.json').read_text())['exit_code'], 7)
            self.assertIn('failure', (path / 'failed.stderr').read_text())
            with self.assertRaises(subprocess.TimeoutExpired):
                perf.command([sys.executable, '-c', 'import time; time.sleep(30)'], path, 'timeout', timeout=.05)
            self.assertEqual(json.loads((path / 'timeout.command.json').read_text())['status'], 'failed')

    def test_interruption_reaps_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp)
            real_wait = subprocess.Popen.wait
            interrupted = False
            def wait(process, *args, **kwargs):
                nonlocal interrupted
                if not interrupted:
                    interrupted = True
                    raise KeyboardInterrupt()
                return real_wait(process, *args, **kwargs)
            with patch.object(subprocess.Popen, 'wait', wait), self.assertRaises(KeyboardInterrupt):
                perf.command([sys.executable, '-c', 'import time; time.sleep(30)'], target, 'interrupt')
            self.assertEqual(json.loads((target / 'interrupt.command.json').read_text())['status'], 'interrupted')

    def test_fixture_cleanup_on_failure_and_interrupt(self):
        for failure in (RuntimeError('readiness'), KeyboardInterrupt()):
            with tempfile.TemporaryDirectory() as tmp:
                fixture = perf.Fixtures(['redis'], Path(tmp))
                calls = []
                def start(_):
                    fixture.containers.append('owned-container')
                    raise failure
                def docker(*args):
                    calls.append(args)
                    return fixture.owner if args[0] == 'inspect' else ''
                with patch.object(fixture, 'start', start), patch.object(fixture, 'docker', docker):
                    with self.assertRaises(type(failure)):
                        with fixture:
                            pass
                self.assertIn(('rm', '-f', '-v', 'owned-container'), calls)
                self.assertEqual(json.loads((Path(tmp) / 'fixtures.json').read_text())['cleanup_failed'], [])

    def test_fixture_cleanup_checks_owner_and_propagates_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = perf.Fixtures([], Path(tmp))
            fixture.containers = ['unrelated']
            with patch.object(fixture, 'docker', return_value='someone-else') as docker:
                with self.assertRaises(RuntimeError):
                    fixture.__exit__(None, None, None)
                self.assertFalse(any(call.args[0] == 'rm' for call in docker.call_args_list))
            fixture.containers = ['owned']
            calls = []
            def docker(*args):
                calls.append(args)
                return fixture.owner if args[0] == 'inspect' else ''
            with patch.object(fixture, 'docker', docker):
                fixture.__exit__(None, None, None)
            self.assertIn(('rm', '-f', '-v', 'owned'), calls)

    def run_fixture(self, directory):
        raw = 'BenchmarkExample-4 10 2 ns/op 16 B/op 1 allocs/op\n' * 6
        (directory / 'raw.txt').write_text(raw)
        workload = dict(id='example', kind='go', benchmarks=['BenchmarkExample'])
        data = dict(status='passed', configuration=dict(mode='baseline', profile=None, count=6, cpu='4', workloads=[workload]),
                    cases=[dict(id='example', status='passed', samples=perf.parse_benchmarks(raw))],
                    harness='v1', harness_fingerprint='one', manifest_hash='one', environment={'cpu':'same'})
        data['files'] = perf.seal(directory)
        perf.write_json(directory / 'run.json', data)
        return data

    def test_compare_rejects_corruption_smoke_profile_and_missing_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            data = self.run_fixture(directory)
            perf.load_run(directory)
            for key, value in (('mode', 'smoke'), ('profile', 'cpu'), ('count', 1)):
                copy = json.loads(json.dumps(data))
                copy['configuration'][key] = value
                perf.write_json(directory / 'run.json', copy)
                with self.assertRaises(ValueError):
                    perf.load_run(directory)
            perf.write_json(directory / 'run.json', data)
            (directory / 'raw.txt').write_text('changed')
            with self.assertRaises(ValueError):
                perf.load_run(directory)
            (directory / 'raw.txt').unlink()
            with self.assertRaises(ValueError):
                perf.load_run(directory)

    def test_compare_rejects_incompatible_environment_or_harness(self):
        import argparse
        with tempfile.TemporaryDirectory() as tmp:
            directories = [Path(tmp) / name for name in ('base', 'candidate')]
            for directory in directories:
                directory.mkdir()
                self.run_fixture(directory)
            for key in ('configuration', 'environment', 'harness_fingerprint'):
                data = self.run_fixture(directories[1])
                data[key] = {} if isinstance(data[key], dict) else 'different'
                perf.write_json(directories[1] / 'run.json', data)
                with self.assertRaises(ValueError):
                    perf.compare(argparse.Namespace(base=str(directories[0]), candidate=str(directories[1]), output=str(Path(tmp) / 'comparison')))

    def test_bootstrap_uses_independent_runs(self):
        low, high = perf.bootstrap_difference([10] * 10, [8] * 10)
        self.assertEqual((low, high), (-2, -2))
        low, high = perf.bootstrap_difference([8, 12] * 5, [8, 12] * 5)
        self.assertLess(low, 0)
        self.assertGreater(high, 0)

    def test_manifest_pins_images_and_declares_all_suites(self):
        self.assertEqual({w['suite'] for w in perf.MANIFEST['workloads']}, {'core', 'integration'})
        self.assertIn('@v0.0.0-', perf.MANIFEST['benchstat'])
        for image in perf.MANIFEST['images'].values():
            self.assertRegex(image, r'@sha256:[a-f0-9]{64}$')
        names = [w['id'] for w in perf.MANIFEST['workloads']]
        self.assertEqual(len(names), len(set(names)))


if __name__ == '__main__':
    unittest.main()
