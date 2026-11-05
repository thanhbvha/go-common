#!/usr/bin/env python3
"""Sequential, fail-closed performance runner. Uses only the Python standard library."""
import argparse
import collections
import contextlib
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import signal
import socket
import statistics
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = json.loads((ROOT / 'benchmarks/manifest.json').read_text())


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def capture(args, cwd=ROOT):
    return subprocess.check_output(args, cwd=cwd, text=True, stderr=subprocess.STDOUT if args[:2] == ["docker", "logs"] else subprocess.PIPE, timeout=120).strip()


def stop_process(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def command(args, directory, stem, env=None, timeout=600, cwd=ROOT):
    """Retain both streams and exit evidence, even on timeout or interruption."""
    directory = Path(directory)
    record = {'argv': list(map(str, args)), 'status': 'running'}
    record_path = directory / (stem + '.command.json')
    write_json(record_path, record)
    with (directory / (stem + '.stdout')).open('w') as stdout, (directory / (stem + '.stderr')).open('w') as stderr:
        process = subprocess.Popen(args, cwd=cwd, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            process.wait(timeout=timeout)
            record.update(exit_code=process.returncode, status='passed' if process.returncode == 0 else 'failed')
        except BaseException as exc:
            stop_process(process)
            record.update(exit_code=process.returncode, status='interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed', reason=type(exc).__name__)
            raise
        finally:
            write_json(record_path, record)
    if process.returncode:
        raise RuntimeError(f'{stem} exited {process.returncode}; see {directory / (stem + ".stderr")}')
    return (directory / (stem + '.stdout')).read_text()


def optional_file(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def fingerprint(paths):
    return digest(b''.join(str(p).encode() + b'\0' + (ROOT / p).read_bytes() + b'\0' for p in sorted(paths) if (ROOT / p).is_file()))


def metadata():
    paths = capture(['git', 'ls-files', '-c', '-o', '--exclude-standard', '-z']).split('\0')
    paths = sorted(set(filter(None, paths)))
    harness = [p for p in paths if p.startswith(('benchmarks/', 'internal/perftest/')) or p.endswith(('benchmark_test.go', 'performance_test.go'))]
    cpu = next((line.split(':', 1)[1].strip() for line in (optional_file('/proc/cpuinfo') or '').splitlines() if line.startswith('model name')), platform.processor())
    environment = {
        'go': capture(['go', 'version']), 'go_env': json.loads(capture(['go', 'env', '-json', 'GOOS', 'GOARCH', 'GOAMD64', 'CGO_ENABLED', 'GOEXPERIMENT', 'GOTOOLCHAIN'])),
        'system': platform.platform(), 'cpu': cpu, 'logical_cpus': os.cpu_count(),
        'affinity': sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
        'host_id': digest((optional_file('/etc/machine-id') or platform.node()).encode()),
        'boot_id': optional_file('/proc/sys/kernel/random/boot_id'),
        'cpu_max': optional_file('/sys/fs/cgroup/cpu.max'), 'memory_max': optional_file('/sys/fs/cgroup/memory.max'),
        'memory_total': next((x for x in (optional_file('/proc/meminfo') or '').splitlines() if x.startswith('MemTotal:')), None),
        'governor': optional_file('/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor'),
        'go_runtime_env': {key: os.environ.get(key, '') for key in ('GODEBUG', 'GOGC', 'GOMEMLIMIT', 'GOFLAGS')},
    }
    if environment['go_runtime_env']['GOFLAGS']:
        raise ValueError('unset GOFLAGS: race/build overrides make benchmark configuration ambiguous')
    return {'revision': capture(['git', 'rev-parse', 'HEAD']), 'dirty': bool(capture(['git', 'status', '--porcelain'])),
            'source_fingerprint': fingerprint(paths), 'harness_fingerprint': fingerprint(harness),
            'manifest_hash': digest((ROOT / 'benchmarks/manifest.json').read_bytes()), 'environment': environment}


class Fixtures:
    """Own exact container IDs only. No reused endpoint, volume, or global prune."""
    def __init__(self, names, directory):
        self.names, self.directory, self.containers, self.images = names, directory, [], {}
        self.owner = uuid.uuid4().hex
        self.env = {}

    def docker(self, *args):
        return capture(['docker', *args])

    def __enter__(self):
        try:
            for name in self.names:
                self.start(name)
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def start(self, name):
        image = MANIFEST['images'][name]
        ports = {'postgres':5432, 'redis':6379, 'nats':4222, 'kafka':9092}
        port = ports[name]
        mapping = f'127.0.0.1::{port}'
        options, arguments = [], []
        if name == 'postgres':
            options = ['-e', 'POSTGRES_DB=performance', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust']
        elif name == 'redis':
            arguments = ['redis-server', '--appendonly', 'yes', '--appendfsync', 'everysec']
        elif name == 'nats':
            arguments = ['-js']
        elif name == 'kafka':
            with socket.socket() as candidate:
                candidate.bind(('127.0.0.1', 0))
                host_port = candidate.getsockname()[1]
            mapping = f'127.0.0.1:{host_port}:9092'
            variables = {'KAFKA_NODE_ID':'1','KAFKA_PROCESS_ROLES':'broker,controller',
                'KAFKA_LISTENERS':'PLAINTEXT://:9092,CONTROLLER://:9093',
                'KAFKA_ADVERTISED_LISTENERS':f'PLAINTEXT://127.0.0.1:{host_port}',
                'KAFKA_CONTROLLER_LISTENER_NAMES':'CONTROLLER','KAFKA_LISTENER_SECURITY_PROTOCOL_MAP':'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
                'KAFKA_CONTROLLER_QUORUM_VOTERS':'1@localhost:9093','KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR':'1',
                'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR':'1','KAFKA_TRANSACTION_STATE_LOG_MIN_ISR':'1',
                'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS':'0','KAFKA_HEAP_OPTS':'-Xms256m -Xmx512m'}
            options = [part for key, value in variables.items() for part in ('-e', key + '=' + value)]
        # Register a unique name before starting Docker: interruption can arrive
        # after daemon creation but before the CLI returns the container ID.
        container_name = 'go-common-performance-' + uuid.uuid4().hex
        self.containers.append(container_name)
        container = self.docker('run', '-d', '--name', container_name, '--label', 'go-common.performance=' + self.owner,
                                '-p', mapping, *options, image, *arguments)
        self.images[name] = json.loads(self.docker('image', 'inspect', image))[0]['Id']
        mapped = self.docker('port', container, f'{port}/tcp').rsplit(':', 1)[1]
        address = '127.0.0.1:' + mapped
        # initdb starts a temporary Unix-socket-only server. Require TCP so a
        # warm build cache cannot outrun the final PostgreSQL server startup.
        health = {'postgres':['pg_isready','-h','127.0.0.1','-U','postgres','-d','performance'], 'redis':['redis-cli','ping'],
                  'kafka':['/opt/kafka/bin/kafka-broker-api-versions.sh','--bootstrap-server','localhost:9092']}
        deadline = time.monotonic() + 120
        while True:
            if self.docker('inspect', '-f', '{{.State.Running}}', container) != 'true':
                raise RuntimeError(f'{name} fixture exited')
            try:
                if name in health:
                    self.docker('exec', container, *health[name])
                else:
                    with socket.create_connection(('127.0.0.1', int(mapped)), timeout=1):
                        pass
                break
            except (subprocess.CalledProcessError, OSError):
                if time.monotonic() > deadline:
                    raise RuntimeError(f'{name} readiness timeout')
                time.sleep(.25)
        self.env.update({
            'postgres': {'ORM_POSTGRES_DSN':f'host=127.0.0.1 port={mapped} user=postgres dbname=performance sslmode=disable', 'ORM_INTEGRATION_REQUIRED':'1', 'PRODUCTION_TEST_POSTGRES_DSN':f'host=127.0.0.1 port={mapped} user=postgres dbname=performance sslmode=disable'},
            'redis': {'WS_REGISTRY_BENCH_ADDR':address, 'PRODUCTION_TEST_REDIS_ADDR':address,'EVENTBUS_REDIS_ADDR':address,'PERF_REDIS_ADDR':address},
            'nats': {'EVENTBUS_NATS_URLS':'nats://' + address},
            'kafka': {'EVENTBUS_KAFKA_BROKERS':address},
        }[name])

    def __exit__(self, kind, value, traceback):
        errors = []
        for index, container in enumerate(reversed(self.containers)):
            try:
                owner = self.docker('inspect', '-f', '{{index .Config.Labels "go-common.performance"}}', container)
                if owner != self.owner:
                    errors.append(container)
                    continue
            except subprocess.CalledProcessError as exc:
                if 'No such' in (exc.stderr or ''):
                    continue
                errors.append(container)
                continue
            try:
                (self.directory / f'fixture-{index}.log').write_text(self.docker('logs', container))
            except subprocess.CalledProcessError:
                pass
            try:
                self.docker('rm', '-f', '-v', container)
            except subprocess.CalledProcessError:
                errors.append(container)
        write_json(self.directory / 'fixtures.json', {'images':self.images,'owned_containers':self.containers,'owner':self.owner,'cleanup_failed':errors})
        if errors and kind is None:
            raise RuntimeError('fixture cleanup failed: ' + ', '.join(errors))


BENCHMARK = re.compile(r'^(Benchmark\S+)\s+(\d+)\s+(.+)$', re.M)


def parse_benchmarks(output):
    if re.search(r'(?m)^\s*--- SKIP:|^SKIP\b|^FAIL\b', output):
        raise ValueError('benchmark skipped or failed')
    rows = []
    for name, iterations, values in BENCHMARK.findall(output):
        fields = values.split()
        if len(fields) % 2:
            raise ValueError('invalid benchmark metrics: ' + name)
        metrics = {fields[i+1]: float(fields[i]) for i in range(0, len(fields), 2)}
        if int(iterations) < 1 or not {'ns/op', 'B/op', 'allocs/op'} <= metrics.keys() or not all(math.isfinite(v) and v >= 0 for v in metrics.values()):
            raise ValueError('missing or invalid benchmark metrics: ' + name)
        rows.append({'name':name,'iterations':int(iterations),'metrics':metrics})
    if not rows:
        raise ValueError('no benchmark results')
    return rows


def validate_inventory(rows, names, cpus, count):
    expected = collections.Counter(name + ('-' + cpu if cpu != '1' else '') for name in names for cpu in cpus.split(',') for _ in range(count))
    actual = collections.Counter(row['name'] for row in rows)
    if expected != actual:
        raise ValueError(f'benchmark inventory mismatch: missing={dict(expected-actual)} unexpected={dict(actual-expected)}')


def validate_load(data):
    if not isinstance(data, list) or not data:
        raise ValueError('missing workload results')
    for row in data:
        required = {'name','completed','errors','duplicates','missing','throughput_per_second','p50_ms','p95_ms','p99_ms','server','client'}
        if not required <= row.keys() or not isinstance(row['completed'], int) or row['completed'] <= 0 or any(row[key] != 0 for key in ('errors','duplicates','missing')):
            raise ValueError('incomplete workload or failed deliveries')
        if not all(math.isfinite(row[key]) and row[key] >= 0 for key in ('throughput_per_second','p50_ms','p95_ms','p99_ms')):
            raise ValueError('invalid workload metrics')
        if not row['p50_ms'] <= row['p95_ms'] <= row['p99_ms']:
            raise ValueError('invalid percentile order')
        resource_keys = {'heap_before_bytes','heap_after_bytes','peak_heap_bytes','allocated_bytes','allocations',
                         'goroutines_before','goroutines_after','peak_goroutines','gc_cycles'}
        for side in ('server', 'client'):
            if not resource_keys <= row[side].keys() or any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in row[side].values()):
                raise ValueError('missing or invalid process resource metrics')
        if row.get('pending', 0) or row.get('in_flight', 0) or row.get('delayed', 0) or row.get('dlq', 0):
            raise ValueError('workload did not drain')
        if row['name'].startswith('websocket-'):
            if row.get('backlog_measured') is not True or any(key not in row for key in ('pending','in_flight','delayed','dlq')):
                raise ValueError('WebSocket requires observed broker backlog')
            if any(not isinstance(row.get(key), int) or isinstance(row[key], bool) or row[key] <= 0 for key in ('sent_operations','expected_receipts','received')):
                raise ValueError('WebSocket requires delivery counts')
            multiplier = 10 if row['name'] == 'websocket-fanout' else 1
            if row['received'] != row['expected_receipts'] or row['expected_receipts'] != row['sent_operations'] * multiplier or row['sent_operations'] < row['completed']:
                raise ValueError('WebSocket receipt count mismatch')
            if not isinstance(row.get('broker_drain_seconds'), (int, float)) or not math.isfinite(row['broker_drain_seconds']) or not 0 <= row['broker_drain_seconds'] <= 15:
                raise ValueError('missing or invalid broker drain duration')


def checked_env():
    # Fixture credentials/endpoints must come from this runner, not the caller.
    env = {k:v for k,v in os.environ.items() if not k.startswith(('PERF_', 'EVENTBUS_', 'WS_', 'ORM_', 'PRODUCTION_', 'APP_', 'OTEL_'))}
    env['GOMAXPROCS'] = '4'
    return env


def select(args):
    workloads = [w for w in MANIFEST['workloads'] if args.suite == 'all' or w['suite'] == args.suite]
    if args.workload:
        wanted = args.workload.split(',')
        workloads = [w for w in workloads if w['id'] in wanted]
        if {w['id'] for w in workloads} != set(wanted):
            raise ValueError('unknown workload or workload outside selected suite')
    if not workloads:
        raise ValueError('no workloads selected')
    if args.benchmark:
        if len(workloads) != 1 or args.benchmark not in workloads[0].get('benchmarks', []):
            raise ValueError('--benchmark must name one exact leaf in one selected Go workload')
        workloads = [dict(workloads[0], benchmarks=[args.benchmark])]
    return workloads


def go_filter(names):
    # Go matches slash-separated expressions independently; enforce inventory afterwards.
    return '|'.join('^' + re.escape(name) + '$' for name in sorted({x.split('/')[0] for x in names}))


def seal(directory):
    return {str(p.relative_to(directory)):digest(p.read_bytes()) for p in sorted(directory.rglob('*')) if p.is_file() and p not in (directory / 'run.json', directory / 'report.md')}


def execute(args):
    workloads = select(args)
    directory = Path(args.output or ROOT / 'artifacts/performance' / (datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8])).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    count = args.count or (1 if args.mode == 'smoke' or args.action == 'profile' else MANIFEST['defaults']['count'])
    config = {'mode':args.mode, 'count':count, 'cpu':args.cpu, 'benchtime':args.benchtime or ('1x' if args.mode == 'smoke' else '1s'),
              'duration_seconds':args.duration, 'warmup_seconds':args.warmup, 'drain_seconds':15, 'profile':args.profile_kind if args.action == 'profile' else None,
              'workloads':workloads}
    record = {'schema':1,'harness':MANIFEST['harness'],'benchstat':MANIFEST['benchstat'],
              'started':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'load_average':os.getloadavg(),
              'status':'running','configuration':config,'cases':[], **metadata()}
    write_json(directory / 'run.json', record)
    print(directory, flush=True)
    try:
        loadgen = directory / 'loadgen'
        if any(w['kind'] == 'load' for w in workloads):
            command(['go','build','-o',str(loadgen),'./benchmarks/loadgen'],directory,'build-loadgen',env=checked_env())
        wsloadgen = directory / 'websocket-loadgen'
        if any(w['id'] == 'websocket' for w in workloads):
            command(['go','build','-o',str(wsloadgen),'./examples/websocket/loadtest'],directory,'build-websocket-loadgen',env=checked_env())
        for workload in workloads:
            print('Running ' + workload['id'], file=sys.stderr, flush=True)
            case = {'id':workload['id'],'status':'running','samples':[]}
            record['cases'].append(case)
            iterations = range(count) if workload['kind'] == 'load' else range(1)
            for iteration in iterations:
                case_dir = directory / (workload['id'] + '-' + str(iteration))
                case_dir.mkdir()
                with Fixtures(workload['fixtures'], case_dir) as fixtures:
                    env = checked_env() | fixtures.env
                    if workload['kind'] == 'go':
                        names = workload['benchmarks']
                        bench_filter = go_filter(names)
                        if args.benchmark:
                            bench_filter = '/'.join('^' + re.escape(x) + '$' for x in args.benchmark.split('/'))
                        benchtime = args.benchtime or ('1x' if args.mode == 'smoke' else workload.get('benchtime','1s'))
                        if workload['id'] == 'redis-registry' and args.mode != 'smoke' and not benchtime.endswith('x'):
                            raise ValueError('registry requires a bounded iteration count, e.g. 1000x')
                        cmd = ['go','test','-v','-run','^$','-bench',bench_filter,'-benchmem','-benchtime',benchtime,'-count',str(count),'-cpu',args.cpu,'-p=1','-timeout=30m']
                        if workload['suite'] == 'integration':
                            cmd += ['-tags=integration']
                        if args.action == 'profile':
                            binary = case_dir / 'benchmark.test'
                            cmd += ['-o',str(binary)]
                            kinds = {'cpu':['-cpuprofile',str(case_dir / 'cpu.pprof')],
                                'memory':['-memprofile',str(case_dir / 'alloc.pprof'),'-memprofilerate=1'],
                                'mutex':['-mutexprofile',str(case_dir / 'mutex.pprof'),'-mutexprofilefraction=1'],
                                'block':['-blockprofile',str(case_dir / 'block.pprof'),'-blockprofilerate=1']}
                            cmd += kinds[args.profile_kind]
                        cmd += [workload['package']]
                        output = command(cmd,case_dir,'benchmark',env=env,timeout=args.timeout)
                        rows = parse_benchmarks(output)
                        validate_inventory(rows,names,args.cpu,count)
                        case['samples'] = rows
                        if args.action == 'profile':
                            for profile in case_dir.glob('*.pprof'):
                                command(['go','tool','pprof','-top','-nodecount=25',str(binary),str(profile)],case_dir,profile.stem+'-top',env=env)
                    else:
                        env |= {'PERF_RUN':'1','PERF_OUTPUT':str(case_dir),'PERF_CONFIG':json.dumps(workload.get('config',{})),
                            'PERF_WS_LOADGEN':str(wsloadgen),'PERF_DURATION':str(args.duration),'PERF_WARMUP':str(args.warmup),'PERF_LOADGEN':str(loadgen),'PERF_PROFILE':config['profile'] or ''}
                        binary = case_dir / 'server.test'
                        command(['go','test','-c','-tags=integration','-o',str(binary),workload['package']],case_dir,'build',env=env)
                        output = command([str(binary),'-test.v','-test.run','^'+workload['test']+'$','-test.timeout=5m'],case_dir,'load',env=env,timeout=args.timeout)
                        if '--- SKIP:' in output or 'PASS' not in output:
                            raise ValueError('workload skipped or did not pass')
                        data = json.loads((case_dir / 'metrics.json').read_text())
                        validate_load(data)
                        if any(not args.duration <= row['elapsed_seconds'] <= args.duration + 15 for row in data):
                            raise ValueError('workload measurement duration/drain mismatch')
                        expected = {'http':{'http-1','http-16'},'websocket':{'websocket-message','websocket-fanout'}}.get(workload['id'], {workload['id']})
                        if collections.Counter(row['name'] for row in data) != collections.Counter(expected):
                            raise ValueError('load scenario inventory mismatch')
                        case['samples'].extend(data)
                        if args.action == 'profile':
                            for profile in case_dir.glob('*/*.pprof'):
                                command(['go','tool','pprof','-top','-nodecount=25',str(binary),str(profile)],profile.parent,profile.stem+'-top',env=env)
                        else:
                            # Baselines need raw metrics, not ten identical
                            # copies of each test executable. Profiles keep it.
                            binary.unlink()
                    case.setdefault('images', []).append(fixtures.images)
            case['status'] = 'passed'
            write_json(directory / 'run.json', record)
        if metadata()['source_fingerprint'] != record['source_fingerprint']:
            raise ValueError('source files changed during measurement; rerun on a stable checkout')
        record['status'] = 'passed'
    except BaseException as exc:
        record.update(status='interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed', error=str(exc))
        for case in record['cases']:
            if case['status'] == 'running':
                case['status'] = record['status']
        raise
    finally:
        record['finished'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        record['files'] = seal(directory)
        write_json(directory / 'run.json', record)
        (directory / 'report.md').write_text('# Performance run\n\nStatus: **' + record['status'] + '**\n\n' + '\n'.join('- ' + c['id'] + ': ' + c['status'] for c in record['cases']) + '\n\nSee run.json for configuration, provenance and samples. Profile/smoke runs are not baselines.\n')
    return directory


def load_run(directory):
    directory = Path(directory).resolve()
    data = json.loads((directory / 'run.json').read_text())
    if not isinstance(data, dict) or not {'status','configuration','cases','files','environment','harness','harness_fingerprint','manifest_hash'} <= data.keys():
        raise ValueError('invalid run record')
    config = data['configuration']
    if not isinstance(config, dict) or not {'mode','profile','count','cpu','workloads'} <= config.keys() or not isinstance(config['count'], int):
        raise ValueError('invalid run configuration')
    if data['status'] != 'passed' or data['configuration']['mode'] != 'baseline' or data['configuration']['profile']:
        raise ValueError('comparison requires successful, unprofiled baseline runs')
    if data['configuration']['count'] < 6:
        raise ValueError('comparison requires at least six independent samples')
    if data['files'] != seal(directory):
        raise ValueError('artifact missing, changed or added after run')
    if [c['id'] for c in data['cases']] != [w['id'] for w in data['configuration']['workloads']] or any(c['status'] != 'passed' for c in data['cases']):
        raise ValueError('case inventory incomplete')
    for workload, case in zip(data['configuration']['workloads'], data['cases']):
        if workload['kind'] == 'go':
            validate_inventory(case['samples'],workload['benchmarks'],data['configuration']['cpu'],data['configuration']['count'])
        else:
            validate_load(case['samples'])
            counts = collections.Counter(row['name'] for row in case['samples'])
            if any(n != data['configuration']['count'] for n in counts.values()):
                raise ValueError('missing independent load samples')
    return directory, data


def bootstrap_difference(before, after):
    """Percentile CI over independent runs, never over individual requests."""
    rng = random.Random(1729)
    values = sorted(statistics.mean(rng.choices(after,k=len(after))) - statistics.mean(rng.choices(before,k=len(before))) for _ in range(10000))
    return values[249], values[9749]


def benchstat(directory):
    target = ROOT / 'artifacts/performance-tools'
    target.mkdir(parents=True, exist_ok=True)
    binary = target / 'benchstat'
    version = MANIFEST['benchstat'].split('@')[1]
    if not binary.exists():
        command(['go','install',MANIFEST['benchstat']],directory,'install-benchstat',env=dict(os.environ,GOBIN=str(target)))
    if version not in capture(['go','version','-m',str(binary)]):
        raise ValueError('benchstat version mismatch; remove artifacts/performance-tools/benchstat and retry')
    return binary


def compare(args):
    base_dir, base = load_run(args.base)
    candidate_dir, candidate = load_run(args.candidate)
    for key in ('harness','harness_fingerprint','manifest_hash','environment','configuration'):
        if base[key] != candidate[key]:
            raise ValueError('incompatible comparison: ' + key)
    if [c.get('images') for c in base['cases']] != [c.get('images') for c in candidate['cases']]:
        raise ValueError('fixture images differ')
    directory = Path(args.output or ROOT / 'artifacts/performance' / ('comparison-' + uuid.uuid4().hex[:8])).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    report = ['# Performance comparison', '', f'Base: `{base_dir}`', f'Candidate: `{candidate_dir}`', '', 'Smoke, profile and incompatible runs are rejected. No automatic merge gate.', '']
    go_cases = [c['id'] for c in base['cases'] if c['samples'] and 'metrics' in c['samples'][0]]
    if go_cases:
        for label, source in (('base',base_dir),('candidate',candidate_dir)):
            (directory / (label + '.txt')).write_text('\n'.join((source / (case + '-0') / 'benchmark.stdout').read_text() for case in go_cases))
        output = command([str(benchstat(directory)),str(directory/'base.txt'),str(directory/'candidate.txt')],directory,'benchstat')
        report += ['## Go benchmarks', '', '```text',output,'```','']
    comparisons = []
    for before, after in zip(base['cases'],candidate['cases']):
        if before['id'] in go_cases:
            continue
        for name in sorted({r['name'] for r in before['samples']}):
            for metric in ('throughput_per_second','p50_ms','p95_ms','p99_ms',
                           'server.allocated_bytes/op','server.allocations/op','server.peak_heap_bytes',
                           'server.heap_after_bytes','server.peak_goroutines','client.peak_heap_bytes'):
                def value(row):
                    if '.' not in metric:
                        return row[metric]
                    side, field = metric.split('.')
                    if field.endswith('/op'):
                        return row[side][field[:-3]] / row['completed']
                    return row[side][field]
                old = [value(r) for r in before['samples'] if r['name']==name]
                new = [value(r) for r in after['samples'] if r['name']==name]
                low, high = bootstrap_difference(old,new)
                comparisons.append({'workload':name,'metric':metric,'before_mean':statistics.mean(old),'after_mean':statistics.mean(new),'difference_ci95':[low,high]})
    if comparisons:
        report += ['## Workload runs', '', '95% bootstrap CI for candidate minus base; independent runs are the sampling unit.', '', '| Workload | Metric | Before mean | After mean | Difference CI95 |','|---|---|---:|---:|---|']
        report += [f"| {r['workload']} | {r['metric']} | {r['before_mean']:.4g} | {r['after_mean']:.4g} | [{r['difference_ci95'][0]:.4g}, {r['difference_ci95'][1]:.4g}] |" for r in comparisons]
    write_json(directory/'comparison.json', {'base':str(base_dir),'candidate':str(candidate_dir),'workloads':comparisons})
    (directory/'report.md').write_text('\n'.join(report)+'\n')
    print(directory/'report.md')


def alternate(args):
    """Run AB/BA pairs on two existing checkouts without switching either one."""
    count = args.count or 10
    if args.mode != 'baseline' or count < 6:
        raise ValueError('A/B requires baseline mode and at least six pairs')
    directory = Path(args.output or ROOT / 'artifacts/performance' / ('ab-' + uuid.uuid4().hex[:8])).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    sources = {'base':Path(args.base_source).resolve(), 'candidate':Path(args.candidate_source).resolve()}
    records = {'base':[], 'candidate':[]}
    experiment = {'status':'running','order':[],'sources':{k:str(v) for k,v in sources.items()}}
    print(directory, flush=True)
    try:
        for iteration in range(count):
            for side in (('base','candidate') if iteration % 2 == 0 else ('candidate','base')):
                source = sources[side]
                target = directory / side / 'samples' / str(iteration)
                target.parent.mkdir(parents=True, exist_ok=True)
                cmd = [sys.executable,str(source / 'benchmarks/performance.py'),'run','--suite',args.suite,
                       '--mode','baseline','--count','1','--cpu',args.cpu,'--duration',str(args.duration),
                       '--warmup',str(args.warmup),'--timeout',str(args.timeout),'--output',str(target)]
                for key in ('workload','benchmark','benchtime'):
                    if getattr(args,key):
                        cmd += ['--'+key,str(getattr(args,key))]
                command(cmd,directory,side+'-'+str(iteration),timeout=args.timeout*max(1,len(select(args))),cwd=source)
                record = json.loads((target / 'run.json').read_text())
                if record['status'] != 'passed' or record['files'] != seal(target):
                    raise ValueError('failed or corrupt A/B repetition')
                if records[side] and record['source_fingerprint'] != records[side][0]['source_fingerprint']:
                    raise ValueError(side + ' source changed between A/B repetitions')
                records[side].append(record)
                experiment['order'].append({'iteration':iteration,'side':side,'directory':str(target)})
                write_json(directory / 'experiment.json',experiment)
                if records['base'] and records['candidate']:
                    for key in ('harness_fingerprint','manifest_hash','environment','configuration'):
                        if records['base'][0][key] != records['candidate'][0][key]:
                            raise ValueError('incompatible A/B checkouts: ' + key)
        for side in sources:
            aggregate = json.loads(json.dumps(records[side][0]))
            aggregate['configuration']['count'] = count
            aggregate['pairing_id'] = str(directory)
            aggregate['finished'] = records[side][-1]['finished']
            for index, case in enumerate(aggregate['cases']):
                case['samples'] = [sample for r in records[side] for sample in r['cases'][index]['samples']]
                case['images'] = [image for r in records[side] for image in r['cases'][index]['images']]
            aggregate['files'] = seal(directory / side)
            write_json(directory / side / 'run.json',aggregate)
            (directory / side / 'report.md').write_text('# Alternating A/B samples\n\nSee the parent experiment.json for execution order.\n')
            # The comparison reads these canonical streams; originals remain sealed.
            for workload in aggregate['configuration']['workloads']:
                if workload['kind'] == 'go':
                    canonical = directory / side / (workload['id']+'-0')
                    canonical.mkdir()
                    (canonical / 'benchmark.stdout').write_text('\n'.join((directory/side/'samples'/str(i)/(workload['id']+'-0')/'benchmark.stdout').read_text() for i in range(count)))
            aggregate['files'] = seal(directory / side)
            write_json(directory / side / 'run.json',aggregate)
        compare(argparse.Namespace(base=str(directory/'base'),candidate=str(directory/'candidate'),output=str(directory/'comparison')))
        experiment['status'] = 'passed'
    except BaseException as exc:
        experiment.update(status='failed',error=str(exc))
        raise
    finally:
        write_json(directory/'experiment.json',experiment)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action',required=True)
    for action in ('run','profile','ab'):
        p = sub.add_parser(action)
        p.add_argument('--suite',choices=['core','integration','all'],default='core')
        p.add_argument('--workload',help='comma-separated manifest IDs')
        p.add_argument('--benchmark',help='one exact manifest leaf; requires one workload')
        p.add_argument('--mode',choices=['smoke','baseline'],default='smoke' if action=='run' else 'baseline')
        p.add_argument('--output')
        p.add_argument('--count',type=int)
        p.add_argument('--cpu',default='1,4')
        p.add_argument('--benchtime')
        p.add_argument('--duration',type=float,default=30)
        p.add_argument('--warmup',type=float,default=5)
        p.add_argument('--timeout',type=float,default=1800)
        p.add_argument('--profile-kind',choices=['cpu','memory','mutex','block'],default='cpu')
        if action == 'ab':
            p.add_argument('--base-source',required=True,help='existing checkout with the same harness')
            p.add_argument('--candidate-source',default=str(ROOT),help='existing candidate checkout')
    p = sub.add_parser('compare')
    p.add_argument('--base',required=True)
    p.add_argument('--candidate',required=True)
    p.add_argument('--output')
    args = parser.parse_args(argv)
    if args.action == 'compare':
        compare(args)
    else:
        if (args.count is not None and not 1 <= args.count <= 100) or not re.fullmatch(r'[1-9]\d*(,[1-9]\d*)*',args.cpu) or len(set(args.cpu.split(','))) != len(args.cpu.split(',')):
            parser.error('invalid count or CPU list')
        if not 0 < args.duration <= 120 or not 0 <= args.warmup <= 60 or not 0 < args.timeout <= 7200:
            parser.error('invalid duration, warmup or timeout')
        if args.benchtime and not re.fullmatch(r'[1-9]\d*(x|ms|s)',args.benchtime):
            parser.error('benchtime must be a positive integer followed by x, ms or s')
        if args.action == 'ab':
            alternate(args)
        else:
            execute(args)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        main()
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError, KeyboardInterrupt) as exc:
        print(f'performance: {exc}',file=sys.stderr)
        sys.exit(1)
