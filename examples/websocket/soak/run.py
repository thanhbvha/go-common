#!/usr/bin/env python3
"""Linux/TLS soak and chaos driver for the WebSocket cluster example.

Needs only Docker (Docker Desktop or WSL). Builds Linux binaries in a golang
container, generates a throwaway certificate, starts Redis + worker + router +
2 gateways behind an nginx TLS proxy, and runs scenarios from loadtest.

  python run.py build
  python run.py up
  python run.py capacity --connections 10000        # 5000 per gateway
  python run.py soak --minutes 30 --connections 1000
  python run.py chaos-redis --outage 30
  python run.py down
  python run.py all --minutes 30

Results go to .work/out (JSON, samples.csv, report.md). Credentials in compose.yaml
are throwaway and the stack binds to loopback only.
"""
import argparse, csv, json, os, statistics, subprocess, sys, threading, time, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
WORK = HERE / ".work"
OUT = WORK / "out"
ADMIN = "soak-admin-token-0123456789abcdef"
GATEWAYS = {"gw1": 18081, "gw2": 18082}
COMPOSE = ["docker", "compose", "-p", "wssoak", "-f", str(HERE / "compose.yaml")]


# Git Bash rewrites container paths such as /opt/x into Windows paths.
os.environ["MSYS_NO_PATHCONV"] = "1"
os.environ["MSYS2_ARG_CONV_EXCL"] = "*"


def sh(cmd, check=True, **kw):
    return subprocess.run(cmd, check=check, **kw)


def build():
    (WORK / "bin").mkdir(parents=True, exist_ok=True)
    (WORK / "certs").mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    script = (
        "set -e; export CGO_ENABLED=0 GOFLAGS=-mod=mod;"
        "go build -o examples/websocket/soak/.work/bin/cluster ./examples/websocket/cluster;"
        "go build -o examples/websocket/soak/.work/bin/loadtest ./examples/websocket/loadtest;"
        "openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=proxy "
        "-addext subjectAltName=DNS:proxy,DNS:localhost,IP:127.0.0.1 "
        "-keyout examples/websocket/soak/.work/certs/key.pem "
        "-out examples/websocket/soak/.work/certs/cert.pem 2>/dev/null;"
        "chmod 644 examples/websocket/soak/.work/certs/key.pem"
    )
    sh(["docker", "run", "--rm", "-v", f"{ROOT}:/src", "-v", "wssoak-gocache:/root/.cache/go-build",
        "-v", "wssoak-gomod:/go/pkg/mod", "-w", "/src", "golang:1.26", "sh", "-c", script])


def up():
    sh(COMPOSE + ["up", "-d", "--wait", "redis", "worker", "router", "gw1", "gw2", "proxy"])
    wait_ready(60)


def down():
    sh(COMPOSE + ["--profile", "load", "down", "-v", "--remove-orphans"], check=False)


def get(port, path, timeout=3):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", headers={"Authorization": f"Bearer {ADMIN}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception:
        return 0, {}


def ready(port):
    return get(port, "/readyz")[0] == 200


def wait_ready(seconds):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if all(ready(p) for p in GATEWAYS.values()):
            return time.time()
        time.sleep(0.5)
    raise SystemExit("gateways not ready")


def sample():
    row = {"t": time.time()}
    for name, port in GATEWAYS.items():
        code, body = get(port, "/admin/stats")
        m, p = body.get("manager", {}), body.get("process", {})
        row[f"{name}_ready"] = int(ready(port))
        row[f"{name}_conns"] = m.get("totalConnections", -1)
        row[f"{name}_goroutines"] = p.get("goroutines", -1)
        row[f"{name}_heap"] = p.get("heapAllocBytes", -1)
        row[f"{name}_lease_failures"] = body.get("gateway", {}).get("LeaseFailures", -1)
        row[f"{name}_slow"] = m.get("runtime", {}).get("slowConsumers", -1)
    return row


class Sampler(threading.Thread):
    def __init__(self, every=10):
        super().__init__(daemon=True)
        self.every, self.rows, self.stop_ev = every, [], threading.Event()

    def run(self):
        while not self.stop_ev.is_set():
            self.rows.append(sample())
            self.stop_ev.wait(self.every)

    def finish(self, name):
        self.stop_ev.set()
        self.join(timeout=15)
        if self.rows:
            with open(OUT / f"{name}-samples.csv", "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(self.rows[0]))
                w.writeheader()
                w.writerows(self.rows)
        return self.rows


def loadgen(name, args, background=False):
    cmd = COMPOSE + ["--profile", "load", "run", "--rm", "-T", "loadgen", "/opt/loadtest", "-config", "/etc/soak.yaml",
                     "-target", "wss://proxy:8443/ws", "-token", "soak-client-token", "-tenant", "demo",
                     "-output", f"/out/{name}.json"] + [str(a) for a in args]
    if background:
        return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    p = sh(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, check=False)
    if p.returncode != 0:
        sys.exit(f"loadgen {name} failed: {p.stderr[-400:]}")
    return None


def result(name):
    p = OUT / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else {}


def rss_mb():
    out = sh(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"],
             capture_output=True, text=True, check=False).stdout
    return {l.split()[0]: l.split()[1] for l in out.splitlines() if "wssoak" in l}


def growth(rows, key, skip_s):
    pts = [(r["t"], r[key]) for r in rows if r[key] >= 0]
    if len(pts) < 4:
        return None
    base_t = pts[0][0] + skip_s
    base = [v for t, v in pts if t >= base_t][:3]
    if not base:
        return None
    b, e = statistics.mean(base), statistics.mean([v for _, v in pts[-3:]])
    return {"baseline": b, "end": e, "growth_pct": round((e - b) / b * 100, 2) if b else None}


def capacity(connections):
    sampler = Sampler(5)
    before = sample()
    sampler.start()
    hold = 90
    loadgen("capacity", ["-scenario", "idle", "-connections", connections, "-accounts", 10, "-hold", f"{hold}s"], background=True)
    time.sleep(60)
    peak = sample()
    rss = rss_mb()
    proc = None
    time.sleep(hold)  # let the run finish
    rows = sampler.finish("capacity")
    conns = sum(peak[f"{g}_conns"] for g in GATEWAYS)
    heap = sum(peak[f"{g}_heap"] - before[f"{g}_heap"] for g in GATEWAYS)
    gor = sum(peak[f"{g}_goroutines"] - before[f"{g}_goroutines"] for g in GATEWAYS)
    r = result("capacity")
    summary = {"requested": connections, "connectedAtPeak": conns,
               "perGateway": {g: peak[f"{g}_conns"] for g in GATEWAYS},
               "heapBytesPerConn": round(heap / conns) if conns else None,
               "goroutinesPerConn": round(gor / conns, 2) if conns else None,
               "rss": rss, "connectErrors": r.get("connectErrors"), "messageErrors": r.get("messageErrors"),
               "readyFlaps": sum(1 for x in rows for g in GATEWAYS if x[f"{g}_ready"] == 0)}
    (OUT / "capacity-summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def soak(minutes, connections, slow):
    msgs = minutes * 60  # rate 1 msg/s per connection
    sampler = Sampler(10)
    sampler.start()
    t0 = time.time()
    loadgen("soak", ["-scenario", "fanout", "-connections", connections, "-group-size", 5, "-accounts", 10,
                     "-messages", msgs, "-rate", 1, "-slow-readers", slow], background=True).wait()
    rows = sampler.finish("soak")
    r = result("soak")
    summary = {"minutes": minutes, "durationActual": round(time.time() - t0), "load": {k: r.get(k) for k in (
        "connections", "peakActiveConnections", "connectErrors", "messageErrors", "sent", "received",
        "expectedReceipts", "missingReceipts", "duplicateReceipts", "p50Millis", "p95Millis", "p99Millis")},
        "slowReadersPercent": slow,
        "readyFlaps": sum(1 for x in rows for g in GATEWAYS if x[f"{g}_ready"] == 0),
        "leaseFailures": max([x[f"{g}_lease_failures"] for x in rows for g in GATEWAYS] or [0]),
        "slowConsumerDisconnects": max([x[f"{g}_slow"] for x in rows for g in GATEWAYS] or [0]),
        "growth": {f"{g}_{k}": growth(rows, f"{g}_{k}", min(300, minutes * 60 * 0.2)) for g in GATEWAYS for k in ("goroutines", "heap")}}
    (OUT / "soak-summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def chaos_redis(outage, connections):
    hold = outage + 120
    sampler = Sampler(1)
    sampler.start()
    proc = loadgen(f"chaos-{outage}", ["-scenario", "idle", "-connections", connections, "-accounts", 10, "-hold", f"{hold}s"], background=True)
    time.sleep(25)
    before = sample()
    sh(COMPOSE + ["stop", "redis"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    down_at = time.time()
    time.sleep(outage)
    during = sample()
    sh(COMPOSE + ["start", "redis"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    up_at = time.time()
    recovered = None
    while time.time() - up_at < 120:
        if all(ready(p) for p in GATEWAYS.values()):
            recovered = round(time.time() - up_at, 1)
            break
        time.sleep(0.5)
    time.sleep(10)
    after = sample()
    # Probe: after recovery a fresh fanout must deliver everything, with no node restart.
    loadgen(f"chaos-{outage}-probe", ["-scenario", "fanout", "-connections", 100, "-group-size", 5, "-messages", 5, "-rate", 1])
    probe = result(f"chaos-{outage}-probe")
    proc.wait(timeout=hold + 60)
    rows = sampler.finish(f"chaos-{outage}")
    r = result(f"chaos-{outage}")
    notready = [x for x in rows if not all(x[f"{g}_ready"] for g in GATEWAYS)]
    summary = {"outageSeconds": outage, "connections": connections,
               "connsBefore": sum(before[f"{g}_conns"] for g in GATEWAYS),
               "connsDuringOutage": sum(during[f"{g}_conns"] for g in GATEWAYS),
               "connsAfterRecovery": sum(after[f"{g}_conns"] for g in GATEWAYS),
               "readyDuringOutage": [during[f"{g}_ready"] for g in GATEWAYS],
               "secondsNotReady": len(notready),
               "recoveredReadyAfterSeconds": recovered,
               "clientDisconnects(messageErrors)": r.get("messageErrors"),
               "postRecoveryFanout": {k: probe.get(k) for k in ("expectedReceipts", "received", "missingReceipts", "connectErrors")},
               "leaseFailuresAfter": max(after[f"{g}_lease_failures"] for g in GATEWAYS),
               "gatewayRestarts": container_restarts()}
    (OUT / f"chaos-redis-{outage}-summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def container_restarts():
    out = sh(["docker", "inspect", "-f", "{{.Name}} {{.RestartCount}} {{.State.StartedAt}}",
              "wssoak-gw1-1", "wssoak-gw2-1"], capture_output=True, text=True, check=False).stdout
    return out.strip().splitlines()


def report(parts):
    lines = ["# WebSocket Linux/TLS soak report", "", f"Generated {time.strftime('%Y-%m-%d %H:%M:%S')}",
             "Topology: 2 gateways + worker + router + Redis 7.4, nginx TLS proxy, Docker on this machine.", ""]
    for title, data in parts.items():
        lines += [f"## {title}", "```json", json.dumps(data, indent=2), "```", ""]
    (OUT / "report.md").write_text("\n".join(lines))
    print("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "up", "down", "capacity", "soak", "chaos-redis", "all"])
    ap.add_argument("--connections", type=int, default=None)
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--outage", type=int, default=30)
    ap.add_argument("--slow", type=int, default=5, help="percent of slow readers during the soak")
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    if a.cmd == "build": build()
    elif a.cmd == "up": up()
    elif a.cmd == "down": down()
    elif a.cmd == "capacity": report({"capacity": capacity(a.connections or 10000)})
    elif a.cmd == "soak": report({"soak": soak(a.minutes, a.connections or 1000, a.slow)})
    elif a.cmd == "chaos-redis": report({f"chaos-redis-{a.outage}s": chaos_redis(a.outage, a.connections or 1000)})
    elif a.cmd == "all":
        build(); down(); up()
        try:
            parts = {"capacity": capacity(10000)}
            parts["soak"] = soak(a.minutes, a.connections or 1000, a.slow)
            for outage in (30, 300):
                parts[f"chaos-redis-{outage}s"] = chaos_redis(outage, 1000)
            report(parts)
        finally:
            down()


if __name__ == "__main__":
    main()
