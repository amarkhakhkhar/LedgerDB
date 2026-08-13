"""Day 8 Kubernetes proof: a lagging Raft replica leaves query-service endpoints."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from urllib.request import Request, urlopen


def kubectl(*args: str) -> str:
    return subprocess.check_output(["kubectl", *args], text=True).strip()


def pod_status(pod: str) -> dict:
    raw = kubectl("exec", pod, "--", "python", "-c", "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/status').read().decode())")
    return json.loads(raw)


def post(pod: str, path: str, payload: dict) -> dict:
    code = "import json,urllib.request; p=json.loads('''%s'''); r=urllib.request.Request('http://127.0.0.1:8000%s',data=json.dumps(p).encode(),headers={'Content-Type':'application/json'},method='POST'); print(urllib.request.urlopen(r).read().decode())" % (json.dumps(payload), path)
    return json.loads(kubectl("exec", pod, "--", "python", "-c", code))


def endpoint_ready(pod: str, namespace: str) -> bool:
    raw = json.loads(kubectl("get", "endpointslice", "-n", namespace, "-l", "kubernetes.io/service-name=ledgerdb-query", "-o", "json"))
    return any(endpoint.get("targetRef", {}).get("name") == pod and endpoint.get("conditions", {}).get("ready") is True
               for item in raw.get("items", []) for endpoint in item.get("endpoints", []))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="default")
    args = parser.parse_args()
    pods = [f"ledgerdb-{i}" for i in range(3)]
    # Slow only the proof cluster's replay enough to observe the not-ready state.
    kubectl("set", "env", "statefulset/ledgerdb", "-n", args.namespace, "RAFT_CATCHUP_APPLY_DELAY_MS=100")
    kubectl("rollout", "status", "statefulset/ledgerdb", "-n", args.namespace, "--timeout=120s")
    deadline = time.monotonic() + 15
    leader = None
    while time.monotonic() < deadline:
        states = [pod_status(pod) for pod in pods]
        found = [pod for pod, state in zip(pods, states, strict=True) if state["role"] == "leader"]
        if len(found) == 1:
            leader = found[0]; break
        time.sleep(.2)
    if leader is None:
        raise SystemExit("no leader")
    follower = next(pod for pod in pods if pod != leader)
    kubectl("delete", "pod", follower, "-n", args.namespace, "--wait=false")
    for key in range(40):
        result = post(leader, "/client-write", {"command": {"operation": "insert", "values": {"key": key, "value": key}}})
        if not result["success"]:
            raise SystemExit(f"write failed: {result}")
    # The recreated replica has a leader-reported log length ahead of its local log;
    # /readyz is 503 and the normal ClusterIP service excludes its endpoint.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            state = pod_status(follower)
            if not state["ready"] and not endpoint_ready(follower, args.namespace):
                print(f"k8s_lag_not_ready=PASS pod={follower} lag={state['leader_log_index'] - state['log_length']}")
                break
        except subprocess.CalledProcessError:
            pass
        time.sleep(.15)
    else:
        raise SystemExit("did not observe lagging replica removed from query service")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            if pod_status(follower)["ready"] and endpoint_ready(follower, args.namespace):
                print(f"k8s_catchup_ready=PASS pod={follower}")
                return
        except subprocess.CalledProcessError:
            pass
        time.sleep(.15)
    raise SystemExit("replica did not return to query service after catch-up")


if __name__ == "__main__":
    main()
