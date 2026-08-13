"""Run the Day 6 leader-failure proof against the Kubernetes StatefulSet."""

from __future__ import annotations

import argparse
import json
import subprocess
import time


def kubectl(*args: str) -> str:
    return subprocess.check_output(["kubectl", *args], text=True).strip()


def pod_status(pod: str) -> dict:
    raw = kubectl("exec", pod, "--", "python", "-c", "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/status').read().decode())")
    return json.loads(raw)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--bound-seconds", type=float, default=5.0)
    args = parser.parse_args()
    pods = [f"ledgerdb-{i}" for i in range(3)]

    def statuses() -> list[dict]:
        result = []
        for pod in pods:
            try:
                item = pod_status(pod)
                item["pod"] = pod
                result.append(item)
            except subprocess.CalledProcessError:
                pass
        return result

    deadline = time.monotonic() + args.bound_seconds
    old = None
    while time.monotonic() < deadline:
        found = [item for item in statuses() if item["role"] == "leader"]
        if len(found) == 1:
            old = found[0]
            break
        time.sleep(0.2)
    if old is None:
        raise SystemExit("no unique leader found in StatefulSet")

    print(f"initial_leader={old['node_id']} pod={old['pod']} term={old['term']}")
    start = time.monotonic()
    kubectl("delete", "pod", old["pod"], "--namespace", args.namespace, "--wait=false")

    new = None
    while time.monotonic() - start < args.bound_seconds:
        found = [item for item in statuses() if item["role"] == "leader" and item["node_id"] != old["node_id"]]
        if len(found) == 1:
            new = found[0]
            break
        time.sleep(0.2)
    if new is None:
        raise SystemExit(f"no replacement leader within {args.bound_seconds}s")

    elapsed = time.monotonic() - start
    print(f"k8s_leader_re_election=PASS old={old['node_id']} new={new['node_id']} elapsed_seconds={elapsed:.3f} bound_seconds={args.bound_seconds}")


if __name__ == "__main__":
    main()
