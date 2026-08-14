"""Day 10 recorded-demo script: integrated SQL, ledger, Raft failover, and convergence."""
from __future__ import annotations
import json, os, shutil, socket, subprocess, sys, tempfile, time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

def request(port,path,payload=None):
    data=json.dumps(payload).encode() if payload is not None else None
    req=Request(f"http://127.0.0.1:{port}{path}",data=data,headers={"Content-Type":"application/json"},method="POST" if payload is not None else "GET")
    with urlopen(req,timeout=2) as r:return json.loads(r.read())

def alloc():
    ss=[socket.socket() for _ in range(3)]
    try:
        for s in ss:s.bind(("127.0.0.1",0))
        return [s.getsockname()[1] for s in ss]
    finally:
        for s in ss:s.close()

def main():
    root=tempfile.mkdtemp(prefix="ledgerdb-v1-demo-"); ports=alloc(); peers=",".join(f"node-{i}=127.0.0.1:{p}" for i,p in enumerate(ports)); procs=[]
    try:
        for i,p in enumerate(ports):
            procs.append(subprocess.Popen([sys.executable,"-m","ledgerdb.cli","--data-dir",os.path.join(root,f"node-{i}"),"raft-server","--node-id",f"node-{i}","--peers",peers,"--port",str(p),"--heartbeat-ms","60","--election-min-ms","350","--election-max-ms","650"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
        deadline=time.monotonic()+6
        leader=None
        while time.monotonic()<deadline:
            try:
                leaders=[i for i,p in enumerate(ports) if request(p,"/status")["role"]=="leader"]
                if len(leaders)==1:leader=leaders[0];break
            except (OSError,URLError):pass
            time.sleep(.05)
        if leader is None:raise RuntimeError("no leader")
        print(f"initial_leader=node-{leader}")
        for i in range(5):
            assert request(ports[leader],"/client-write",{"command":{"operation":"insert","values":{"key":i,"amount":i*10,"category":i%2}}})["success"]
        for i in range(5):
            assert request(ports[leader],"/transaction",{"idempotency_key":f"demo-{i}","debit_account":"cash","credit_account":"revenue","amount":10,"transaction_key":i})["success"]
        q=request(ports[leader],"/query",{"sql":"SELECT key, amount FROM ledger WHERE amount BETWEEN 0 AND 100"})
        assert q["success"], q
        print(f"sql_before_kill=PASS rows={len(q['rows'])} plan={','.join(q['plan'])}")
        victim=(leader+1)%3
        print(f"kill_node=node-{victim}")
        procs[victim].kill();procs[victim].wait(timeout=2)
        restarted=time.monotonic()
        # Keep serving from the old leader while one follower is down.
        for i in range(5,10):
            result=request(ports[leader],"/transaction",{"idempotency_key":f"demo-{i}","debit_account":"cash","credit_account":"revenue","amount":10,"transaction_key":i})
            assert result["success"], result
        print("service_during_failure=PASS")
        procs[victim]=subprocess.Popen([sys.executable,"-m","ledgerdb.cli","--data-dir",os.path.join(root,f"node-{victim}"),"raft-server","--node-id",f"node-{victim}","--peers",peers,"--port",str(ports[victim]),"--heartbeat-ms","60","--election-min-ms","350","--election-max-ms","650"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:
            try:
                state=request(ports[victim],"/status")
                if state["ready"]:break
            except (OSError,URLError):pass
            time.sleep(.05)
        else:raise RuntimeError("restarted node did not become ready")
        states=[request(p,"/status") for p in ports]
        assert len({s["log_digest"] for s in states})==1
        balances=[request(p,"/state")["ledger_balance"] for p in ports]
        assert len(set(tuple(x) for x in balances))==1
        print(f"follower_catchup=PASS recovery_seconds={time.monotonic()-restarted:.3f}")
        print("v1_demo=PASS")
    finally:
        for p in procs:
            if p.poll() is None:p.kill();p.wait(timeout=2)
        shutil.rmtree(root,ignore_errors=True)
if __name__=="__main__":main()
