"""Real broker; Kafka-aware loopback proxy drops alert Produce requests deterministically."""
import json
import pathlib
import socket
import struct
import subprocess
import tempfile
import threading
import time
import sys
import os

ROOT=pathlib.Path(__file__).resolve().parents[1]
IMAGE='docker.redpanda.com/redpandadata/redpanda:v24.3.18'
blocked=threading.Event(); fault=threading.Event(); fault.set(); stopped=threading.Event()
dlq_mode='--dlq' in sys.argv
commit_mode='--commit' in sys.argv
oversized_mode='--oversized' in sys.argv
fault_topic=b'telemetry.events.v1' if commit_mode else (b'telemetry.dlq.v1' if dlq_mode else b'telemetry.alerts.v1')
def exact(sock,count):
    data=b''
    while len(data)<count:
        part=sock.recv(count-len(data))
        if not part: raise EOFError()
        data+=part
    return data
def proxy(client):
    upstream=None
    try:
        upstream=socket.create_connection(('127.0.0.1',39092),timeout=5)
        upstream.settimeout(None)
        def responses():
            try:
                while not stopped.is_set():
                    part=upstream.recv(65536)
                    if not part: break
                    client.sendall(part)
            except OSError: pass
            finally:
                try:client.shutdown(socket.SHUT_RDWR)
                except OSError:pass
        threading.Thread(target=responses,daemon=True).start()
        while not stopped.is_set():
            prefix=exact(client,4);size=struct.unpack('>I',prefix)[0]
            if size>2*1024*1024:raise ValueError('proxy request bound')
            request=exact(client,size)
            if fault.is_set() and request[:2]==(b'\x00\x08' if commit_mode else b'\x00\x00') and fault_topic in request:
                blocked.set();break
            upstream.sendall(prefix+request)
    except (OSError,EOFError,ValueError):pass
    finally:
        client.close()
        if upstream:upstream.close()

listener=socket.socket();listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
listener.bind(('127.0.0.1',29092));listener.listen(32);listener.settimeout(.2)
def accept():
    while not stopped.is_set():
        try:client,_=listener.accept()
        except socket.timeout:continue
        except OSError:break
        threading.Thread(target=proxy,args=(client,),daemon=True).start()
threading.Thread(target=accept,daemon=True).start()
def run(args,**kwargs):return subprocess.run(args,check=True,text=True,capture_output=True,timeout=30,**kwargs).stdout
def rpk(*args,**kwargs):return run(['docker','run','--rm','-i','--network','host','--entrypoint','rpk',IMAGE,*args,'-X','brokers=127.0.0.1:29092'],**kwargs)
process=None
created=False
try:
    run(['docker','run','-d','--name','nexstream-s07-delivery-broker','-p','127.0.0.1:39092:9092',IMAGE,
         'redpanda','start','--overprovisioned','--smp','1','--memory','512M','--reserve-memory','0M','--check=false',
         '--kafka-addr','0.0.0.0:9092','--advertise-kafka-addr','127.0.0.1:29092'])
    created=True
    for _ in range(40):
        try:rpk('topic','create','telemetry.events.v1','telemetry.alerts.v1','telemetry.dlq.v1');break
        except subprocess.CalledProcessError:time.sleep(.25)
    with tempfile.TemporaryDirectory(prefix='nexstream-outage-') as folder:
        state=pathlib.Path(folder)/'checkpoint'
        command=[os.environ.get('NEXSTREAM_PROCESSOR_BINARY',str(ROOT/'build-s07-Debug/nexstream-processor')),'127.0.0.1:29092','nexstream.outage.v1','80','80','1',str(state)]
        process=subprocess.Popen(command,stdout=subprocess.DEVNULL)
        event=dict(schema_version=1,host_id='outage-host',message_id='stable-message',sample_timestamp_ms=int(time.time()*1000),cpu_usage_percent=99,memory_used_bytes=99,memory_total_bytes=100)
        payload=json.dumps(event)
        if dlq_mode:payload='not-json'
        if oversized_mode:payload=json.dumps({**event,'padding':'x'*262144})
        rpk('topic','produce','telemetry.events.v1','-k','outage-host',input=payload+'\n')
        assert blocked.wait(15),'no alert Produce intercepted'
        assert process.wait(timeout=20)==2,'unresolved delivery must stop processing'
        assert state.exists() if commit_mode else not state.exists(),'diagnostic ordering must match the injected failure window'
        offsets=rpk('group','describe','nexstream.outage.v1')
        print('AFTER OUTAGE:',offsets)
        if not commit_mode:assert 'stable-message' not in (state.read_text() if state.exists() else '')
        if '--broker-restart' in sys.argv:
            run(['docker','stop','nexstream-s07-delivery-broker'])
            run(['docker','start','nexstream-s07-delivery-broker'])
            for _ in range(40):
                try:rpk('cluster','info');break
                except subprocess.CalledProcessError:time.sleep(.25)
        fault.clear()
        process=subprocess.Popen(command,stdout=subprocess.DEVNULL)
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            if state.exists() and (dlq_mode or 'stable-message' in state.read_text()):break
            time.sleep(.1)
        else:raise AssertionError('retry did not checkpoint unchanged identity')
        alerts=rpk('topic','consume','telemetry.dlq.v1' if dlq_mode else 'telemetry.alerts.v1','-n','1' if dlq_mode else ('4' if commit_mode else '2'),'-f','%v\n')
        records=[json.loads(line) for line in alerts.splitlines()]
        if dlq_mode:
            assert len(records)==1 and records[0]['source_offset']==0 and records[0]['source_partition']==0
            print('PASS DLQ transport outage: input uncommitted; broker/consumer restart retries same source offset; oversized=',oversized_mode)
        elif commit_mode:
            from collections import Counter
            assert len(records)==4 and sorted(Counter(r['alert_id'] for r in records).values())==[2,2]
            print('PASS OffsetCommit transport failure: confirmed outputs and diagnostic survive; bounded exit, uncommitted input retries duplicate stable IDs')
        else:
            assert len(records)==2 and {r['metric'] for r in records}=={'cpu','ram'}
            assert all(r['message_id']=='stable-message' for r in records)
            print('PASS alert transport outage: no checkpoint/commit before confirmed delivery; restart retries original identity')
        if '--trim-history' in sys.argv:
            process.terminate();assert process.wait(timeout=15)==0
            # Destructive fault is scoped to this script's ephemeral broker/topic, removed in finally.
            rpk('topic','trim-prefix','telemetry.events.v1','-p','0','-o','1','--no-confirm')
            process=subprocess.Popen(command,stdout=subprocess.DEVNULL)
            assert process.wait(timeout=15)==1
            print('PASS missing retained prefix: partition recovery fails closed instead of silently resetting state')
finally:
    if process and process.poll() is None:process.terminate();process.wait(timeout=15)
    stopped.set();listener.close()
    if created:subprocess.run(['docker','rm','-f','-v','nexstream-s07-delivery-broker'],capture_output=True,timeout=30)
