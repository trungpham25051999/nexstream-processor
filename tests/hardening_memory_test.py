"""Bounded malformed/oversized input on real S07H broker; process RSS is observed."""
import json
import pathlib
import subprocess
import tempfile
import threading
import time
ROOT=pathlib.Path(__file__).resolve().parents[1]
def rpk(*args,input=None):return subprocess.run(['docker','exec','-i','nexstream-s07h-kafka','rpk',*args,'-X','brokers=redpanda:9092'],input=input,text=True,capture_output=True,check=True,timeout=30).stdout
def watermark(topic):return {p['partition']:p['high_watermark'] for p in json.loads(rpk('topic','describe',topic,'-p','--format','json'))[0]['partitions']}
def committed(group):
    offsets={}
    for line in rpk('group','describe',group).splitlines():
        row=line.split()
        if len(row)>3 and row[0]=='telemetry.events.v1' and row[2].isdigit():offsets[int(row[1])]=int(row[2])
    return offsets
def caught_up(group,timeout=35):
    target=watermark('telemetry.events.v1');deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        current=committed(group)
        if all(current.get(p)==v for p,v in target.items()):return
        time.sleep(.1)
    raise AssertionError('consumer did not finish bounded input')
with tempfile.TemporaryDirectory(prefix='nexstream-malformed-') as folder:
    log=pathlib.Path(folder)/'worker.log';handle=log.open('w');group='nexstream.memory.'+str(time.time_ns())
    process=subprocess.Popen([str(ROOT/'build-s07-Debug/nexstream-processor'),'127.0.0.1:29093',group,'80','80','2',str(pathlib.Path(folder)/'state')],stdout=handle,stderr=handle)
    samples=[];finished=threading.Event()
    def rss():
        text=pathlib.Path(f'/proc/{process.pid}/status').read_text()
        return int(next(line.split()[1] for line in text.splitlines() if line.startswith('VmRSS:')))*1024
    def monitor():
        while not finished.wait(.01):
            try:samples.append(rss())
            except (FileNotFoundError,StopIteration):return
    try:
        caught_up(group);baseline=rss();monitor_thread=threading.Thread(target=monitor);monitor_thread.start()
        before=watermark('telemetry.dlq.v1')[0]
        # A 60 KiB deeply nested record must hit parser depth bound before recursion/heap growth.
        nested='['*30000+'0'+']'*30000
        oversized=json.dumps({'padding':'x'*262144})
        payload=(nested+'\n')*100+(oversized+'\n')*20
        rpk('topic','produce','telemetry.events.v1','-p','0','-k','invalid',input=payload)
        caught_up(group)
        assert watermark('telemetry.dlq.v1')[0]-before==120
        assert process.poll() is None
        peak=max(samples+[rss()]);assert peak-baseline<64*1024*1024
        result={'malformed_records':100,'malformed_record_bytes':len(nested),'oversized_records':20,
                'oversized_record_bytes':len(oversized),'baseline_rss_bytes':baseline,'peak_rss_bytes':peak,
                'rss_growth_limit_bytes':64*1024*1024,'dlq_records':120}
        print('PASS bounded malformed/oversized input:',json.dumps(result))
        output=ROOT/'tests/results/2026-11-22-sprint-07-hardening/memory.json';output.write_text(json.dumps(result,indent=2))
        subprocess.run(['docker','stop','nexstream-s07h-kafka'],check=True,capture_output=True,timeout=30)
        subprocess.run(['docker','start','nexstream-s07h-kafka'],check=True,capture_output=True,timeout=30)
        for _ in range(50):
            try:rpk('cluster','info');break
            except subprocess.CalledProcessError:time.sleep(.1)
        rpk('topic','produce','telemetry.events.v1','-p','1','-k','invalid',input='invalid-after-restart\n')
        caught_up(group);assert process.poll() is None
        print('PASS real broker restart: consumer rejoins and resumes committed partitions')
        start=time.monotonic();process.terminate();assert process.wait(timeout=15)==0
        print('PASS bounded shutdown elapsed_ms=',round((time.monotonic()-start)*1000,3))
    finally:
        finished.set()
        if 'monitor_thread' in locals():monitor_thread.join(timeout=2)
        if process.poll() is None:process.terminate();process.wait(timeout=15)
        handle.close();print('WORKER LOG');print(log.read_text())
