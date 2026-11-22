"""Real two-partition fixture: nexstream-s07h-kafka; no production topics are touched."""
import collections
import json
import os
import pathlib
import re
import signal
import subprocess
import tempfile
import time

ROOT=pathlib.Path(__file__).resolve().parents[1]
BINARY=pathlib.Path(os.environ.get('NEXSTREAM_PROCESSOR_BINARY',str(ROOT/'build-s07-Debug/nexstream-processor')))
def rpk(*args,input=None):
    p=subprocess.run(['docker','exec','-i','nexstream-s07h-kafka','rpk',*args,'-X','brokers=redpanda:9092'],input=input,text=True,capture_output=True,check=True,timeout=20)
    return p.stdout
def watermarks(topic):
    return {p['partition']:p['high_watermark'] for p in json.loads(rpk('topic','describe',topic,'-p','--format','json'))[0]['partitions']}
def until(predicate,seconds=25):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        if predicate():return
        time.sleep(.1)
    raise AssertionError('condition deadline exceeded')
def committed(group):
    result={}
    for line in rpk('group','describe',group).splitlines():
        row=line.split()
        if len(row)>3 and row[0]=='telemetry.events.v1' and row[2].isdigit():result[int(row[1])]=int(row[2])
    return result
def caught_up(group):
    wanted=watermarks('telemetry.events.v1')
    until(lambda:all(committed(group).get(p)==v for p,v in wanted.items()))
def produce(partition,host,message):
    event=dict(schema_version=1,host_id=host,message_id=message,sample_timestamp_ms=int(time.time()*1000),cpu_usage_percent=99,memory_used_bytes=99,memory_total_bytes=100)
    rpk('topic','produce','telemetry.events.v1','-p',str(partition),'-k',host,input=json.dumps(event)+'\n')
def alerts(host):
    count=sum(watermarks('telemetry.alerts.v1').values())
    if not count:return []
    records=rpk('topic','consume','telemetry.alerts.v1','-o','start','-n',str(count),'-f','%v\n')
    return [a for a in map(json.loads,records.splitlines()) if a['host_id']==host]
def assignments(path):
    owned=set()
    for line in path.read_text().splitlines():
        if line.startswith('REVOKED'):owned.clear()
        match=re.match(r'ASSIGNED telemetry.events.v1 (\d+)',line)
        if match:owned.add(int(match[1]))
    return owned

with tempfile.TemporaryDirectory(prefix='nexstream-rebalance-') as folder:
    directory=pathlib.Path(folder);processes=[];handles=[]
    def start(group,name,cpu='80',count='2'):
        log=directory/(name+'.log');handle=log.open('w');handles.append(handle)
        process=subprocess.Popen([str(BINARY),'127.0.0.1:29093',group,cpu,'80',count,str(directory/(name+'.state'))],stdout=handle,stderr=handle)
        processes.append(process);return process,log
    try:
        group='nexstream.hardening.'+str(time.time_ns());prefix='rebalance-'+str(time.time_ns())
        a,alog=start(group,'a');until(lambda:assignments(alog)=={0,1})
        for p in (0,1):produce(p,prefix+str(p),'first')
        caught_up(group)
        b,blog=start(group,'b')
        until(lambda:assignments(alog) in ({0},{1}) and assignments(blog) in ({0},{1}) and assignments(alog)!=assignments(blog))
        for p in (0,1):produce(p,prefix+str(p),'second')
        caught_up(group)
        for p in (0,1):assert {r['metric'] for r in alerts(prefix+str(p))}=={'cpu','ram'}
        print('PASS two instances: partition rebalance preserves consecutive threshold state')
        victim=a if 0 in assignments(alog) else b
        survivor=b if victim is a else a;survivor_log=blog if victim is a else alog
        victim.kill();victim.wait(timeout=10)
        until(lambda:assignments(survivor_log)=={0,1})
        for p in (0,1):produce(p,prefix+str(p),'third')
        caught_up(group)
        for p in (0,1):
            records=alerts(prefix+str(p));assert len(records)==4 and len({r['alert_id'] for r in records})==4
        extra1,extra1_log=start(group,'extra1');extra2,extra2_log=start(group,'extra2')
        def three_ready():
            paths=(survivor_log,extra1_log,extra2_log);owned=[assignments(path) for path in paths]
            return (all(process.poll() is None for process in (survivor,extra1,extra2)) and
                    sorted(map(len,owned))==[0,1,1] and set().union(*owned)=={0,1} and
                    any('ASSIGNED idle' in path.read_text() for path in paths) and
                    re.search(r'STATE\s+Stable',rpk('group','describe',group)))
        until(three_ready)
        for p in (0,1):produce(p,prefix+str(p),'fourth')
        caught_up(group)
        for p in (0,1):assert len(alerts(prefix+str(p)))==6
        for extra in (extra1,extra2):extra.terminate();assert extra.wait(timeout=10)==0
        print('PASS three instances/two partitions: idle group member remains healthy and active partitions advance')
        survivor.terminate();assert survivor.wait(timeout=10)==0
        print('PASS SIGKILL owner: other instance reconstructs committed partition state without duplicate committed alerts')
        wrong,wrong_log=start(group,'wrong',cpu='81')
        assert wrong.wait(timeout=15)==1 and 'metadata mismatch' in wrong_log.read_text()
        print('PASS changed algorithm/config fails closed against committed group metadata')

        crash_group='nexstream.crash.'+str(time.time_ns());crash_host='crash-'+str(time.time_ns())
        crash,crash_log=start(crash_group,'crash',count='1');caught_up(crash_group)
        before=committed(crash_group).get(0,0)
        tmp=directory/'crash.state.tmp';tmp.symlink_to('/dev/full')
        produce(0,crash_host,'unchanged-id')
        assert crash.wait(timeout=15)==1 and 'checkpoint write' in crash_log.read_text()
        assert committed(crash_group).get(0,0)==before
        tmp.unlink()
        retry,retry_log=start(crash_group,'retry',count='1');caught_up(crash_group)
        records=alerts(crash_host);counts=collections.Counter(r['alert_id'] for r in records)
        assert len(records)==4 and sorted(counts.values())==[2,2]
        retry.terminate();assert retry.wait(timeout=10)==0
        print('PASS output-success/checkpoint-failure: input not committed; retry duplicates stable alert IDs (at least once)')
        print('PASS ownership group:',group)
    finally:
        for process in processes:
            if process.poll() is None:process.terminate();process.wait(timeout=15)
        for handle in handles:handle.close()
        for path in directory.glob('*.log'):
            print('WORKER LOG',path.name);print(path.read_text())
