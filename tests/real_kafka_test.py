"""Explicit real Redpanda test; requires nexstream-s07-redpanda container."""
import json
import pathlib
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
def docker(*args, **kwargs):
    return subprocess.run(['docker', 'exec', '-i', 'nexstream-s07-redpanda', 'rpk', *args],
                          check=True, capture_output=True, text=True, timeout=20, **kwargs).stdout

with tempfile.TemporaryDirectory(prefix='nexstream-s07-') as directory:
    checkpoint = pathlib.Path(directory) / 'state'
    group = 'nexstream.processor.restart.' + str(time.time_ns())
    host = 'restart-' + str(time.time_ns())
    command = [str(ROOT / 'build-s07-Debug/nexstream-processor'), '127.0.0.1:19092', group, '80', '80', '2', str(checkpoint)]
    event = dict(schema_version=1, host_id=host, message_id='first', sample_timestamp_ms=int(time.time()*1000),
                 cpu_usage_percent=99, memory_used_bytes=99, memory_total_bytes=100)
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL)
    try:
        docker('topic', 'produce', 'telemetry.events.v1', '-k', host, input=json.dumps(event)+'\n')
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if checkpoint.exists() and host in checkpoint.read_text():
                break
            time.sleep(.1)
        else:
            raise AssertionError('first sample not checkpointed')
        process.terminate(); assert process.wait(timeout=10) == 0
        reader = subprocess.Popen(['docker', 'exec', 'nexstream-s07-redpanda', 'rpk',
            'topic', 'consume', 'telemetry.alerts.v1', '-o', 'end', '-n', '2', '-f', '%v\n'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(1)
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL)
        event.update(message_id='second', sample_timestamp_ms=event['sample_timestamp_ms']+1000)
        docker('topic', 'produce', 'telemetry.events.v1', '-k', host, input=json.dumps(event)+'\n')
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if 'second' in checkpoint.read_text():
                break
            time.sleep(.1)
        else:
            raise AssertionError('second sample not checkpointed')
        output, error = reader.communicate(timeout=20)
        assert reader.returncode == 0, error
        alerts = [json.loads(line) for line in output.splitlines()]
        actual = [a for a in alerts if a['host_id'] == host]
        assert {a['metric'] for a in actual} == {'cpu', 'ram'}, output
        print('PASS real broker: checkpoint/restart/consecutive CPU+RAM/delivery before commit')
    finally:
        if process.poll() is None:
            process.terminate(); process.wait(timeout=10)
