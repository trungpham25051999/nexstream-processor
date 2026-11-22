"""Configuration rejection and a bounded initial group-assignment failure."""
import pathlib
import socket
import subprocess
import tempfile
import time

ROOT=pathlib.Path(__file__).resolve().parents[1]
BINARY=str(ROOT/'build-s07-Debug/nexstream-processor')
with tempfile.TemporaryDirectory(prefix='nexstream-deadline-') as folder:
    state=str(pathlib.Path(folder)/'state')
    for cpu,count in [('80oops','2'),('80','4294967296')]:
        result=subprocess.run([BINARY,'127.0.0.1:1','nexstream.invalid',cpu,'80',count,state],text=True,capture_output=True,timeout=5)
        assert result.returncode==1 and 'invalid numeric configuration' in result.stderr
    print('PASS numeric configuration is parsed completely without unsigned narrowing/wrap')
    # Reserved but non-listening loopback port: no external service can race this fixture.
    with socket.socket() as unavailable:
        unavailable.bind(('127.0.0.1',0));port=unavailable.getsockname()[1]
        start=time.monotonic()
        result=subprocess.run([BINARY,f'127.0.0.1:{port}','nexstream.deadline','80','80','2',state],text=True,capture_output=True,timeout=42)
        elapsed=time.monotonic()-start
        assert result.returncode==1 and 'consumer assignment budget exceeded' in result.stderr
        assert 30<=elapsed<=42
        print('PASS initial assignment/reconnect/shutdown budget elapsed_seconds=',round(elapsed,3))
        print(result.stderr)
