import httplib
import time

conn = httplib.HTTPConnection('localhost:6011')
conn.request('PUT', '/device/partition/account/container')
resp = conn.getresponse()
print resp.status

conn = httplib.HTTPConnection('localhost:6011')
conn.request('PUT', '/device/partition/account/container/file1', '',
    {'X-Timestamp': '%f' % time.time(), 'X-Size': '10', 'X-Content-Type': 'text/plain', 'X-Etag': '1234567890abcdef'})
resp = conn.getresponse()
print resp.status

conn = httplib.HTTPConnection('localhost:6011')
conn.request('PUT', '/device/partition/account/container/file2', '',
    {'X-Timestamp': '%f' % time.time(), 'X-Size': '12', 'X-Content-Type': 'something/else', 'X-Etag': '1234567890abcdef'})
resp = conn.getresponse()
print resp.status

conn = httplib.HTTPConnection('localhost:6011')
conn.request('GET', '/device/partition/account/container')
resp = conn.getresponse()
print resp.status
print resp.read()

conn = httplib.HTTPConnection('localhost:6011')
conn.request('DELETE', '/device/partition/account/container/file2', '',
    {'X-Timestamp': '%f' % time.time()})
resp = conn.getresponse()
print resp.status

conn = httplib.HTTPConnection('localhost:6011')
conn.request('GET', '/device/partition/account/container')
resp = conn.getresponse()
print resp.status
print resp.read()
