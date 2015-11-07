#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
import readline
import os
import json

if len(sys.argv) == 1:
    sys.stderr.write("Usage: {0} <endpoint>\n\n".format(sys.argv[0]))
    sys.stderr.write("Ex) {0} http://localhost:9200\n".format(sys.argv[0]))
    sys.exit(1)

endpoint = sys.argv[1]

hist_file = os.path.join(os.path.expanduser("~"), ".sql-on-es.hist")

try:
    readline.read_history_file(hist_file)
except (IOError):
    pass

import atexit
atexit.register(readline.write_history_file, hist_file)

import httplib
import urlparse
import urllib

# readline.parse_and_bind('tab: complete')
# readline.parse_and_bind('set editing-mode emacs')

class HttpClient:
    def __init__(self, endpoint):
        o = urlparse.urlparse(endpoint)

        self.protocol = o.scheme
        self.hostname = o.hostname
        self.port = o.port
    
    def get(self, sql):

        conn = httplib.HTTPConnection(self.hostname, self.port)
        conn.request("GET", "/_sql?sql=" + urllib.quote(sql))

        response = conn.getresponse()

        return json.loads(response.read())

class SQLExecutor:
    def __init__(self, endpoint):
        self.http_client = HttpClient(endpoint)
    
    def run(self, sql):
        json_obj = self.http_client.get(sql)

        print json.dumps(json_obj, indent = 2)

executor = SQLExecutor(endpoint)

while True:
    try:
        sql = raw_input('SQL> ')
        executor.run(sql)
    except (EOFError):
        break

print "\nexit..."
