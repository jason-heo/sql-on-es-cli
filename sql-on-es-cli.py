#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
import readline
import os
import json
import httplib
import urlparse
import urllib

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


class OutputMode:
    TABLE = 1
    JSON = 2


class JsonOutput:
    @staticmethod
    def emit(json_obj):
        print json.dumps(json_obj, indent = 2)


class TableOutput:
    @staticmethod
    def emit(json_obj):
        if "aggregations" in json_obj:
            TableOutput.print_aggr_output(json_obj)
        else:
            TableOutput.print_docs_output(json_obj)
    
    @staticmethod
    def print_aggr_output(json_obj):
        pass

    @staticmethod
    def print_docs_output(json_obj):
        field_len_map = TableOutput.get_field_len_map(json_obj)
        TableOutput.print_header(field_len_map)
        TableOutput.print_data(field_len_map, json_obj)

    @staticmethod
    def get_field_len_map(json_obj):
        ret_val = {}

        # value들 간의 최대 문자열 길이
        for doc in json_obj["hits"]["hits"]:
            source = doc["_source"]
            for field in source:
                if field not in ret_val:
                    ret_val[field] = {}

                    # value 길이와 field 길의 중 max 값
                    ret_val[field]['len'] = max(len(str(source[field])), len(field))
                    ret_val[field]['is_str'] = type(source[field]) == str
                else:
                    if ret_val[field] < len(str(source[field])):
                        ret_val[field]['len'] = len(str(source[field]))
        return ret_val

    @staticmethod
    def print_header(field_len_map):
        for k in field_len_map:
            sys.stdout.write(("| %-" + str(field_len_map[k]["len"]) + "s ") % k)
        sys.stdout.write("|\n")

        for k in field_len_map:
            sys.stdout.write(("|%" + str(field_len_map[k]["len"]) + "s") % ("-" * (field_len_map[k]["len"] + 2)))
        sys.stdout.write("|\n")

    @staticmethod
    def print_data(field_len_map, json_obj):
        for doc in json_obj["hits"]["hits"]:
            source = doc["_source"]
            for k in field_len_map:
                TableOutput.print_field(field_len_map, k, source) 
            sys.stdout.write("|\n")

    @staticmethod
    def print_field(field_len_map, k, source):
        str_format = TableOutput.get_str_format(field_len_map[k])
        
        v = ""
        if k in source:
            v = source[k]
        sys.stdout.write(str_format % v)

    @staticmethod
    def get_str_format(field_info):
        # 문자열은 left align, 그외는 right align
        align_sign = ""
        if field_info['is_str'] == True:
            align_sign = "-"

        return "| %" + align_sign + str(field_info["len"]) + "s "
        
class SQLExecutor:
    def __init__(self, endpoint):
        self.http_client = HttpClient(endpoint)
        self.output_mode = OutputMode.TABLE
        # self.output_mode = OutputMode.JSON
    
    def run(self, sql):
        if sql.strip() == "": return

        json_obj = self.http_client.get(sql)
        
        if self.output_mode == OutputMode.TABLE:
            TableOutput.emit(json_obj)
        else:
            JsonOutput.emit(json_obj)


executor = SQLExecutor(endpoint)

while True:
    try:
        sql = raw_input('SQL> ')
        executor.run(sql)
    except (EOFError):
        break

print "\nexiting..."
