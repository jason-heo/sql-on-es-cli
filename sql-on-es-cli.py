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
    print_id_type = True
    id_max_len = 0
    type_max_len = 0

    @staticmethod
    def print_meta(json_obj):
        took = float(json_obj["took"]) / 1000
        doc_count = json_obj["hits"]["total"]
        print "\n%d doc in set (%.3f sec)\n" % (doc_count, took)

    @staticmethod
    def emit(json_obj):
        if "aggregations" in json_obj:
            TableOutput.print_aggr_output(json_obj)
        else:
            TableOutput.print_docs_output(json_obj)
        TableOutput.print_meta(json_obj)

    @staticmethod
    def print_aggr_output(json_obj):
        JsonOutput.emit(json_obj["aggregations"])

        grp_by_fld_info = [] # contains order of group by fields
        docs = []

        TableOutput.es_output2array(json_obj["aggregations"],
                                    docs,
                                    grp_by_fld_info)
    
    @staticmethod
    def es_output2array(aggr, docs, grp_by_fld_info):
        TableOutput.visit_aggr_node(aggr, docs, grp_by_fld_info)
    
    @staticmethod
    def visit_aggr_node(aggr, docs, grp_by_fld_info):
        for grp_field in aggr: # "aggregations"->"grp_field"
            if grp_field == "key" or grp_field == "doc_count":
                continue
            for grp_doc in aggr[grp_field]["buckets"]: #"grp_field"->"buckets"[]
                grp_by_fld_info.append({grp_field:grp_doc['key']})
                for sub_key in grp_doc:
                    if sub_key != "key" and sub_key != "doc_count":
                        # sub level group by field or actual value
                        if "buckets" in grp_doc[sub_key]:
                            TableOutput.visit_aggr_node(grp_doc,
                                                        docs,
                                                        grp_by_fld_info)
                        else:
                            print grp_by_fld_info
                            print sub_key, "=>", grp_doc[sub_key]["value"]
                print "======="
                grp_by_fld_info.pop()

    @staticmethod
    def print_docs_output(json_obj):
        if len(json_obj["hits"]["hits"]) == 0:
            print "Empty set\n"

        TableOutput.id_max_len = len("_id")
        TableOutput.type_max_len = len("_type")

        field_len_map = TableOutput.get_field_len_map(json_obj)

        if len(json_obj["hits"]["hits"]) > 0:
            TableOutput.print_header(field_len_map)
            TableOutput.print_data(field_len_map, json_obj)

    @staticmethod
    def get_field_len_map(json_obj):
        ret_val = {}

        # value들 간의 최대 문자열 길이
        for doc in json_obj["hits"]["hits"]:
            TableOutput.get_field_info(doc, ret_val)

        return ret_val
    
    @staticmethod
    def get_field_info(doc, ret_val):
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
        
        # need to store max len of _id and _type so that print _id, _type
        if TableOutput.print_id_type is True:
            TableOutput.id_max_len = max(TableOutput.id_max_len,
                                         len(str(doc["_id"])))

            TableOutput.type_max_len = max(TableOutput.type_max_len,
                                           len(str(doc["_type"])))


    @staticmethod
    def print_header(field_len_map):
       
        # print |_id|_type|field1|...|fieldn|
        if TableOutput.print_id_type is True:
            sys.stdout.write(("| %-" + str(TableOutput.id_max_len) + "s ") % "_id")
            sys.stdout.write(("| %-" + str(TableOutput.type_max_len) + "s ") % "_type")
        for k in field_len_map:
            sys.stdout.write(("| %-" + str(field_len_map[k]["len"]) + "s ") % k)
        sys.stdout.write("|\n")
        
        # print |---|----|...|---|
        if TableOutput.print_id_type is True:
            sys.stdout.write(("|%" + str(TableOutput.id_max_len) + "s") % ("-" * (TableOutput.id_max_len + 2)))
            sys.stdout.write(("|%" + str(TableOutput.type_max_len) + "s") % ("-" * (TableOutput.type_max_len + 2)))

        for k in field_len_map:
            sys.stdout.write(("|%" + str(field_len_map[k]["len"]) + "s") % ("-" * (field_len_map[k]["len"] + 2)))
        sys.stdout.write("|\n")

    @staticmethod
    def print_data(field_len_map, json_obj):
        for doc in json_obj["hits"]["hits"]:
            if TableOutput.print_id_type is True:
                TableOutput._print_id_type(doc)

            source = doc["_source"]
            for k in field_len_map:
                TableOutput.print_field(field_len_map, k, source) 
            sys.stdout.write("|\n")

    @staticmethod
    def _print_id_type(doc):
        id_format = TableOutput.get_str_format(TableOutput.id_max_len, True)
        type_format = TableOutput.get_str_format(TableOutput.type_max_len, True)
        
        sys.stdout.write(id_format % doc["_id"])
        sys.stdout.write(type_format % doc["_type"])
        
    @staticmethod
    def print_field(field_len_map, k, source):
        str_format = TableOutput.get_str_format(field_len_map[k]["len"],
                                                field_len_map[k]["is_str"])
        
        v = ""
        if k in source:
            v = source[k]
        sys.stdout.write(str_format % v)

    @staticmethod
    def get_str_format(value_len, is_str):
        # 문자열은 left align, 그외는 right align
        align_sign = ""
        if is_str == True:
            align_sign = "-"

        return "| %" + align_sign + str(value_len) + "s "
        
class SQLExecutor:
    def __init__(self, endpoint):
        self.http_client = HttpClient(endpoint)
        self.output_mode = OutputMode.TABLE
        # self.output_mode = OutputMode.JSON
    
    def run(self, sql):
        if sql.strip() == "": return

        json_obj = self.http_client.get(sql)
        
        if json_obj.get("status", 200) != 200:
            print_error(json_obj)
            return

        if self.output_mode == OutputMode.TABLE:
            TableOutput.emit(json_obj)
        else:
            JsonOutput.emit(json_obj)

def print_error(json_obj):
    print "status: " + str(json_obj.get("status", 200))
    print "error: " + json_obj.get("error", "")

executor = SQLExecutor(endpoint)

while True:
    try:
        sql = raw_input('SQL> ')
        executor.run(sql)
    except (EOFError):
        break

print "\nexiting..."
