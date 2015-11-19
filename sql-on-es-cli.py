#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import readline
import os
import json
import httplib
import urlparse
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')

def print_usage():
        sys.stderr.write("Usage: {0} <endpoint> [-j]\n".format(sys.argv[0]))
        sys.stderr.write("   -j: print Json Format rather than TABLE\n")
        sys.stderr.write("\nEx) {0} http://localhost:9200\n".format(sys.argv[0]))
        sys.exit(1)

hist_file = os.path.join(os.path.expanduser("~"), ".sql-on-es.hist")

try:
    readline.read_history_file(hist_file)
except (IOError):
    pass

import atexit
atexit.register(readline.write_history_file, hist_file)

import getopt
def parseopt():
    try:
        optlist, args = getopt.getopt(' '.join(sys.argv[1:]).split(), 'j')

        if len(args) == 0:
            print_usage()
            sys.exit(1)
    except getopt.GetoptError as err:
        sys.stderr.write("Error: " + str(err) + "\n\n")
        print_usage()
        sys.exit(1)
    
    output_mode = OutputMode.TABLE
    
    if '-j' in dict(optlist):
        output_mode = OutputMode.JSON
    return args[0], output_mode

class HttpClient:
    def __init__(self, endpoint):
        o = urlparse.urlparse(endpoint)

        self.protocol = o.scheme
        self.hostname = o.hostname
        self.port = o.port
        
        if self.protocol == "" or self.hostname == "":
            sys.stderr.write("Error: invalid url. '" + endpoint + "'\n")
            sys.exit(1)
    
    def get(self, sql):
        try:
            conn = httplib.HTTPConnection(self.hostname, self.port)
            conn.request("GET", "/_sql?sql=" + urllib.quote(sql))

            response = conn.getresponse()
            
            return json.loads(response.read())
        except Exception as e:
            sys.stderr.write("Http Error: %s\n" % str(e))
            sys.exit(1)


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
    def emit(json_obj):
        output = []
        field_order = []
        if "aggregations" in json_obj:
            (output, field_order) = TableOutput.print_aggr_output(json_obj)
        else:
            (output, field_order) = TableOutput.print_normal_output(json_obj)

        TableOutput.print_output(output, field_order)
        TableOutput.print_meta(json_obj, len(output))

    @staticmethod
    def print_meta(json_obj, row_count):
        took = float(json_obj["took"]) / 1000
        print "\n%d rows printed" % row_count
        hitted_doc_count = json_obj["hits"]["total"]
        print "%d docs hitted (%.3f sec)\n" % (hitted_doc_count, took)

    @staticmethod
    def print_aggr_output(json_obj):
        aggr_output = []
        field_order = []

        TableOutput.agg_output2arr(json_obj["aggregations"],
                                   aggr_output,
                                   field_order)
        return (aggr_output, field_order)

    @staticmethod
    # output: array of dict
    def print_output(aggr_output, field_order):
        field_name_len_map = TableOutput.get_field_len_map(aggr_output,
                                                             field_order)
        
        TableOutput.print_header(field_order, field_name_len_map)
        TableOutput.print_data(aggr_output, field_order, field_name_len_map)

    @staticmethod
    def get_field_len_map(aggr_output, field_order):
        field_name_len_map = {}
        for field in field_order:
            field_name_len_map[field] = len(field)

        TableOutput.set_field_name_len_map(aggr_output, field_name_len_map)

        return field_name_len_map
    
    @staticmethod
    def print_header(field_order, field_name_len_map):
        # print |field1|field2|field2|...|fieldn|
        for k in field_order:
            sys.stdout.write(("| %-" + str(field_name_len_map[k]) + "s ") % k)
        sys.stdout.write("|\n")
        
        # print |---|---|----|...|---|
        for k in field_order:
            sys.stdout.write(("|%" + str(field_name_len_map[k]) + "s") % ("-" * (field_name_len_map[k] + 2)))
        sys.stdout.write("|\n")
    
    @staticmethod
    def print_data(aggr_output, field_order, field_name_len_map):
        for arr in aggr_output:
            for field in field_order:
                sys.stdout.write(("| %"+str(field_name_len_map[field])+"s ") % str(arr[field]))
            sys.stdout.write("|\n")

    @staticmethod
    def set_field_name_len_map(output, field_name_len_map):
        for arr in output:
            for k in arr:
                field_name_len_map[k] = max(field_name_len_map[k],
                                            len(str(arr[k])))

    @staticmethod
    def agg_output2arr(aggr, docs, field_order):
        field_hash = {}
        grp_by_fld_info = {} # contains group by field
        TableOutput.visit_aggr_node(aggr,
                                    docs,
                                    grp_by_fld_info,
                                    field_order,
                                    field_hash)
    
    @staticmethod
    def visit_aggr_node(aggr, docs, grp_by_fld_info, field_order, field_hash):
        for grp_field in aggr: # "aggregations"->"grp_field"
            if grp_field == "key" or grp_field == "doc_count":
                continue
            for grp_doc in aggr[grp_field]["buckets"]: #"grp_field"->"buckets"[]
                if grp_field not in field_hash:
                    field_order.append(grp_field)
                    field_hash[grp_field] = True
                grp_by_fld_info[grp_field] = grp_doc['key']
                aggr_found = False
                for sub_key in grp_doc:
                    if sub_key != "key" and sub_key != "doc_count":
                        # sub level group by field or actual value
                        if "buckets" in grp_doc[sub_key]:
                            TableOutput.visit_aggr_node(grp_doc,
                                                        docs,
                                                        grp_by_fld_info,
                                                        field_order,
                                                        field_hash)
                        else:
                            aggr_found = True
                            if sub_key not in field_hash:
                                field_order.append(sub_key)
                                field_hash[sub_key] = True
                            grp_by_fld_info[sub_key] = grp_doc[sub_key]["value"]
    
                if aggr_found:
                    docs.append(grp_by_fld_info.copy())

    @staticmethod
    def print_normal_output(json_obj):
        if len(json_obj["hits"]["hits"]) == 0:
            print "Empty set\n"
            return
        
        normal_output = []
        field_order = []    # field 출력 순서.
        
        field_order_made = False

        for doc in json_obj["hits"]["hits"]:
            row = {}
            if TableOutput.print_id_type is True:
                row["_id"] = doc["_id"]
                row["_type"] = doc["_type"]

                if field_order_made == False:
                    field_order.append("_id")
                    field_order.append("_type")

            row.update(doc["_source"])
            
            if field_order_made == False:
                field_order += doc["_source"].keys()
                field_order_made = True

            normal_output.append(row)

        return (normal_output, field_order)
        
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

class SQLExecutor:
    def __init__(self, endpoint, output_mode):
        self.http_client = HttpClient(endpoint)
        self.output_mode = output_mode
    
    def run(self, sql):
        if sql.strip() == "": return

        json_obj = self.http_client.get(sql)
        
        if json_obj.get("status", 200) != 200:
            print_es_error(json_obj)
            return

        if self.output_mode == OutputMode.TABLE:
            TableOutput.emit(json_obj)
        else:
            JsonOutput.emit(json_obj)

def print_es_error(json_obj):
    print "status: " + str(json_obj.get("status", 200))
    print "error: " + str(json_obj.get("error", ""))

if __name__ == "__main__":
    endpoint, output_mode = parseopt()

    executor = SQLExecutor(endpoint, output_mode)

    while True:
        try:
            sql = raw_input('SQL> ')
            executor.run(sql)
        except (EOFError):
            break

    print "\nexiting..."
