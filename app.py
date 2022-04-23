import json

from flask import Flask, request
import os
import subprocess
import re
import time

app = Flask(__name__)

op_re = re.compile("Operator \\[(.*)\\] takes time: ([0-9]+)")
sort_re = re.compile(" (Reducer.*): done sorting span.*time=([0-9]+)")


def get_time(input_path: str):
    op_time = {}
    with open(input_path) as f:
        for line in f:
            res = op_re.findall(line)
            if res:
                if res[0][0] not in op_time:
                    op_time[res[0][0]] = 0
                op_time[res[0][0]] = op_time[res[0][0]] + int(res[0][1])
            res = sort_re.findall(line)
            if res:
                # print(res)
                if res[0][0] not in op_time:
                    op_time[res[0][0]] = 0
                op_time[res[0][0]] = op_time[res[0][0]] + int(res[0][1])
    return op_time


def addTime(op: {}, op_time: {}):
    if op['str'] in op_time:
        print(op_time[op['str']])
        op['time'] = op_time[op['str']]
    if 'children' in op:
        for child in op['children']:
            addTime(child, op_time)


@app.route('/', methods=["GET", "POST"])
def test():
    # basic_para = request.values.get('basic_para')
    workerNum = request.values.get('workerNum')
    containerNum = request.values.get('containerNum')
    maxThread = request.values.get('maxThread')
    memPerThread = request.values.get('memPerThread')
    set_info = request.values.get('set_info')
    query_info = request.values.get('query_info')
    inputQuery = request.values.get('inputQuery')
    print(workerNum)
    print(containerNum)
    print(maxThread)
    print(memPerThread)
    print(set_info)
    print(query_info)
    print(inputQuery)

    app_str = "hive --database " + set_info + " -e \"source /home/hive/queries/ssb/" + query_info + ".sql;\""
    print(app_str)
    # ret=os.system(str)
    ret = subprocess.Popen([app_str], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    print(bytes.decode(out))

    # time.sleep(30)

    # get yarn
    app_id = ""
    out = bytes.decode(out).split('\n')
    for s in out:
        if s.startswith("applicationId:"):
            app_id = s.strip().split(':')[1].strip()
        if s.startswith("total-time:"):
            total_time = s.strip().split(':')[1].strip()
    yarn_command = "yarn logs -applicationId " + app_id + " > vis4GHive.log"
    ret = subprocess.Popen([yarn_command], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    print(bytes.decode(out))

    # get vertex
    task_re = re.compile('.*VertexName: (.*)Vertex.*, TaskAttemptID:(.*), processorName.*')
    vertex_list = []
    with open("vis4GHive.log") as f:
        for line in f:
            res = task_re.findall(line)
            if res:
                vertex_id = res[0][0].split(",")[0]
                if vertex_id not in vertex_list:
                    vertex_list.append(vertex_id)
    print(vertex_list)

    # add time for ghive
    tree_dict_ghive = {}
    for item in vertex_list:
        f = open('operator_' + item, 'r')
        tree_ghive = json.load(f)
        op_time = get_time('vis4GHive.log')
        # print(op_time)
        for op in tree_ghive:
            addTime(op, op_time)
        tree_dict_ghive[item] = tree_ghive

    # add into ignore
    subprocess.Popen(["rm  /tmp/plan/ignore_vertices.txt"], shell=True, stdout=subprocess.PIPE)
    text = "{\n"
    for vertex in vertex_list:
        text += "\"" + vertex + "\": false,\n"
    text += "}"
    file = open("/tmp/plan/ignore_vertices.txt", mode='w')
    file.write(text)

    # run hive
    ret = subprocess.Popen([app_str], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    print(bytes.decode(out))

    # time.sleep(30)

    # get yarn
    app_id = ""
    for s in bytes.decode(out):
        if s.startswith("applicationId:"):
            app_id = s.strip().split(':')[1].strip()
        if s.startswith("total-time:"):
            total_time = s.strip().split(':')[1].strip()
    yarn_command = "yarn logs -applicationId " + app_id + " > vis4Hive.log"
    ret = subprocess.Popen([yarn_command], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    print(bytes.decode(out))

    # add time for hive
    tree_dict_hive = {}
    for item in vertex_list:
        f = open('operator_' + item, 'r')
        tree_ghive = json.load(f)
        op_time = get_time('vis4Hive.log')
        # print(op_time)
        for op in tree_ghive:
            addTime(op, op_time)
        tree_dict_hive[item] = tree_ghive


    return "success"


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)
