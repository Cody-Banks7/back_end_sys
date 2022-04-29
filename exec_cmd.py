import os
import subprocess

from parse_dag import get_op_tree, get_frontier_op, get_dag
from parse_operator import get_operator_time, add_time
from parse_task import parse_task_logs
import json
import time
import re

HIVE_HOME = "/home/hive/env/hive-3.1.0"

def exec_cmd(hive_command):
    # Replace the hive-exec Jar for using GHive
    os.system(str.format("cp jars/hive-exec-3.1.0.jar.ghive {}/lib/hive-exec-3.1.0.jar", HIVE_HOME))
    print("Run GHive with Command:", hive_command)
    ret = subprocess.Popen([hive_command], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    print("GHive Result:", bytes.decode(out))

    # Get GHive ApplicationId
    ghive_app_id = ""
    for s in bytes.decode(out).split('\n'):
        if s.startswith("applicationId:"):
            ghive_app_id = s.strip().split(':')[1].strip()
    print("GHive ApplicationId:", ghive_app_id)

    # Replace the hive-exec Jar for using Hive
    os.system(str.format("cp jars/hive-exec-3.1.0.jar.hive {}/lib/hive-exec-3.1.0.jar", HIVE_HOME))
    print("Run Hive with Command:", hive_command)
    ret = subprocess.Popen([hive_command], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    print("Hive Result:", bytes.decode(out))

    # Get Hive ApplicationId
    hive_app_id = ""
    for s in bytes.decode(out).split('\n'):
        if s.startswith("applicationId:"):
            hive_app_id = s.strip().split(':')[1].strip()
    print("Hive ApplicationId:", hive_app_id)

    # Sleep several seconds waiting for log aggregation.
    time.sleep(60)

    # Get the log for GHive.
    ghive_yarn_command = str.format("yarn logs -applicationId {}", ghive_app_id)
    ret = subprocess.Popen([ghive_yarn_command], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    ghive_log = bytes.decode(out)

    # Get the log for Hive.
    hive_yarn_command = str.format("yarn logs -applicationId {}", hive_app_id)
    ret = subprocess.Popen([hive_yarn_command], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    hive_log = bytes.decode(out)

    # Parse logs and generate the output file.
    parse_task_logs(ghive_log, hive_log)

    # get vertex
    task_re = re.compile('.*VertexName: (.*)Vertex.*, TaskAttemptID:(.*), processorName.*')
    vertex_list = []
    for line in ghive_log.split('\n'):
        res = task_re.findall(line)
        if res:
            vertex_id = res[0][0].split(",")[0]
            if vertex_id not in vertex_list:
                vertex_list.append(vertex_id)
    print(vertex_list)

    # get DAG
    os.remove("tmp/dag.txt")
    os.system("hadoop fs -get /tmp/plan/dag.txt tmp/")
    f = open('tmp/dag.txt', 'r')
    dag = json.load(f)

    get_dag(dag)  # 拿到对应的边
    # 和vertex_list拼成一个文件可以直接扔可视化

    op_tree = get_op_tree(dag)

    frontier_op = get_frontier_op(op_tree)

    # add time for ghive
    ghive_op_time = get_operator_time(input_log=ghive_log)
    for item in vertex_list:
        add_time(op_tree[item], ghive_op_time, True)


    # add time for hive
    hive_op_time = get_operator_time(input_log=hive_log)
    print("hive_op_time:", hive_op_time)
    print("frontier_op:", frontier_op)
    for vertex, op in frontier_op.items():
        if vertex in hive_op_time:
            if op not in hive_op_time:
                hive_op_time[op] = 0
                print(op, "not in hive_op_time")
            hive_op_time[op] += hive_op_time[vertex]

    for item in vertex_list:
        add_time(op_tree[item], hive_op_time, False)

    with open('output/op_trees.json', 'w') as f:
        f.write(json.dumps(op_tree))


if __name__ == '__main__':
    exec_cmd(str.format("hive --database {} -e \"source /home/hive/queries/ssb/{}.sql;\"", "ssb_7_orc", "Q4.3"))

