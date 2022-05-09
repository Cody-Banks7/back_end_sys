import json

from flask import Flask, request, jsonify

import exec_cmd

from flask_cors import CORS
import fetch_vertex
app = Flask(__name__)
CORS(app)
global operator_chosen

def read_json_obj(filename: str):
    with open(filename, 'r', encoding='utf-8') as fp:
        json_obj = json.load(fp)
    return json_obj


@app.route('/api/dag/', methods=['GET'])
def get_dag():
    plan_obj = read_json_obj("output/dag.json")
    return jsonify(plan_obj), 200, {"Content-Type": "application/json"}


@app.route('/api/hive-tasks/', methods=['GET'])
def get_all_tasks():
    tasks_obj = read_json_obj("output/hive_tasks.json")
    print('get_all_tasks finished')
    return jsonify(tasks_obj), 200, {"Content-Type": "application/json"}


@app.route('/api/ghive-tasks/', methods=['GET'])
def get_all_tasks2():
    tasks_obj = read_json_obj("output/ghive_tasks.json")
    print('get_all_tasks finished')
    return jsonify(tasks_obj), 200, {"Content-Type": "application/json"}


@app.route('/api/operators/', methods=['GET'])
def get_all_tasks3():
    tasks_obj = read_json_obj("output/op_trees.json")
    print('get_all_tasks finished')
    return jsonify(tasks_obj), 200, {"Content-Type": "application/json"}

@app.route('/vertex_select', methods=['GET','POST'])
def get_vertex_select():
    vertex_index = request.values.get('index')
    global operator_chosen
    if vertex_index == operator_chosen:
        return "false"
    fetch_vertex.fetch_vertex(vertex_index)
    print('get_vertex_select finished')
    operator_chosen=vertex_index
    return "success"

@app.route('/', methods=["GET", "POST"])
def test():
    # basic_para = request.values.get('basic_para')
    set_info = request.values.get('set_info')
    query_info = request.values.get('query_info')
    inputQuery = request.values.get('inputQuery')
    print(set_info)
    print(query_info)
    print(inputQuery)
    hive_command = "hive --database " + set_info + " -e \"source /home/hive/queries/ssb/" + query_info + ".sql;\""

    # Execute, generate output files to output/ directory.
# exec_cmd.exec_cmd(hive_command)
 
    # Execute, generate output files to output/ directory.
    global operator_chosen
    operator_chosen=exec_cmd.exec_cmd(hive_command)

    return "success"


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)
