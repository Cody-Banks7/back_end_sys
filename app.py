from flask import Flask, request
import os
import subprocess
import re

app = Flask(__name__)


# @app.route('/')
# def hello_world():
#     return 'Hello World!'

@app.route('/', methods=["GET", "POST"])
def test():
    # basic_para = request.values.get('basic_para')
    global Exestr
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

    # get yarn
    app_id = ""
    for s in bytes.decode(out):
        if s.startswith("application id:"):
            app_id = s.strip().split(':')[1]
    yarn_command = "yarn logs -applicationId application_" + app_id + " > vis4GHive.log"
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
                vertex_id = res[0][0]
                if vertex_id not in vertex_list:
                    vertex_list.append(vertex_id)

    # add into ignore
    subprocess.Popen(["hadoop fs -cat /tmp/plan/ignore_vertices.txt"], shell=True, stdout=subprocess.PIPE)
    text = "{\n"
    for vertex in vertex_list:
        text += "\""+vertex+"\": false,\n"
    text += "}"
    file=open("/tmp/plan/ignore_vertices.txt",mode='w')
    file.write(text)

    #repalce jar
    subprocess.Popen(["cp ~/experiment/hive-exec-3.1.0.jar.hive ~/env/hive-3.1.0/lib/hive-exec-3.1.0.jar"], shell=True, stdout=subprocess.PIPE)
    subprocess.Popen(["cp hive-cli-3.1.0.jar.nowrite"], shell=True, stdout=subprocess.PIPE)

    #run hive
    ret = subprocess.Popen([app_str], shell=True, stdout=subprocess.PIPE)
    (out, err) = ret.communicate()
    print(bytes.decode(out))

    return "success"


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)
