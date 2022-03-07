from flask import Flask, request

# from flask_cors import CORS
app = Flask(__name__)


# @app.route('/')
# def hello_world():
#     return 'Hello World!'

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
    return "success"





if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001)