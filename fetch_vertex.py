import json
def read_json_obj(filename: str):
    with open(filename, 'r', encoding='utf-8') as fp:
        json_obj = json.load(fp)
    return json_obj


def fetch_vertex(index):
    op_trees_all = read_json_obj("output/op_trees_all.json")
    with open('output/op_trees.json', 'w') as f:
        operator_tree_chosen = {'dag': op_trees_all[index]}
        f.write(json.dumps(operator_tree_chosen))
