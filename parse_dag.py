import json
import re


def get_sub_op_tree(li: list, v_op_tree):
    for vertex in li:
        v_op_tree[vertex['str']] = vertex['subTree'][0]
        if 'children' in vertex:
            get_sub_op_tree(vertex['children'], v_op_tree)

def get_op_tree(li: list):
    v_op_tree = {}
    get_sub_op_tree(li, v_op_tree)
    return v_op_tree
    # with open("output/operator_trees.json") as f:
    #     f.write(json.dumps(v_op_tree))

def get_frontier_op_rec(sub_tree, frontier_op):
    # print(sub_tree['str'])
    if re.match("Map [0-9]+", sub_tree["str"]) or re.match("Reducer [0-9]+", sub_tree["str"]):
        res = re.findall("\\[(.*)]", sub_tree["parentId"])
        if res:
            frontier_op[sub_tree["str"]] = res[0]
    else:
        if "children" in sub_tree:
            for child in sub_tree["children"]:
                get_frontier_op_rec(child, frontier_op)


def get_frontier_op(v_op_tree):
    frontier_op = {}
    for vertex_name, tree in v_op_tree.items():
        get_frontier_op_rec(tree, frontier_op)
    return frontier_op


def get_sub_dag(li: list, edges: list):
    for vertex in li:
        edge = {}
        edge['src'] = vertex['str']
        if 'parentId' in vertex:
            edge['dest'] = vertex['parentId']
            edges.append(edge)
        if 'children' in vertex:
            get_sub_dag(vertex['children'], edges)

def get_dag(li: list):
    edges = []
    get_sub_dag(li, edges)
    with open("output/dag.json", "w") as f:
        f.write(json.dumps(edges))

