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


def get_sub_dag(li: list, edges: list, edge_idx_map: dict):
    for vertex in li:
        edge = {}
        if vertex['str'] in edge_idx_map:
            edge['src'] =edge_idx_map[vertex['str']]
            if 'parentId' in vertex:
                edge['dst'] =edge_idx_map[vertex['parentId']]
                edges.append(edge)
            if 'children' in vertex:
                get_sub_dag(vertex['children'], edges,edge_idx_map)

def get_dag(li: list, ver_li: list):
    vertexes = []
    edge_idx_map = {}
    i = 0
    for item in ver_li:
        current_node = {'idx': i, 'vdat': {}}
        current_node['vdat']['vertex_name'] = item.strip()
        current_node['vdat']['hv_type'] = item.strip().split(' ')[0]
        edge_idx_map[item.strip()] = i
        vertexes.append(current_node)
        i += 1
    print('edge_idx_map:',edge_idx_map)
    edges = []
    get_sub_dag(li, edges, edge_idx_map)
    file_in = {}
    file_in['vertexes'] = vertexes
    file_in['edges'] = edges
    with open("output/dag.json", "w") as f:
        f.write(json.dumps(file_in))
