import re

op_re = re.compile("Operator \\[(.*)\\] takes time: ([0-9]+)")
sort_re = re.compile(" (Reducer.*): done sorting span.*time=([0-9]+)")


def trim(s):
    res = re.findall("\\[(.*)]", s)
    if res:
        return res[0]
    return s


def task_line_filter(line_string: str):
    if re.search('Operator.*takes time:.*', line_string):
        return True
    if re.search('.*done sorting span.*time=.*', line_string):
        return True
    return False


def get_operator_time(input_log: str):
    op_time = {}
    for line in input_log.split('\n'):
        if not task_line_filter(line):
            continue
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


def add_time(op: {}, op_time: {}, gpu: bool):
    if gpu:
        if trim(op['str']) in op_time:
            op['gpu_time'] = op_time[trim(op['str'])]
    else:
        if trim(op['str']) in op_time:
            op['cpu_time'] = op_time[trim(op['str'])]
    if 'children' in op:
        for child in op['children']:
            add_time(child, op_time, gpu)