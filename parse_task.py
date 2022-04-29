import json
import re

task_re = re.compile('.*VertexName: (.*), .*, TaskAttemptID:(.*), processorName.*')
start_reading = re.compile('Profiling: Tez Container task starting at time ([0-9]+) ms')
end_reading = re.compile('Profiling: Tez \'Shuffle\' on .* stage ending at time ([0-9]+) .*s')
end_reading_map = re.compile('Profiling: MRReaderMapred has no next data time: ([0-9]+) .*s')
start_processing = re.compile('Tez \'Processor\' on .* stage starting at time ([0-9]+) .*s')
end_processing = re.compile('Profiling: Tez \'Processor\' on .* stage ending at time ([0-9]+) .*s')
start_writing = re.compile('Profiling: Tez \'Processor\' on .* stage starting at time ([0-9]+) .*s')
end_writing = re.compile('Profiling: Tez \'Output\' at .* stage ending at time ([0-9]+) .*s')
end_writing_map = re.compile('Profiling: Tez \'Spill\' at .* stage ending at time ([0-9]+) .*s')
end_task = re.compile('Profiling: Tez Container task ending at time ([0-9]+) ms')


def task_line_filter(line_string: str):
    if re.search('VertexName: .*, TaskAttemptID:.*', line_string):
        return True
    if re.search('\|: Profiling: Tez.*', line_string):
        return True
    return False

'''
{"Reducer 2": {"task_id": [start reading, end reading, start processing, end processing, start writing, end writing]}}
'''
def parse_task_log(log_string):
    vertex_map = {}
    vertex_id = None
    task_id = None
    for line in log_string.split('\n'):
        if not task_line_filter(line):
            continue
        res = task_re.findall(line)
        if res:
            vertex_id = res[0][0]
            task_id = res[0][1]
            if (vertex_id, task_id) not in vertex_map:
                vertex_map[(vertex_id, task_id)] = [0, 0, 0, 0, 0, 0]
            print(res[0][0], res[0][1])
        res = start_reading.findall(line)
        if res:
            if task_id and vertex_id:
                print(res)
                vertex_map[(vertex_id, task_id)][0] = int(res[0])
        res = end_reading.findall(line)
        if res:
            print(res)
            vertex_map[(vertex_id, task_id)][1] = int(res[0])
        res = end_reading_map.findall(line)
        if res:
            print(res)
            vertex_map[(vertex_id, task_id)][1] = int(res[0])
        res = start_processing.findall(line)
        if res:
            print(res)
            vertex_map[(vertex_id, task_id)][2] = int(res[0])
        res = end_processing.findall(line)
        if res:
            print(res)
            vertex_map[(vertex_id, task_id)][3] = int(res[0])
        res = start_writing.findall(line)
        if res:
            print(res)
            vertex_map[(vertex_id, task_id)][4] = int(res[0])
        res = end_writing.findall(line)
        if res:
            print(res)
            vertex_map[(vertex_id, task_id)][5] = int(res[0])
        res = end_writing_map.findall(line)
        if res:
            print(res)
            vertex_map[(vertex_id, task_id)][5] = int(res[0])
        if res:
            vertex_id = None
            task_id = None
    return vertex_map

def to_json(mmp):
    result = []
    for vec_task, times in mmp.items():
        t = {}
        t['vec_name'] = vec_task[0]
        t['task_id'] = vec_task[1]
        t['start_time'] = times[0]
        t['end_time'] = times[-1]
        t['step_info'] = times
        result.append(t.copy())
        print(times[-1] - times[0])
    return json.dumps(result)


def parse_task_logs(ghive_log, hive_log):
    ghive_vertex_map = parse_task_log(ghive_log)
    ghive_tasks = to_json(ghive_vertex_map)
    with open('output/ghive_tasks.json', 'w') as f:
        f.write(ghive_tasks)

    hive_vertex_map = parse_task_log(hive_log)
    hive_tasks = to_json(hive_vertex_map)
    with open('output/hive_tasks.json', 'w') as f:
        f.write(hive_tasks)
