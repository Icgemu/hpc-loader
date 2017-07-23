import os
import os.path
import sys
import time
import re
from datetime import datetime as dt
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

node_pattern = re.compile(r'\.(\w+)=((\w|\.)+)')
node_info = ("",-1l,-1.0,-1.0,-1l,0)

es = Elasticsearch()
#es = Elasticsearch(['168.168.5.2:19200','192.168.246.1:19200','192.168.246.3:19200','192.168.246.4:19200'])
#es = Elasticsearch(['168.168.5.2:19200'])
bulk_json = []
def writeSQl(data):
    #print data
    # cur.execute("INSERT INTO node_status(name, cpu_usage,disk_usage,\
    #     mem_usage,create_time,update_time)VALUES(%s\
    #     , %s, %s, %s, %s, %s)", data)
    node_name, cpu_usage, disk_usage, mem_usage, create_time, update_time = data
    res = {}
    res['node_name'] = node_name
    res['cpu_usage'] = cpu_usage
    res['disk_usage'] = disk_usage
    res['mem_usage'] = mem_usage
    res['create_time'] = create_time
    res['update_time'] = update_time

    timeStamp = create_time/1000
    timeArray = time.localtime(timeStamp)
    index_name = time.strftime("hpc.node.%Y.%m", timeArray)
    hour = time.strftime("%H", timeArray)
    week = int(time.strftime("%w", timeArray))

    res['hourofday'] = hour
    res['dayofweek'] = week
    try:
        source = res
        body = {
            '_index': index_name,
            '_type': 'stats',
            '_source': source
        }
        bulk_json.append(body)
        if len(bulk_json) > 3999:
            success, _ = bulk(es, bulk_json, raise_on_error=True)
            del bulk_json[0:len(bulk_json)]
            print 'Performed %d actions' % success
    except KeyError as e:
            print e
    # conn.commit()
def node(line):
    '''
    node info extraction
    '''
    global node_info
    arr_str = line.split(';')
    time_str = arr_str[0]
    node_str = arr_str[4]
    log_str = arr_str[5].strip('\n')

    time_arr = dt.strptime(time_str, '%m/%d/%Y %H:%M:%S')
    #t_str = dt.strftime(time_arr, '%Y-%m-%d %H:%M:%S')
    timestamp = int(time.mktime(time_arr.timetuple())) * 1000
    t_now = int(time.mktime(dt.now().timetuple())) * 1000
    match = node_pattern.search(log_str)
    node, t, cpu, disk, mem, cnt = node_info

    if node != node_str and cnt > 0:
        writeSQl((node, cpu, disk, mem, t, t_now))
    if match:
        key = match.group(1)
        val = match.group(2)
        if key == 'mem_usage':
            mem = int(val)
        elif key == 'cpu_usage':
            cpu = float(val)
        else:
            disk = float(val)
        cnt = cnt + 1
        if cnt == 3:
            writeSQl((node, cpu, disk, mem, t, t_now))
            node_info = ("", -1l, -1.0, -1.0, -1l, 0)
        else:
            node_info = (node_str, timestamp, cpu, disk, mem, cnt)


def fead(filepath):
    '''
    read file line
    '''
    with open(filepath, mode='r') as stream:
        for line in stream.readlines():
            if ';Node;' in line:
                node(line)
            else:
                pass


if __name__ == "__main__":
    ROOT = sys.argv[1]

    FILES = os.listdir(ROOT)
    FILES.sort()
    for name in FILES:
        if '.' in name:
            continue
        filepath = os.path.join(ROOT, name)
        print filepath
        fead(filepath)
    if len(bulk_json) > 0:
        success, _ = bulk(es, bulk_json, raise_on_error=True)
        del bulk_json[0:len(bulk_json)]
        print 'Performed final %d actions' % success
