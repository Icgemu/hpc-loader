import os
import os.path
import sys
import time
import psycopg2
import re
from datetime import datetime as dt
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

conn = psycopg2.connect(database="hpc", user="eshgfuu", password="oio", host="localhost", port="5432")
#conn = psycopg2.connect(database="hpc", user="postgres", password="oio", host="168.168.5.2", port="5432")
cur = conn.cursor()

node_pattern = re.compile(r'\.(\w+)=((\w|\.)+)')
node_info = ("",-1l,-1.0,-1.0,-1l,0)

#es = Elasticsearch()
#es = Elasticsearch(['168.168.5.2:19200','192.168.246.1:19200','192.168.246.3:19200','192.168.246.4:19200'])
es = Elasticsearch(['168.168.5.2:19200'])
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

pattern2 = re.compile(r'\((\w+):ncpus=(\d+)\)')
pattern3 = re.compile(r'of\s(.+)\son')
def job(line):
    '''
    job info extraction
    '''
    arr_str = line.split(';')
    time_str = arr_str[0]
    code_str = arr_str[1]
    # server_str = arr_str[2]
    # action_str = arr_str[3]
    job_str = arr_str[4]
    log_str = arr_str[5].strip('\n')

    time_arr = dt.strptime(time_str, '%m/%d/%Y %H:%M:%S')
    t_str = dt.strftime(time_arr, '%Y-%m-%d %H:%M:%S')
    res = {}
    res['job_id'] = job_str
    res['create_time'] = t_str
    res['update_time'] = dt.strftime(dt.now(), '%Y-%m-%d %H:%M:%S')
    #submit 3 , run 4 , success 0 , failure 1, cancel 2
    res['job_status'] = 3
    if code_str == '0008':
        if 'owner' in log_str:
            for item in log_str.split(','):
                if '=' in item:
                    key = item.split('=')[0]
                    val = item.split('=')[1]
                    res[key.strip().replace(' ', '_')] = val.strip()
            cur.execute("INSERT INTO job_submit(job_name, job_owner,job_queue,job_id,\
                job_status,create_time,update_time)VALUES(%(job_name)s, %(owner)s, %(queue)s, \
                %(job_id)s, %(job_status)s, %(create_time)s,%(update_time)s)", res)

            cur.execute("INSERT INTO job_result(job_name, job_owner,job_queue,job_id,\
                job_status,st,create_time,update_time)VALUES(%(job_name)s, %(owner)s, %(queue)s, \
                %(job_id)s, %(job_status)s, %(create_time)s,%(create_time)s,%(update_time)s)", res)
            conn.commit()
        elif 'exec_vnode' in log_str:
            match = pattern2.findall(log_str)
            res['job_status'] = 4
            if len(match) > 0:
                for item in match:
                    key, val = item
                    res['job_run_node'] = key
                    res['job_run_ncpu'] = int(val)
                    cur.execute("INSERT INTO job_dispatch(job_id, job_run_node,job_run_ncpu,\
                        job_status,create_time,update_time)VALUES(%(job_id)s, %(job_run_node)s, %(job_run_ncpu)s, \
                        %(job_status)s, %(create_time)s,%(update_time)s)", res)
                    cur.execute("update job_submit set job_status=%(job_status)s, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
                    conn.commit()

    elif code_str == '0010':
        for item in log_str.split(' '):
            key = item.split('=')[0].lower().replace('.', '_')
            val = item.split('=')[1]
            if 'cput' in key:
                pass
                #val = tt(val)
            elif 'walltime' in key:
                pass
                #val = tt(val)
            elif 'vmem' in key:
                pass
                #val = long(val.replace('kb', ''))
            elif 'mem' in key:
                pass
                #val = long(val.replace('kb', ''))
            else:
                val = long(val)

            res[key] = val
        # cur.execute("select * from job_result where job_id = %(job_id)s ",res)
        # x = cur.fetchone()
        # if x:
        js = 0
        if res['exit_status'] != 0:
            js = 1
        res['job_status'] = js
        cur.execute("update job_result set job_status=%(job_status)s, job_cpupercent=%(resources_used_cpupercent)s\
                , job_ncpu=%(resources_used_ncpus)s, job_cput=%(resources_used_cput)s, job_mem=%(resources_used_mem)s\
                , job_vmem=%(resources_used_vmem)s, job_walltime=%(resources_used_walltime)s, et=%(create_time)s\
                , update_time=%(update_time)s where job_id=%(job_id)s", res)
        cur.execute("update job_submit set job_status=%(job_status)s, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
        cur.execute("update job_dispatch set job_status=%(job_status)s, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
        conn.commit()
    elif code_str == '0080':
        if 'delete job request received' in log_str:
            cur.execute("update job_result set job_status=2,et=%(create_time)s \
                        ,update_time=%(update_time)s where job_id=%(job_id)s", res)
            cur.execute("update job_submit set job_status=2, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
            cur.execute("update job_dispatch set job_status=2, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
            conn.commit()


def fead(filepath):
    '''
    read file line
    '''
    with open(filepath, mode='r') as stream:
        for line in stream.readlines():
            #print line
            # arr_str = line.split(';')
            # action = arr_str[3]
            if ';Job;' in line:
                job(line)
            elif ';Node;' in line:
                node(line)


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
    conn.close()
