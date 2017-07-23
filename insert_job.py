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
    try:
        time_arr = dt.strptime(time_str, '%m/%d/%Y %H:%M:%S')
    except ValueError, e:
        return
    t_str = dt.strftime(time_arr, '%Y-%m-%d %H:%M:%S')
    res = {}
    res['job_id'] = job_str
    # res['create_time'] = t_str
    res['ts'] = t_str
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
                job_status,submit_time, update_time)VALUES(%(job_name)s, %(owner)s, %(queue)s, \
                %(job_id)s, %(job_status)s,%(ts)s,%(update_time)s)", res)

            cur.execute("INSERT INTO job_result(job_name, job_owner,job_queue,job_id,\
                job_status,st,update_time)VALUES(%(job_name)s, %(owner)s, %(queue)s, \
                %(job_id)s, %(job_status)s, %(ts)s ,%(update_time)s)", res)
            conn.commit()
        elif 'exec_vnode' in log_str:
            match = pattern2.findall(log_str)
            res['job_status'] = 4
            if len(match) > 0:
                res['job_run_id'] = res['job_id'] + dt.strftime(time_arr, '.%Y%m%d%H%M%S')
                for item in match:
                    key, val = item
                    res['job_run_node'] = key
                    res['job_run_ncpu'] = int(val)
                    cur.execute("\
                    INSERT INTO job_dispatch(job_id, job_run_node,job_run_ncpu,job_run_id\
                        ,job_status,create_time,update_time)VALUES(%(job_id)s, %(job_run_node)s, %(job_run_ncpu)s, \
                        %(job_run_id)s,%(job_status)s, %(ts)s,%(update_time)s)\
                        ", res)
                    cur.execute("update job_submit set job_status=%(job_status)s, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
                cur.execute("update job_result set job_status=%(job_status)s, \
                    job_run_id=%(job_run_id)s, update_time=%(update_time)s \
                    where job_id=%(job_id)s", res)
                conn.commit()
        # elif code_str == '0080':
        elif 'Job to be deleted at request of' in log_str:
            cur.execute("update job_result set job_status=2,et=%(ts)s \
                        ,update_time=%(update_time)s where job_id=%(job_id)s", res)
            cur.execute("update job_submit set job_status=2, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
            cur.execute("update job_dispatch set job_status=2, \
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
        
        # if x:
        STATUS = 0
        if res['exit_status'] != 0:
            STATUS = 1
        cur.execute("select job_status from job_result where job_id = %(job_id)s ", res)
        x = cur.fetchone()
        if x and x[0] == 2:
            STATUS = 2
        res['job_status'] = STATUS
        cur.execute("update job_result set job_status=%(job_status)s,job_exit_status=%(exit_status)s\
                , job_cpupercent=%(resources_used_cpupercent)s\
                , job_ncpu=%(resources_used_ncpus)s, job_cput=%(resources_used_cput)s, job_mem=%(resources_used_mem)s\
                , job_vmem=%(resources_used_vmem)s, job_walltime=%(resources_used_walltime)s, et=%(ts)s\
                , update_time=%(update_time)s where job_id=%(job_id)s", res)
        cur.execute("update job_submit set job_status=%(job_status)s, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
        cur.execute("update job_dispatch set job_status=%(job_status)s, \
                        update_time=%(update_time)s where job_id=%(job_id)s", res)
        conn.commit()


def fead(filepath):
    '''
    read file line
    '''
    with open(filepath, mode='r') as stream:
        for line in stream.readlines():
            if ';Job;' in line:
                job(line)
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
    
    conn.close()
