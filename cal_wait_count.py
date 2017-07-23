import os
import os.path
import sys
import time
import re
import psycopg2
from datetime import datetime as dt
from datetime import timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


conn = psycopg2.connect(database="hpc", user="eshgfuu", password="oio", host="localhost", port="5432")
#conn = psycopg2.connect(database="hpc", user="postgres", password="oio", host="168.168.5.2", port="5432")
cur = conn.cursor()


es = Elasticsearch()
#es = Elasticsearch(['168.168.5.2:19200','192.168.246.1:19200','192.168.246.3:19200','192.168.246.4:19200'])
#es = Elasticsearch(['168.168.5.2:19200'])
bulk_json = []

def calTimeList(row, type, out):
    dff = 1
    data = []
    jid, q, o, s ,e, n = row

    out.write("s,e : " + dt.strftime(s, "%Y-%m-%d %H:%M:%S") +" = " + dt.strftime(e, "%Y-%m-%d %H:%M:%S") + "\n")
    s_str = dt.strftime(s, "%Y-%m-%d %H:%M")
    e_str = dt.strftime(e, "%Y-%m-%d %H:%M")

    t1 = dt.strptime(s_str,"%Y-%m-%d %H:%M")
    t2 = dt.strptime(e_str,"%Y-%m-%d %H:%M")

    if s - t1 == timedelta(seconds = 0):
        f = dt.strftime(s, "%Y-%m-%d %H:%M")
        data.append((jid, q,o, f, n))
        print "equal : " + dt.strftime(s, "%Y-%m-%d %H:%M:%S") +" = " + dt.strftime(t1, "%Y-%m-%d %H:%M:%S")
    
    if t2 - t1  == timedelta(seconds = 0):
        return 

    while t2  - t1 >= timedelta(seconds = 60) :
        t1 = t1 + timedelta(seconds = 60)
        f = dt.strftime(t1 ,"%Y-%m-%d %H:%M")
        data.append((jid,q,o,f,n))

    if data.count > 0 :
        bulk_json = []
        for item in data:
            out.write(item.__str__())
            out.write("\n")
            res = {}
            job_id ,job_queue, job_owner, job_time,job_ncpu = item
            res['job_id']  = job_id
            res['job_queue']  = job_queue
            res['job_owner']  = job_owner
            res['job_time']  = job_time
            res['job_ncpu']  = job_ncpu

            body = {
                '_index': "hpc.job."+ job_time[0:7].replace('-','.'),
                '_type': type,
                '_source': res
            }
            bulk_json.append(body)
        try:
            success, _ = bulk(es, bulk_json, raise_on_error=True)
            out.write('Performed %d actions' % success + "\n")
        except KeyError as e:
            print e

with open("lastUpdate.txt", mode="r") as w:
    t = w.readline()
    res = {}
    res["t"] = t
    with open("list-wait.txt", mode="w") as tt:
        cur.execute("select a2.job_id as jid , a2.job_queue as q, a2.job_owner as o, a2.st as s, a1.create_time as e, a2.job_ncpu as n \
        from job_dispatch as a1, job_result as a2 where a1.job_run_id = a2.job_run_id AND a2.job_status < 3  AND a2.update_time > %(t)s\
        group by jid,q,o, s,e,n order by s;", res)
        for record in cur :
            calTimeList(record,'wait',tt)
    with open("list-run.txt", mode="w") as tt:
        cur.execute("select a2.job_id as jid , a2.job_queue as q, a2.job_owner as o, a1.create_time as s, a2.et as e, a2.job_ncpu as n \
        from job_dispatch as a1, job_result as a2 where a1.job_run_id = a2.job_run_id AND a2.job_status < 3  AND a2.update_time > %(t)s\
        group by jid,q,o, s,e,n order by s;", res)
        for record in cur :
            calTimeList(record,'run',tt)

with open("lastUpdate.txt", mode="w") as w:
    w.write(dt.strftime(dt.now(), "%Y-%m-%d %H:%M:%S"))

