import json
from datetime import datetime as dt
import time
import re
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


pattern = re.compile(r'\.(\w+)=(\w+)')


def node(arr_str):
    '''
    node action
    '''
    #arr_str = l_str.split(';')
    time_str = arr_str[0]
    code_str = arr_str[1]
    server_str = arr_str[2]
    action_str = arr_str[3]
    node_str = arr_str[4]
    log_str = arr_str[5].strip('\n')

    time_arr = dt.strptime(time_str, '%m/%d/%Y %H:%M:%S')
    hour = str(time_arr.hour)
    week = str(time_arr.isoweekday())
    if week == 0:
        week = 7
    timestamp = int(time.mktime(time_arr.timetuple())) * 1000
    res = {'time': timestamp, 'code': code_str, 'server': server_str,
           'action': action_str, 'node': node_str, 'log': log_str, 'hourofday': hour, 'dayofweek': week}

    match = pattern.search(log_str)
    if match:
        key = match.group(1)
        val = match.group(2)
        if key == 'mem_usage':
            val = int(val)
        else:
            val = float(val)
        res[key] = val
    return res


pattern2 = re.compile(r'\((\w+):ncpus=(\d+)\)')
pattern3 = re.compile(r'of\s(.+)\son')


def job(arr_str):
    '''
    Job action
    '''
    #arr_str = l_str.split(';')
    time_str = arr_str[0]
    code_str = arr_str[1]
    server_str = arr_str[2]
    action_str = arr_str[3]
    job_str = arr_str[4]
    log_str = arr_str[5].strip('\n')

    time_arr = dt.strptime(time_str, '%m/%d/%Y %H:%M:%S')
    hour = str(time_arr.hour)
    week = str(time_arr.isoweekday())
    timestamp = int(time.mktime(time_arr.timetuple())) * 1000
    res = {'time': timestamp, 'code': code_str, 'server': server_str,
           'action': action_str, 'job': job_str, 'log': log_str, 'hourofday': hour, 'dayofweek': week}

    def tt(val):
        ts = val.split(':')
        second = float(ts[2]) / 60.0
        minute = (float(ts[1]) + second) / 60.0
        hour = float(ts[0]) + minute
        return hour

    if code_str == '0010':
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

    elif code_str == '0008':
        if 'owner' in log_str:
            for item in log_str.split(','):
                if '=' in item:
                    key = item.split('=')[0]
                    val = item.split('=')[1]
                    res[key.strip().replace(' ', '_')] = val.strip()
        elif 'exec_vnode' in log_str:
            match = pattern2.findall(log_str)
            if len(match) > 0:
                e_vnode = []
                for item in match:
                    key, val = item
                    vnode = {}
                    vnode['exec_vnode'] = key
                    vnode['ncpus'] = int(val)
                    e_vnode.append(vnode)
                res['exec_vnodes'] = e_vnode
            match1 = pattern3.search(log_str)
            if match1:
                res['request_by'] = match1.group(1)

    return res


req_log_pattern = re.compile(
    r'Type\s(\d+)\srequest\sreceived\sfrom\s(.+),\ssock=(\d+)')


def req(arr_str):
    '''
    Req Action

    '''
    #arr_str = l_str.split(';')
    time_str = arr_str[0]
    code_str = arr_str[1]
    server_str = arr_str[2]
    action_str = arr_str[3]
    #job_str = arr_str[4]
    log_str = arr_str[5].strip('\n')

    time_arr = dt.strptime(time_str, '%m/%d/%Y %H:%M:%S')
    hour = str(time_arr.hour)
    week = str(time_arr.isoweekday())
    timestamp = int(time.mktime(time_arr.timetuple())) * 1000
    res = {'time': timestamp, 'code': code_str, 'server': server_str,
           'action': action_str, 'log': log_str, 'hourofday': hour, 'dayofweek': week}

    match = req_log_pattern.search(log_str)
    if match:
        res['type'] = int(match.group(1))
        res['received_from'] = match.group(2)
        res['sock'] = int(match.group(3))

    return res


def svr(arr_str):
    '''
    Req Action

    '''
    #arr_str = l_str.split(';')
    time_str = arr_str[0]
    code_str = arr_str[1]
    server_str = arr_str[2]
    action_str = arr_str[3]
    src_str = arr_str[4]
    log_str = arr_str[5].strip('\n')

    time_arr = dt.strptime(time_str, '%m/%d/%Y %H:%M:%S')
    hour = str(time_arr.hour)
    week = str(time_arr.isoweekday())
    timestamp = int(time.mktime(time_arr.timetuple())) * 1000
    res = {'time': timestamp, 'code': code_str, 'server': server_str,
           'action': action_str, 'from': src_str, 'log': log_str, 'hourofday': hour, 'dayofweek': week}

    return res


def tpp(arr_str):
    '''
    TPP Action

    '''
    return svr(arr_str)


def hook(arr_str):
    '''
    Hook Action

    '''
    return svr(arr_str)


def que(arr_str):
    '''
    Que Action

    '''
    return svr(arr_str)


switch = {
    "Node": node,
    "Job": job,
    "Req": req,
    "Svr": svr,
    "TPP": tpp,
    "Hook": hook,
    "Que": que
}
es = Elasticsearch()
with open('in/20170604', mode='rb') as stream:
    bulk_json = []
    for line in stream.readlines():
        arr_str = line.split(';')
        time_arr = dt.strptime(arr_str[0], '%m/%d/%Y %H:%M:%S')
        index_name = dt.strftime(time_arr, 'hpc.%Y.%m')
        try:
            source = switch[arr_str[3]](arr_str)
            body = {
                '_index': index_name,
                '_type': arr_str[3],
                '_source': source
            }
            bulk_json.append(body)

            if len(bulk_json) > 3999:
                success, _ = bulk(es, bulk_json, raise_on_error=True)
                del bulk_json[0:len(bulk_json)]
                print 'Performed %d actions' % success
        except KeyError as e:
            print e
    if len(bulk_json) > 0:
        success, _ = bulk(es, bulk_json, raise_on_error=True)
        del bulk_json[0:len(bulk_json)]
        print 'Performed final %d actions' % success
