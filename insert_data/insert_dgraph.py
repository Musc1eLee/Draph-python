# /usr/bin/env python
# coding=utf-8

import time
import json
from copy import deepcopy
import config  #config has your server ip and port
import pydgraph


def insert_dgraph(client, datas):
	txn = client.txn()

	# logger.info("insert dgraph relationship is %s"%datas)
	insert_count = 0
	try:
		insert_list = []
		for i in datas:
			insert_list.append(i)
			if len(insert_list) == 500:
				txn.mutate(set_obj=insert_list, commit_now=True)
				insert_count += len(insert_list)
				insert_list = []
				time.sleep(0.01)
		txn.mutate(set_obj=insert_list, commit_now=True)
		insert_count += len(insert_list)
		time.sleep(0.01)
	except Exception as e:
		raise e
	finally:
		# logger.info("insert list is %s"%insert_list)
		time.sleep(10)
		txn.discard()
		return insert_count


def insert_node(client, insert_data, fig_task_info):
	txn = client.txn()
	fig_task_id = fig_task_info.id
	task_scan_count = fig_task_info.scan_count
	# into_graph_info = query_ES_for_dgraph(task_id)  #查询ES
	node_uid_info = []
	node_ip_info = []

	for i in insert_data:
		uid_ip_dic = {}
		start_ip = i.get('start', {}).get('ip_str', '')
		end_ip = i.get('end', {}).get('ip_str', '')
		query = """
            {
                nodeExist(func:has(ip_str)) @filter(eq(ip_str, %s) or eq(ip_str, %s)){
                    uid
                    has_pid
                    ip_str
                    task_id
                }
            }
        """ % (start_ip, end_ip)
		res = txn.query(query)
		ppl = json.loads(res.json)
		exist_data = ppl['nodeExist']
		for node in exist_data:
			ip_str = node.get("ip_str", '')
			uid_ip_dic[ip_str] = node
		start_node = uid_ip_dic.get(start_ip, {})
		end_node = uid_ip_dic.get(end_ip, {})
		insert_2_graph = {}
		if start_ip not in node_ip_info:
			node_ip_info.append(start_ip)
			insert_2_graph['ip_str'] = start_ip

			if not start_node.get('has_pid',''):
				insert_2_graph['has_pid'] = 1
			if start_node.get('uid', ''):  # update dgraph data
				graph_task_id = start_node.get('task_id', [])
				insert_2_graph['uid'] = start_node.get('uid', '')
				node_uid_info.append(insert_2_graph)
			else:  # insert dgraph data
				node_uid_info.append(insert_2_graph)


		if end_ip not in node_ip_info:
			node_ip_info.append(end_ip)
			insert_2_graph['ip_str'] = end_ip
			insert_2_graph['has_pid'] = -1

			if end_node.get('uid', ''):  # update dgraph data
				insert_2_graph['uid'] = end_node.get('uid','')
				node_uid_info.append(insert_2_graph)
			else:  # insert dgrapg data
				node_uid_info.append(insert_2_graph)
	try:
		insert_list = []
		for i in node_uid_info:
			insert_list.append(i)
			if len(insert_list) == 500:
				txn.mutate(set_obj=insert_list, commit_now=True)
				insert_list = []
				time.sleep(0.01)
		if insert_list:
			txn.mutate(set_obj=insert_list, commit_now=True)
			time.sleep(0.01)
	except Exception as e:
		raise e
	finally:
		time.sleep(10)
		repeat_node_num = len(insert_data) - len(node_uid_info)
		# logger.info('repeat node number is %s' % repeat_node_num)


def dgraph_relationship(client, insert_data):
	mutate_data = []
	queue_scan_to = {}
	for i in insert_data:
		start_ip = i.get('start')
		end_ip = i.get('end')
		query = """
            {
                nodeExist(func:has(ip_str)) @filter(eq(ip_str, %s) or eq(ip_str, %s)){
                    uid
                    ip_str
                    scan_to{
                        uid
                    }
                }
            }
        """ % (start_ip.get("ip_str"), end_ip.get("ip_str"))
		res = client.txn(read_only=True).query(query)
		ppl = json.loads(res.json)
		exist_data = ppl['nodeExist']
		uid_ip_dic = {}
		for node in exist_data:
			uid = node.get("uid", '')
			ip_str = node.get("ip_str", '')
			uid_ip_dic[ip_str] = node
			if not queue_scan_to.has_key(uid):
				queue_scan_to[uid] = node.get('scan_to', [])  # 存放单个队列中relationship信息
		start_node = uid_ip_dic.get(start_ip.get("ip_str", ''), {})
		end_node = uid_ip_dic.get(end_ip.get("ip_str", ''), {})

		scan_to_dic = {"uid": end_node.get("uid")}  # 形成该条数据的relationship信息
		if scan_to_dic not in queue_scan_to.get(start_node.get("uid", ''), []):
			scan_to = queue_scan_to.get(start_node.get('uid', ''), [])
			scan_to.append(scan_to_dic)
			queue_scan_to[start_node.get('uid', '')] = scan_to
	for k in queue_scan_to.keys():
			mutate_data.append({"uid": k, "scan_to": queue_scan_to.get(k)})
	# logger.info("dgraph input data is %s" % mutate_data)
	return mutate_data


def dgraph_data_operation(data):  # is a recive data handle method

	node_info = {
		"ip_str": "",
	}
	node = {
		"start": node_info,
		"end": node_info
	}

	dgraph_data_list = []
	for a in data:
		one_data = a.get('job-data', [])
		for i in one_data:
			p_ip = i.get('ip', {}).get('dip', '')
			s_ip_list = [j.get('dip', '') for j in i.get('sip', []) if j.get('dip', '')]
			temp_start = deepcopy(node_info)
			temp_start.update({"ip_str": str(p_ip)})
			for d in s_ip_list:
				temp_end = deepcopy(node_info)
				temp_end.update({"ip_str": str(d)})
				temp_node = deepcopy(node)
				temp_node.update({"start": temp_start, "end": temp_end})
				dgraph_data_list.append(temp_node)

	return dgraph_data_list


def insert_main(insert_data):
	# logger.info("will insert dgraph data is %s" % insert_data)
	dgraph_host = "%s:%s" % (config.DGRAPH_IP, config.DGRAPH_PORT)
	
	insert_count = 0
	if insert_data:
		client_stub = pydgraph.DgraphClientStub(dgraph_host)
		client = pydgraph.DgraphClient(client_stub)
		try:
			insert_node(client, insert_data)
			# logger.info("now is start mutate data")
			mutate_data = dgraph_relationship(client, insert_data)
			insert_count = insert_dgraph(client, mutate_data)
		except Exception as e:
			print ('insert dgraph error:%s' % e)
			# logger.error('insert dgraph error:%s' % e)
		finally:
			client_stub.close()
			return insert_count
	else:
		print ("insert dgraph data is null")
		# logger.info("insert dgraph data is null")


if __name__ == '__main__':

	job_status = [
		{"job-data": [{"ip": {"dip": "172.18.0.1"}, "sip": [{"dip": "172.18.0.10"}, {"dip": "172.18.0.20"}]}]},
		{"job-data": [{"ip": {"dip": "172.18.0.2"}, "sip": [{"dip": "172.18.0.202"}, {"dip": "172.18.0.112"}]}]},
	]
	insert_data = dgraph_data_operation(job_status)
	# print(insert_data)
	insert_main(insert_data)
