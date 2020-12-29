# coding=utf-8
import json
import time
import pydgraph
import concurrent.futures
from app import app
import sys
import copy

reload(sys)
sys.setdefaultencoding('utf-8')

from flask import render_template, request, g, jsonify, make_response, Response

DGRAPH_SERVER_ADDR = "127.0.0.1:9080"


def create_client():
	'''
		This is the default method to start the client, the maximum data size of grpc is 5 Mb
	'''
	client_stub = pydgraph.DgraphClientStub(DGRAPH_SERVER_ADDR)
	client = pydgraph.DgraphClient(client_stub)
	return client_stub, client


class handle_dgraph:
	def __init__(self):
		self.client_stub = pydgraph.DgraphClientStub(DGRAPH_SERVER_ADDR, options=[('grpc.max_receive_message_length', 1024*1024*512)])
		self.client = pydgraph.DgraphClient(self.client_stub)
		self.nodes, self.edges, self.node_key, self.scan_uid_list = [], [], [], []
		self.parent_node = []

	def return_client(self):
		return self.client

	def return_txn(self):
		return self.client.txn()

	def drop_all(self):
		return self.client.alter(pydgraph.Operation(drop_all=True))

	def drop_task(self, task_id):  # delete data  //useless
		txn = self.client.txn()
		try:
			query = """
                query all($a:string){
                    all(func: eq(task_id, $a)){
                        uid
                    }    
                }
            """
			variables = {'$a': task_id}
			res1 = self.client(query, variables=variables)  # 单个条件的只当字段查询
			ppl = json.loads(res1.json)
			all_delete = ppl.get('all', [])
			txn.mutate(del_obj=all_delete, commit_now=True)
		except Exception as e:
			raise e
		finally:
			txn.discard()

	def return_ret(self, query_info):
		"""
			Determine query structure based on conditions
			1、condition is null：Just need to return all values
			2、condition exists：{
				Situation 1：result node is leef node，return node to the root node
				Situation 2：result node is middle node，return the node to the root node, and return the child nodes of the node
			}
		"""
		flag = True
		try:
			if query_info.get("start_ip", '') and query_info.get('end_ip',''):
				ret, flag = self.get_all_path(query_info)
				if not flag:
					return ret, flag
			else:
				self.return_query_result(query_info)
				self.get_all_datas(self.res_json)
		except Exception as e:
			raise e

		# if self.parent_node:
		# for n in self.nodes:
		# 	n["hidden"] = False
		ret_nodes = copy.deepcopy(self.nodes)
		ret_edges = copy.deepcopy(self.edges)
		is_query = False
		if self.parent_node:
			is_query = True

		ret = {"all_nodes": ret_nodes, "all_edges": ret_edges, 'is_query': is_query}
		self.nodes, self.edges, self.node_key, self.uid_list = [], [], [], []
		return ret, flag

	@staticmethod
	def create_graphql_condition(query_info):
		"""
			static method ,create query graphql info
			query_info format is {sechema_index_key:value}
			return graphql condition
			task_id is schema defined Field， any field splicing that can be indexed can be used
		"""
		search_params = ''
		ret_params = ''
		flag_num = 1
		for keys in query_info:
			value = query_info.get(keys)
			if value and flag_num == 1:
				if keys == "task_id":
					search_params += 'anyofterms(task_id, %s)' % int(value)
				else:
					search_params += 'eq(%s,"%s")' % (keys, value)
				flag_num += 1
			elif value and flag_num > 1:
				if keys == "task_id":
					search_params += 'and anyofterms(task_id, %s)' % int(value)
				else:
					search_params += ' and eq(%s,"%s")' % (keys, value)
				flag_num += 1
		if search_params:
			ret_params = '@filter(%s)' % search_params

		return ret_params

	def return_query_result(self, query_info):
		"""
			return result with parent node list and child node list
		"""
		search_params = self.create_graphql_condition(query_info)
		app.logger.info("search params is %s" % search_params)
		txn = self.client.txn()
		if search_params:
			query = """
					{
						child_nodeCount(func: has(ip_str)) %s @recurse(depth:2, loop:true) {
							ip_str
							uid
							scan_to
							has_pid
						}
						parent_nodeCount(func:has(ip_str)) %s @recurse(depth:500, loop:true) {
							ip_str
							uid
							~scan_to
						}
					}
			""" % (search_params, search_params)
		else:
			query = """
				{
					child_nodeCount(func:eq(has_pid, 1)) @recurse(depth:500, loop:true){
						ip_str
						uid
						scan_to
						has_pid
					}
				}
			"""
		self.res_json = json.loads(txn.query(query).json)
		txn.discard()

	def get_all_datas(self, nodes_info):
		child_node = nodes_info.get("child_nodeCount", [])
		self.parent_node = nodes_info.get('parent_nodeCount', [])
		self.get_child_info(child_node)
		self.get_parent_info(self.parent_node)

	def get_child_info(self, nodes_info, pid=-1, count=1):
		for node in nodes_info:
			p_hidden = False
			if count > 5:
				p_hidden = True
			uid = node.get("uid", "")
			ip_str = node.get("ip_str", "")
			scan_to = node.get("scan_to", [])
			if len(scan_to) == 0:
				node_info = {"id": uid, "label": ip_str, 'hidden': True}
				if uid not in self.node_key:
					self.node_key.append(uid)
					self.nodes.append(node_info)
				if pid != -1:
					edges_info = {"from": pid, "to": uid}
					if edges_info not in self.edges and pid != -1:
						self.edges.append(edges_info)
			else:
				subids = []
				for scan_info in scan_to:
					sub_id = scan_info.get('uid')
					subids.append(sub_id)
				p_node = {"id": uid, "label": ip_str + '[' + str(len(subids)) + ']', 'subids': subids, 'hidden': p_hidden}
				if {"from": pid, "to": uid} not in self.edges and pid != -1:
					self.edges.append({"from": pid, "to": uid})
				count += 1
				self.get_child_info(nodes_info=scan_to, pid=uid,count=count)
				if uid not in self.node_key:
					self.node_key.append(uid)
					self.nodes.append(p_node)

	def get_parent_info(self, nodes_info, pid=-1):
		for node in nodes_info:
			uid = node.get("uid", '')
			parent_scan_to = node.get("~scan_to", [])
			ip_str = node.get('ip_str', '')
			if len(parent_scan_to) != 0:
				subids = []
				for scan_info in parent_scan_to:
					sub_id = scan_info.get('uid', '')
					subids.append(sub_id)
				if pid != -1:
					p_node = {"id": uid, "label": ip_str, 'subids': subids, 'hidden': False}
					if uid not in self.node_key:
						self.node_key.append(uid)
						self.nodes.append(p_node)
				if {"to": pid, "from": uid} not in self.edges and uid != -1:
					self.edges.append({"from": uid, "to": pid})
				self.get_parent_info(nodes_info=parent_scan_to, pid=uid)
			else:
				node_info = {"id": uid, "label": ip_str, 'hidden': False}
				if uid not in self.node_key and pid != -1:
					self.node_key.append(uid)
					self.nodes.append(node_info)
				if pid != -1:
					edges_info = {"to": pid, "from": uid}
					if edges_info not in self.edges and uid != -1:
						self.edges.append({"from": uid, "to": pid})

	## Unicom relation query related functions
	def get_scan_to(self, query_ip):
		txn = self.client.txn(read_only=True)
		query = """
			{
				get_uid_structure(func:eq(ip_str, %s)) @recurse(depth:2000, loop:true){
					uid
					~scan_to
				}
			}
		""" % query_ip
		scan_to_json = json.loads(txn.query(query).json)
		uid_structure = scan_to_json.get('get_uid_structure', [])

		self.scan_uid_list = []
		self.get_uid(uid_structure)
		all_uid = copy.deepcopy(self.scan_uid_list)

		return all_uid

	def get_relationship(self, rel_info):

		uid_rel = rel_info.get("path", [])
		app.logger.info("uid rel is %s" % uid_rel)

		p_uid = -1
		for i in uid_rel:
			uid = i.get('uid')

			if uid not in self.node_key:
				self.node_key.append(uid)
				self.nodes.append({"id": i.get('uid'), "label": i.get('ip_str', ''), 'hidden': False})
			if {'from': p_uid, 'to': uid} not in self.edges:
				self.edges.append({'from': p_uid, 'to': uid})
			p_uid = uid

		app.logger.info("edges is %s" % self.edges)

	def get_path(self, parent_uid, start_ip, end_ip):
		txn = self.client.txn(read_only=True)
		query1 = """
			{
			  A as var(func: eq(ip_str, %s)) 
			  path as shortest(to: uid(A), from: %s) {
				 scan_to
			  }
			  path(func: uid(path)) {
				uid
				ip_str
			  }
			}
		""" % (start_ip, parent_uid)

		query2 = """
			{
			  A as var(func: eq(ip_str, %s)) 
			  path as shortest(to: uid(A), from: %s) {
				 scan_to
			  }
			  path(func: uid(path)) {
				 uid  
				 ip_str
			  }
			}
		""" % (end_ip, parent_uid)

		start_path_info = json.loads(txn.query(query1).json)
		end_path_info = json.loads(txn.query(query2).json)
		txn.discard()
		return start_path_info, end_path_info

	def get_all_path(self, query_info):
		"""
			1、Get all the parent node uids of two nodes
			2、Compare the two parent node lists, find the same parent node under the shortest path
			3、Get two paths based on the same parent node and return
		"""
		start_ip = query_info.get('start_ip', '')
		end_ip = query_info.get('end_ip', '')
		## step one
		start_all_uid = self.get_scan_to(start_ip)
		end_all_uid = self.get_scan_to(end_ip)

		## step two
		try:
			same_parent_uid = min(list(set(end_all_uid).intersection(set(start_all_uid))))

			if not same_parent_uid:
				return '查询节点之间不存在联通关系', False
		except:
			return '查询节点之间不存在联通关系', False

		## step three
		start_path_info, end_path_info = self.get_path(parent_uid=same_parent_uid, start_ip=start_ip, end_ip=end_ip)

		self.get_relationship(start_path_info)
		self.get_relationship(end_path_info)

		return '', True

	def get_uid(self, uid_list):
		for i in uid_list:
			if i.get('uid', -1) not in self.scan_uid_list:
				self.scan_uid_list.append(i.get('uid', -1))
			_scan_to = i.get('~scan_to', [])
			if _scan_to:
				self.get_uid(_scan_to)
			else:
				break


dgraph = handle_dgraph()

if __name__ == "__main__":
	query_info = {'start_ip': '150.138.192.90', 'end_ip': '150.138.192.155'}

	dgraph.return_ret(query_info=query_info)
