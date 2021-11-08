import socket 
import json
import time
import sys
import os
import linecache
import hashlib
from math import ceil
from threading import Thread 
from SocketServer import ThreadingMixIn 
from random import randint

file_name_to_path = {}
file_name_to_info_path = {}

peer_port=0 
kilobytes = 1000
chunk_size = 256 *kilobytes
readsize = 1024
def to_four_digit(num):
	str_num = str(num)
	while(len(str_num) < 4):
		str_num = "0" + str_num
	return str_num


def md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def upload(file_path):
	file_name = file_path.split("/")[-1]
	directory_name = file_name.split(".")[0]
	file_name_to_path[file_name] = file_path
	print file_name_to_path
	create_upload_record(file_path)
	split_and_insert(file_path, directory_name)

def hash_function(file_name):
	max_id=max_static_id('static_nodes.txt')
	hash_val=(abs(hash(file_name)))%max_id
	return hash_val

def create_upload_record(file_path):
    size = os.path.getsize(file_path)
    file_name = file_path.split("/")[-1]
    hash_val = hash_function(file_name)
    chunk_count = ceil(float(size)/chunk_size)
    md5_hash = md5(file_path)
    f = open(file_name + "-info" , 'w')
    f.write(file_name + "\n")
    f.write(str(hash_val)+ "\n")
    f.write(str(chunk_count)+ "\n")
    f.write(str(md5_hash)+"\n")
    f.close()


def split_and_insert(from_file, to_dir, chunk_size=chunk_size): 
    
    if not os.path.exists(to_dir):                  
        os.mkdir(to_dir)                            
    else:
        for fname in os.listdir(to_dir):            
            os.remove(os.path.join(to_dir, fname)) 
    partnum = 0
    input = open(from_file, 'rb')
    from_file_name = from_file.split("/")[-1]            
    while 1:                                       
        chunk = input.read(chunk_size)              
        if not chunk: break
        partnum  = partnum+1
        filename = os.path.join(to_dir, (from_file_name + '-%04d' % partnum))
        fileobj  = open(filename, 'wb')
        fileobj.write(chunk)
        fileobj.close()     
        client_node.send_insert_query(from_file_name,partnum)
        time.sleep(0.01)
        #inja ye chunk joda karde o too ye file neveshte  partnum = shomare chunk     
    input.close(  )

def join(fromdir, tofile):
    output = open(tofile, 'wb')
    chunks  = os.listdir(fromdir)
    chunks.sort(  )
    for filename in chunks:
        filepath = os.path.join(fromdir, filename)
        fileobj  = open(filepath, 'rb')
        while 1:
            filebytes = fileobj.read(readsize)
            if not filebytes: break
            output.write(filebytes)
        fileobj.close(  )
    output.close(  )

def max_static_id(file_path):
	max_id = 0
	with open(file_path, "r") as f:
		for line in f.readlines():
			current_record_id  = int(line.split(" - ")[0])
			if current_record_id > max_id:
				max_id = current_record_id
	return max_id

def find_next_static_id(file_path , peer_id):
	max_id  = 1000000

	with open(file_path, "r") as f:
		for line in f.readlines():
			current_record  = int(line.split(" - ")[0])
			if (current_record < max_id) and (current_record > int(peer_id)):
				max_id = current_record
	return max_id 

def select_id_from_statics(file_path):
	num_of_lines = -1
	with open(file_path) as f:
		num_of_lines = sum(1 for _ in f)
	print "num_of_lines = " , num_of_lines
	maximum_static_id=max_static_id(file_path)
	while True:
		static_node_selected = randint(1, num_of_lines)
		print "static_node_selected = " , static_node_selected
		line = linecache.getline(file_path, static_node_selected)
		print "line = ", line
		peer_id = int(line.split(" - ")[0])
		peer_port = int(line.split(" - ")[1])
		print "peer_id" , peer_id , "peer_port" , peer_port
		if peer_id!=maximum_static_id:
			break
		
	next_static_id = find_next_static_id(file_path , peer_id)
	print "next_static_id = " , next_static_id

	dynamic_id = randint(peer_id+1, next_static_id-1)
	print "dynamic_id = " , dynamic_id 

	answer={
		'id': dynamic_id,
		'prev_static_port':peer_port,
		'prev_static_id':peer_id
	}
	return answer

def send_message_to_peer(message,ip,port):
	'sending message to peer with port ',port
	node_socket=create_socket(ip,port)
	node_socket.send(json.dumps(message))
	node_socket.close()

def create_socket(ip,port):
    s = socket.socket()
    s.connect((ip,int(port)))
    print "creating socket to node : ip= "+ip+"port",port
    return s

def add_to_static_nodes_file(peer_id,peer_port):
  print "adding static node to file"
  static_nodes_file = open("static_nodes.txt", "a")
  file_str = peer_id+" - "+str(peer_port)+'\n'
  print file_str
  static_nodes_file.write(file_str)
  static_nodes_file.close()    

def start_download(info_file_path):
	with open(info_file_path,"r") as f:
		file_name=f.readlines()
		print 'file name in start download:-',file_name[0],'-'
		clean_file_name=file_name[0][:-1]
		print 'clean_filename:-'+clean_file_name+'-'
		if client_node.self_has_record(clean_file_name):
			print 'self has record'
			hash_val=hash_function(clean_file_name)
			ans=client_node.find_value_from_hashtable(hash_val,clean_file_name)
			file_chunk_ip=ans["chunk_ip"]
			print 'chunk ip in self has record :'
			print file_chunk_ip
			file_name_to_info_path[clean_file_name] = info_file_path
			print 'file name to info path in start download:'
			print file_name_to_info_path
			download_file(clean_file_name,file_chunk_ip,1)
		else:
			'self nadare recordo !'
			client_node.send_find_records_req(clean_file_name)
			file_name_to_info_path[clean_file_name] = info_file_path
			print 'file name to info path in start download:'
			print file_name_to_info_path


def download_file(file_name,chunk_ip, is_self):
	print 'file name in dl file :-'+file_name+'-'
	print file_name_to_info_path
	info_file_path = file_name_to_info_path[file_name]
	f = open(info_file_path, 'r')
	lines = f.readlines()
	chunks = int(lines[2].split(".")[0])
	print "chunks :-" , chunks , "-"
	md5_hash = lines[3][:-1]
	print "md5 :-" + md5_hash + "-"
	dir_name = "downloaded-"+ file_name
	
	if not os.path.exists(dir_name):
		os.mkdir(dir_name)
	else:
		for fname in os.listdir(dir_name):
			os.remove(os.path.join(dir_name, fname))
	
	start_time=time.clock()
	print 'start_time:',start_time
	for i in range(1, chunks+1):
		print "downloading chunk-" + str(i) + "-"
		dl_ip = "127.0.0.1"
		print 'chunk ip in download file'
		print chunk_ip
		if (is_self):
			dl_port = chunk_ip[i][0]["port"]
		else:
			dl_port = chunk_ip[str(i)][0]["port"]	
		chunk_socket = create_socket(dl_ip, dl_port)
		msg = {
			'type' : 'get_chunk',
			'file_name' :	file_name,
			'chunk_num' : i		
		}
		chunk_socket.send(json.dumps(msg))
		chunk_file_name = dir_name + "/" + file_name + "-" + to_four_digit(i)
		chunk_file = open(chunk_file_name, 'w')
		chunk_response = chunk_socket.recv(2048)
		while (chunk_response):
			chunk_file.write(chunk_response)
			chunk_response = chunk_socket.recv(2048)
		chunk_file.close()

	end_time=time.clock()
	print 'end_time:',end_time
	print 'time:',end_time-start_time
	join(dir_name, "jadid-"+file_name)

	new_md5_hash = md5(file_name+"jadid")
	print "-" , new_md5_hash , "?=?" , md5_hash , "-"
	
	if(new_md5_hash == md5_hash):
		print "Same File Here! :)"
	else:
		print "Different File :|"


		# loaded_response = json.loads(chunk_response)
		# content = loaded_response["content"]
		# chunk_num = loaded_response["chunk_num"]
		# chunk_file.write(content)
		# chunk_file.close()

		

 



class File_records:
	def __init__(self,file_name):
		self.file_name=file_name
		self.chunk_ip={}
	def add_to_chunk_ip(self,chunk_num,ip):
		if self.chunk_ip.has_key(chunk_num)==False:
			self.chunk_ip[chunk_num]=[]
		self.chunk_ip[chunk_num].append(ip)
	def print_file_records(self):
		print self.file_name,': '
		print self.chunk_ip

class Node:
	def __init__(self):
		print "caling parent node constructor"
	def print_node(self):
		print 'peer_id = ',self.peer_id
		print 'nextpeer_id = ',self.nextpeer_id
		print 'nextpeer_port =',self.nextpeer_port
		print 'prevpeer_id = ',self.prevpeer_id
		print 'prevpeer_port =',self.prevpeer_port
	def create_message_id_for_next_peer(self):
		message = {
			'type':'prev_id_port',
			'id':self.peer_id,
			'port':peer_port
		}
		return message
	def create_message_id_for_prev_peer(self):
		message = {
			'type':'next_id_port',
			'id':self.peer_id,
			'port':peer_port
		}
		return message
	def send_message_to_next_node(self,message):
		'sending message to next node'
		next_node_socket=create_socket('127.0.0.1',self.nextpeer_port)
		next_node_socket.send(json.dumps(message))
		next_node_socket.close()
	def send_message_to_prev_node(self,message):
		prev_node_socket=create_socket('127.0.0.1',self.prevpeer_port)
		'sending message to prev node'
		prev_node_socket.send(json.dumps(message))
		prev_node_socket.close()
	def set_prev_id_port(self,prev_id,prev_port):
		self.prevpeer_id=prev_id
		self.prevpeer_port=prev_port
		self.print_node()
	def set_next_id_port(self,next_id,next_port):
		self.nextpeer_id=next_id
		self.nextpeer_port=next_port
		self.print_node()
	def add_record_to_hashtable(self,key,value):
		print 'adding record to hashtable with key:',key,' value:',value
		if self.hashtable.has_key(key)==False:
			self.hashtable[key]=[]
		file_name=value['file_name']
		chunk_num=value['chunk_num']
		port=value['port']
		peer_id=value['id']
		new_record={
			'port':port,
			'id':peer_id
		}
		for item in self.hashtable[key]:
			if item.file_name==file_name:
				item.add_to_chunk_ip(chunk_num,new_record)
				for item in self.hashtable[key]:
					print 'balayii'
					item.print_file_records()
				return

		new_file_records=File_records(file_name)
		new_file_records.add_to_chunk_ip(chunk_num,new_record)
		self.hashtable[key].append(new_file_records)
		for item in self.hashtable[key]:
			print 'payiini'
			item.print_file_records()
	def find_value_from_hashtable(self,key,file_name):
		print 'file name in find value from :',file_name
		for item in self.hashtable[key]:
			if item.file_name==file_name:
				ans={
					'file_name':item.file_name,
					'chunk_ip':item.chunk_ip
				}
				return ans
	def self_has_record(self,file_name):
		hash_val=hash_function(file_name)
		if self.hashtable.has_key(hash_val):
			for item in self.hashtable[hash_val]:
				if item.file_name==file_name:
					return True
		return False
	def handle_new_request(self,request_type,key,value,self_requesting,message):
		record_place=self.find_record_place(key)
		print "place="+record_place
		if record_place=='self_node':
			if self_requesting:
				if(request_type=='insert'):
					self.add_record_to_hashtable(key,value)
				elif(request_type=='delete'):
					del self.hashtable[key]
					print self.hashtable
			else:
				if(request_type=='add_node'):
					ans_message = {
						'type':'i_am_your_next',
						'next_id':self.peer_id,
						'next_port':peer_port,
						'prev_id':self.prevpeer_id,
						'prev_port':self.prevpeer_port

					}
					self.prevpeer_id=message["new_node_id"]
					self.prevpeer_port=message["new_node_port"]
					send_message_to_peer(ans_message,'127.0.0.1',self.prevpeer_port)
					self.print_node()
				elif(request_type=='delete'):
					del self.hashtable[key]
					print self.hashtable
				else:
					if(request_type=='insert'):
						ans_message = {
							'type':'accept_insert_req',
							'id':self.peer_id,
							'port':peer_port,
							'record_index':message["index"]
						}
					elif(request_type=='find_value'):
						val=self.find_value_from_hashtable(key,message['file_name'])
						ans_message  = {
							'type':'found_value',
							'key':key,
							'file_name':value,
							'value':val
						}
					port=message['requesting_peer_port']
					send_message_to_peer(ans_message,'127.0.0.1',port)
		else:
			if self_requesting:
				message_type=''
				if request_type=='insert':
					message_type='insert_query'
					pending_insert_record = {
						'index':self.insert_request_num,
						'key':key,
						'value':value
					}
					self.insert_request_num=self.insert_request_num+1
					self.pending_insert_requests.append(pending_insert_record)
					message = {
						'type':message_type,
						'requestuesting_peer_id':self.peer_id,
						'requesting_peer_port':peer_port,
						'key':key,
						'index':self.insert_request_num-1
					}
				elif request_type=='find_value':
					message_type='find_value_query'
					message = {
						'type':message_type,
						'requesting_peer_id':self.peer_id,
						'requesting_peer_port':peer_port,
						'key':key,
						'index':self.insert_request_num-1,
						'file_name':value
					}
				elif request_type=='delete':
					message_type='delete_query'
					message = {
						'type':message_type,
						'requesting_peer_id':self.peer_id,
						'requesting_peer_port':peer_port,
						'key':key,
						'index':self.insert_request_num-1
					}
			if record_place=='next_node':
				self.send_message_to_next_node(message)
			if record_place=='prev_node':
				self.send_message_to_prev_node(message)
	def find_record_place(self,key):
		if (int(key)>=(int(self.peer_id))):
			return 'next_node'
		elif (int(key)<(int(self.peer_id))):
			if (int(key)>=(int(self.prevpeer_id))):
				return 'self_node'
			else :
				return 'prev_node'
	def send_record_to_correct_node(self,accepting_port,pending_record_index):
		record=self.find_record_in_pendings(pending_record_index)
		message = {
			'type':'record_to_add',
			'key':record["key"],
			'value':record["value"]
		}
		send_message_to_peer(message,'127.0.0.1',accepting_port)
	def send_insert_query(self,file_name,chunk_num):
		print 'send_insert_query'
		hash_key=hash_function(file_name)
		print 'file_name : ',file_name
		print 'hash_key : ',hash_key
		value = {
			'file_name':file_name,
			'chunk_num':chunk_num,
			'port':peer_port,
			'id':self.peer_id
		}
		print 'value : ',value
		self.handle_new_request('insert',hash_key,value,True,None)
	def find_record_in_pendings(self,pending_record_index):
		for request in self.pending_insert_requests:
			if pending_record_index==request["index"]:
				found_record = request
				self.pending_insert_requests.remove(request)
				return found_record
	def send_find_records_req(self,file_name):
		hash_val=hash_function(file_name)
		self.handle_new_request('find_value',hash_val,file_name,True,None)
	def give_prev_dynamic_records(self,conn):
		print self.hashtable.keys()
		for key in self.hashtable.keys():
			k1=(int(key))
			print 'k1=-',k1,'-'
			k2=(int(self.prevpeer_id))
			print 'k2=-',k2,'-'
			if k1<k2:
				msg={
					'type':'record_to_add',
					'key':key,
					'value':self.hashtable[key],
				}
				del self.hashtable[key]
				print self.hashtable
				conn.send(json.dumps(msg))
		conn.close()		
class Static_node(Node):
	def __init__(self,peer_id=-1,nextpeer_id=-1,nextpeer_port='',prevpeer_id=-1,prevpeer_port=''):
		print "caling static constructor"
		self.peer_id=peer_id
		self.nextpeer_id=nextpeer_id
		self.nextpeer_port=nextpeer_port
		self.prevpeer_id=prevpeer_id
		self.prevpeer_port=prevpeer_port
		self.insert_request_num=0
		self.hashtable={}
		self.pending_insert_requests=[]
class Dynamic_node(Node):
	def __init__(self,file_path):
		print "caling dynamic constructor"
		answer_from_statics=select_id_from_statics(file_path)
		self.peer_id=answer_from_statics["id"]
		message = {
			'type':'add_node',
			'new_node_id':self.peer_id,
			'new_node_port':peer_port
		}
		send_message_to_peer(message,'127.0.0.1',answer_from_statics['prev_static_port'])
		dynamic_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
		dynamic_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
		dynamic_socket.bind((TCP_IP, TCP_PORT)) 
		dynamic_socket.listen(1) 
		(conn, (ip,port)) = dynamic_socket.accept() 
		print ip, port
		data = conn.recv(2048)
		print "dynamic socket received data:", data, "-"
		load_data = json.loads(data)
		self.prevpeer_id=load_data['prev_id']
		self.nextpeer_id=load_data['next_id']
		self.nextpeer_port=load_data['next_port']
		self.prevpeer_port=load_data['prev_port']
		conn.close()
		dynamic_socket.close()
		self.insert_request_num=0
		self.hashtable={}
		self.ask_for_records()
		self.pending_insert_requests=[]
    	
	def ask_for_records(self):
		message = {
    		'type':'ask_for_records'
    	}
		next_node_socket=create_socket('127.0.0.1',self.nextpeer_port)
		next_node_socket.send(json.dumps(message))
		print 'waiting for record'
		while True:
			data=next_node_socket.recv(2048)
			if data:
				print "Dynamic received record:", data, "-"
				load_data = json.loads(data)
				self.add_record_to_hashtable(load_data["key"],load_data["value"])
			else:
				break
    	

client_node=Node()
#added by ali

class CmdThread(Thread):
	def __init__(self):
		Thread.__init__(self)
		print "++++ new CmdThread! ++++"
	def run(self):
		while True:
			cmd_input = raw_input()
			print "CmdThread: " , "-" + cmd_input + "-"
			command = cmd_input.split(" ")[0]
			# print "-" + cmd_input.split(" ")[0] + "-" +  cmd_input.split(" ")[1]+"-"
			if command == "upload":
				file_path = cmd_input.split(" ")[1]
				print "uploading  -" + file_path +"-"
				upload(file_path)
			elif command == "find_value":
				file_name = cmd_input.split(" ")[1]
				client_node.send_find_records_req(file_name)
			elif command == 'download':
				info_file_path=cmd_input.split(" ")[1]
				start_download(info_file_path)
			else:
				print "UNKONWN COMMAND!"



class ClientThread(Thread): 
    def __init__(self,ip,port): 
        Thread.__init__(self) 
        self.ip = ip 
        self.port = port 
        print "[+] New server socket thread started for " + ip + ":" + str(port) 
    
    def send_chunk(self,conn, file_name, chunk_num):
    	directory_name = file_name.split(".")[0]
    	print 'file_name in send chunk :-'+file_name+'-'
    	chunk_path = directory_name + "/" + file_name + '-'+to_four_digit(chunk_num)
    	print "chunk_path = -" + chunk_path + "-"
    	f = open(chunk_path.encode('ascii'), 'r')
    	content = f.read(2048)
    	while (content):
    		conn.send(content)
    		content = f.read(2048)
    	f.close()
    
    def run(self):  
        data = conn.recv(2048) 
        if data:
            print "Server received data:", data, "-"
            clean_data = data[:-2]
            print "clean_data ="+clean_data
            if clean_data.split(" --")[0]=='find_value':
            	key=clean_data.split(" --")[1]
            	client_node.handle_new_request('find_value',key,None,True,None)
            elif clean_data.split(" --")[0]=='delete':
            	key=clean_data.split(" --")[1]
            	client_node.handle_new_request('delete',key,None,True,None)
            else:
	            load_data = json.loads(data)
	            req_type = load_data["type"]
	            print "request type= "+req_type+'-'
	            if req_type == 'prev_id_port' :
	            	prev_id=load_data['id']
	            	prev_port=load_data['port']
	            	client_node.set_prev_id_port(prev_id,prev_port)
	            elif req_type == 'next_id_port':
	            	next_id=load_data['id']
	            	next_port=load_data['port']
	            	client_node.set_next_id_port(next_id,next_port)
	            	client_node.print_node()
	            elif req_type == 'insert_query' :
	            	key=load_data['key']
	            	client_node.handle_new_request('insert',key,None,False,load_data)
	            elif req_type == 'find_value_query':
	            	key=load_data['key']
	            	client_node.handle_new_request('find_value',key,None,False,load_data)
	            elif req_type == 'get_chunk':
	            	file_name = load_data["file_name"]
	            	chunk_num = load_data["chunk_num"]
	            	self.send_chunk(conn, file_name, chunk_num)
	            elif req_type == 'accept_insert_req' :
	            	accepting_id=load_data["id"]
	            	accepting_port=load_data["port"]
	            	pending_record_index=load_data["record_index"]
	            	print 'accepted from node',accepting_id
	            	client_node.send_record_to_correct_node(accepting_port,pending_record_index)
	            elif req_type == 'ask_for_records':
	            	client_node.give_prev_dynamic_records(conn)
	            elif req_type == 'record_to_add' :
	            	client_node.add_record_to_hashtable(load_data['key'],load_data['value'])
	            elif req_type == 'add_node':
	            	new_node_id=load_data["new_node_id"]
	            	client_node.handle_new_request('add_node',new_node_id,None,False,load_data)
	            elif req_type == 'delete_query':
	           		key=load_data["key"]
	           		client_node.handle_new_request('delete',key,None,False,load_data)
	            elif req_type == 'found_value':
	           		key=load_data["key"]
	           		value=load_data["value"] #value oon dictionary e javabas 
	           		print 'found value ',value['chunk_ip'], 'for key',key,'for file',value['file_name']
	           		download_file(value['file_name'],value['chunk_ip'], 0)
	           		#download o inja bezan
	            conn.close()











TCP_IP = '127.0.0.1' 
TCP_PORT = randint(10000,20000)
peer_port=TCP_PORT
print "peer_port = ",peer_port
BUFFER_SIZE = 20  # Usually 1024, but we need quick response 

if sys.argv[1]=='--static' :
	print 'creating static node'
	if len(sys.argv)==4:
		peer_id=sys.argv[3]
		new_node = Static_node(peer_id)
		client_node=new_node
	elif len(sys.argv)==8:
		peer_id=sys.argv[3]
		nextpeer_id=int(sys.argv[5])
		nextpeer_port=sys.argv[7]
		new_node=Static_node(peer_id,nextpeer_id,nextpeer_port)
		client_node=new_node
		message=new_node.create_message_id_for_next_peer()
		client_node.send_message_to_next_node(message)
	add_to_static_nodes_file(client_node.peer_id,peer_port)
elif sys.argv[1]=='--dynamic' :
	print 'creating dynamic node'
	file_path=sys.argv[2]
	new_node=Dynamic_node(file_path)
	client_node=new_node
	client_node.print_node()
	message=client_node.create_message_id_for_prev_peer()
	print message
	client_node.send_message_to_prev_node(message)
client_node.print_node()
 
tcpServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
tcpServer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
tcpServer.bind((TCP_IP, TCP_PORT)) 
threads = []

#added by ali
newthread = CmdThread()
newthread.start()
threads.append(newthread)

while True: 
    tcpServer.listen(4) 
    print "Multithreaded Python server : Waiting for connections from TCP clients..." 
    (conn, (ip,port)) = tcpServer.accept() 
    print ip, port
    newthread = ClientThread(ip,port) 
    newthread.start() 
    threads.append(newthread) 
 
for t in threads: 
    t.join() 
