import time
import json
from socket_server import ServerSocket
from mongo_manager import MongoManager
from pc_protocol import PC
import signal
import sys

pc = PC()
contants = {
    'reorder_period': 1200,  # 20 mins
    'connection_lost_period': 30,  # 30s
}

class CrawlMaster:
    clients = {}
    server_status = pc.STATUS_RUNNING

    last_reorder_time = time.time()

    mongo_mgr = MongoManager()

    def __init__(self, mongo_client=None, mongo_host='localhost'):
        self.server = ServerSocket(callback=self.on_message)

        self.server.start()

    def on_message(self, msg):
        self.period_check()
        print('Heart Beat request: ' + msg)
        request = json.loads(msg)
        msg_type = request[pc.MSG_TYPE]
        client_state = {}
        response = {}
        response[pc.SERVER_STATUS] = self.server_status
        if msg_type == pc.REGISTER:
            # 注册
            client_id = self.get_free_id()
            client_state['status'] = pc.STATUS_RUNNING
            client_state['time'] = time.time()
            self.clients[client_id] = client_state
            return client_id
        elif msg_type == pc.UNREGISTER:
            cliend_id = request.get(pc.CLIENT_ID)
            del self.clients[cliend_id]
            return json.dumps(response)
        # elif msg_type == pc.LOCATIONS:
        #     print("type: pc.LOCATION ------")
        #     print(request)
        #     print(pc.REQUEST_SIZE)
        #     print(request[pc.CLIENT_ID])
        #
        #     items = []
        #
        #     print(items)
        #     response[pc.MSG_TYPE] = pc.LOCATIONS
        #     response[pc.CRAWL_DELAY] = 2
        #     response[pc.DATA] = json.dumps(items)
        #     return json.dumps(response)
        # elif msg_type == pc.TRIPLES:
        #     items = []
        #     response[pc.MSG_TYPE] = pc.LOCATIONS
        #     response[pc.DATA] = json.dumps(items)
        #     return json.dumps(response)
        client_id = request.get(pc.CLIENT_ID)
        if client_id == None:
            response[pc.ERROR] = pc.ERR_NOT_FOUND
            return json.dumps(response)
        if msg_type == pc.HEARTBEAT:
            if self.server_status is not self.clients[client_id]['status']:
                if self.server_status == pc.STATUS_RUNNING:
                    response[pc.ACTION_REQUIRED] = pc.RESUMED_REQUIRED
                elif self.server_status == pc.STATUS_PAUSED:
                    response[pc.ACTION_REQUIRED] = pc.PAUSED_REQUIRED
                elif self.server_status == pc.STATUS_SHUTDOWN:
                    response[pc.ACTION_REQUIRED] = pc.SHUT_DOWN_REQUIRED
            client_state['status'] = pc.STATUS_RUNNING
            client_state['time'] = time.time()
            self.clients[client_id] = client_state
            return json.dumps(response)
        else:
            client_state['status'] = msg_type
            client_state['time'] = time.time()
            self.clients[client_id] = client_state
        return json.dumps(response)
    def get_free_id(self):
        i = 0
        for key in self.clients:
            if i < int(key):
                break
            i += 1
        return str(i)

    def period_check(self):
        client_status_ok = True
        print("period check start")
        print(self.clients)
        for cid in self.clients.keys():
            print("cid: " + cid)
            state = self.clients[cid]
            # no heart beat for 2 mins, remove it
            if time.time() - state['time'] > contants['connection_lost_period']:
                # 检查每个spider上一次连接的时间，如果时间间隔超过则改变spider状态
                state['status'] = pc.STATUS_CONNECTION_LOST
                continue
            if state['status'] == self.server_status:
                client_status_ok = False
                break
        print("period check over")

if __name__ == '__main__':
    crawl_master = CrawlMaster()
    crawl_master.period_check()
    signal.signal(signal.SIGINT, exit_signal_handler)
    signal.pause()
    signal.close()
    sys.exit(1)

