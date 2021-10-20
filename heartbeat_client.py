from .socket_client import SocketClient
import json
import threading
import time
import socket
class HeartBeat:

    server_status = pc.STATUS_RUNNING
    run_heartbeat = False
    client_id = -1
    hb_period = 5
    socket_client = SocketClient('localhost', 20011)

    def __init__(self):
        self.run_heartbeat = False

    def connect(self):
        register_request = {}
        register_request[pc.MSGTYPE] = pc.REGISTER
        self.client_id = self.socket_client.send(json.dumps(register_request))
        if self.client_id is None:
            raise IOError('Connection Failed')

    def disconnect(self):
        unregister_request = {}
        unregister_request[pc.MSGTYPE] = pc.UNREGISTER
        self.client_id = self.socket_client.send(json.dumps(unregister_request))

    def start(self):
        try:
            t = threading.Thread(target=self.heatbeat, name=None)

            t.setDaemon(True)
            t.start()
        except Exception:
            print("Error: unable to start thread")

    def heartbeat(self):
        skip_wait = False
        while self.run_heartbeat:
            if skip_wait is False:
                time.sleep(self.hb_period)
            else:
                skip_wait = False
            try:
                hb_request = {}
                hb_request[pc.MSG_TYPE] = pc.HEARTBEAT
                hb_request[pc.CLIENT_ID] = self.client_id  # 第一次创建爬虫连接时得到clientid
                hb_response_data = self.socket_client.send(json.dumps(hb_request))

                # should be network error
                if hb_response_data is None:
                    continue
                # print 'Heart Beat response' + json.dumps(hb_response_data)
                response = json.loads(hb_response_data)

                # 如果服务器返回的数据是error
                err = response.get(pc.ERROR)
                if err is not None:
                    # 服务端没有爬虫客户端的id
                    if err == pc.ERR_NOT_FOUND:
                        register_request = {}
                        register_request[pc.MSG_TYPE] = pc.REGISTER
                        self.client_id = self.socket_client.send(json.dumps(register_request))

                        # skip heartbeat period and send next heartbeat immediatly
                        skip_wait = True
                        self.heart_beat()
                        return
                    return

                # 如果客户端已经创建过了，就查看服务器返回来的命令
                action = response.get(pc.ACTION_REQUIRED)
                if action is not None:
                    action_request = {}
                    if action == pc.PAUSE_REQUIRED:
                        self.server_status = pc.PAUSED
                        action_request[pc.MSG_TYPE] = pc.PAUSED
                    elif action == pc.RESUMED_REQUIRED:
                        self.server_status = pc.RESUMED
                        action_request[pc.MSG_TYPE] = pc.RESUMED
                    elif action == pc.SHUTDOWN_REQUIRED:
                        # server_status 改了之后会影响到主线程
                        self.server_status = pc.SHUTDOWN
                        # stop heartbeat thread
                        return
                    action_request[pc.CLIENT_ID] = self.client_id
                    self.socket_client.send(json.dumps(action_request))
                else:
                    server_status = response[pc.SERVER_STATUS]

            except socket.error as msg:
                print('Send Data Error. Error Code :' + str(msg[0]) + ' Message ' + msg[1])
                self.server_status = pc.STATUS_CONNeCTION_LOST

    def get_target_items(self, msg_type):
        hb_request = {}
        hb_request[pc.MSG_TYPE] = msg_type
        hb_request[pc.CLIENT_ID] = self.client_id
        response = self.socket_client.send(json.dumps(hb_request))
        return json.loads(response)

    def finish_target_items(self, msg_type, items):
        hb_request = {}
        hb_request[pc.MSG_TYPE] = msg_type
        hb_request[pc.CLIENT_ID] = self.client_id
        hb_request[pc.FINISHED_ITEMS] = json.dumps(items)
        response = self.socket_client.send(json.dumps(hb_request))
        return json.loads(response)