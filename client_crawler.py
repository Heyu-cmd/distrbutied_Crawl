# 视频第六课 52.02
# 主线程，爬虫线程，心跳线程（负责与服务端进行通信）
# 当主线程获取到任务时，起一个爬虫线程，负责爬虫线程的维护和检查
import argparse
import json
import time
import socket
import threading
import requests
from lxml import etree
from redis_manager import RedisManager
from socket_client import SocketClient
from mongo_manager import MongoManager
from pc_protocol import PC


parser = argparse.ArgumentParser(prog='CrawlerClient', description='Start a crawler client')
parser.add_argument('-S', '--host-all', type=str, nargs=1, help='Host server for all services')
parser.add_argument('-s', '--host', type=str, nargs=1, help='Crawler host server address,default is localhost')
parser.add_argument('-p', '--host-port', type=int, nargs=1, help='Crawler host server port number,default is xx')
parser.add_argument('-m', '--mongo', type=str, nargs=1, help='Mongo host server address,default is localhost')
parser.add_argument('-n', '--mongo-port', type=int, nargs=1, help='Mongo port number,default is 27017')
parser.add_argument('-r', '--redis', type=str, nargs=1, help='redis server address,default is localhost')
parser.add_argument('-x', '--redis-port', type=int, nargs=1, help='redis port number,default is 6379')


def get_page_content(task):
    global mongo_manager
    print(task)
    print("getting")
    ids = []
    print("downloading %s" % (task['url']))
    ids.append(task['id'])
    try:
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }

        response = requests.get('http://www.52xxit.com/thread-3789-1-1.html', headers=headers, verify=False)
        html = response.content.decode("gbk")

        dom = etree.HTML(html)

        title = dom.xpath("./span[@id='thread_subject']/text()")[0]
        domin = dom.xpath("./div[@id='pt']/div/a")[3]
        pub_time = dom.xpath()


    except Exception:
        pass
    mongo_manager.finishItems(ids)


def heart_beat():
    # run_heartbeat 检查心跳是否要退出
    # 单独的线程，独立的完成心跳
    print("heart beat !!!")
    global server_status, run_heartbeat, client_id
    skip_wait = False
    while run_heartbeat:
        if skip_wait is False:
            time.sleep(hb_period)
        else:
            skip_wait = False
        try:
            hb_request = {}
            hb_request[pc.MSG_TYPE] = pc.HEARTBEAT
            hb_request[pc.CLIENT_ID] = client_id  # 第一次创建爬虫连接时得到clientid

            hb_response_data = socket_client.send(json.dumps(hb_request).encode()).decode()

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
                    client_id = socket_client.send(json.dumps(register_request))

                    # skip heartbeat period and send next heartbeat immediatly
                    skip_wait = True
                    heart_beat()
                    return
                return

            # 如果客户端已经创建过了，就查看服务器返回来的命令
            action = response.get(pc.ACTION_REQUIRED)
            if action is not None:
                action_request = {}
                if action == pc.PAUSE_REQUIRED:
                    server_status = pc.PAUSED
                    action_request[pc.MSG_TYPE] = pc.PAUSED
                elif action == pc.RESUMED_REQUIRED:
                    server_status = pc.RESUMED
                    action_request[pc.MSG_TYPE] = pc.RESUMED
                elif action == pc.SHUTDOWN_REQUIRED:
                    # server_status 改了之后会影响到主线程
                    server_status = pc.SHUTDOWN
                    # stop heartbeat thread
                action_request[pc.CLIENT_ID] = client_id
                socket_client.send(json.dumps(action_request).encode())
            else:
                server_status = response[pc.SERVER_STATUS]

        except socket.error as msg:
            print('Send Data Error. Error Code :' + str(msg[0]) + ' Message ' + msg[1])
            server_status = pc.STATUS_CONNeCTION_LOST

def get_task():
    global hb_period
    global curtask
    while True:
        if curtask is None:
            curtask = mongo_manager.dequeueUrl(20)
        else:
            time.sleep(hb_period)
# thread pool size
max_num_thread = 5

# db manager
redis_manager = RedisManager()
mongo_manager = MongoManager()

# # 开始抓取的第一个网页
# redis_manager.enqueueUrl(url='http://www.mafengwo.cn', status='new')

start_time = time.time()
is_root_page = True
threads = []
CRAWL_DELAY = 1

pc = PC()
hb_period = 5
run_heartbeat = True
server_status = pc.STATUS_RUNNING

# use hdfs to save pages
# hdfs_clent = ...


socket_client = SocketClient('localhost', 20011)
register_request = {}
register_request[pc.MSG_TYPE] = pc.REGISTER
client_id = socket_client.send(json.dumps(register_request).encode()).decode()

curtask = []

try:

    t = threading.Thread(target=heart_beat, name=None)
    # set daemon so main thread can exit when receives ctrl-c
    t.setDaemon(True)
    t.start()
except Exception:
    print('Error : unable to start thread_heart_beat')
try:

    t2 = threading.Thread(target=get_task, name=None)
    t2.setDaemon(True)
    t2.start()
except Exception:
    print('Error : unable to start thread get_task')
while True:
    if server_status == pc.STATUS_PAUSED:
        time.sleep(hb_period)
        continue
    if server_status == pc.STATUS_SHUTDOWN:
        run_heartbeat = False
        for t in threads:
            t.join()
        break


    # take lists from mongo
    curtask = mongo_manager.dequeueUrl(20)
    # print(curtask)
    # print("cur task is: " + json.dumps(curtask))
    # Go on next level. before that, needs to wait all current level crawling done
    if curtask is None:
        time.sleep(hb_period)
        continue

    # loking for an empty thread from pool to crawl

    if is_root_page is True:

        # get_page_content()
        is_root_page = False
    else:
        while True:
            # first remove all finished running threads
            for t in threads:
                if not t.is_alive():
                    threads.remove(t)
            if len(threads) >= max_num_thread:
                time.sleep(CRAWL_DELAY)
                continue
            try:
                # curtask = mongo_manager.dequeueUrl(1)
                t = threading.Thread(target=get_page_content, name=None,
                                     args=(curtask.pop(),))
                threads.append(t)
                # set daemon so main thread can exit when receives ctrl-c
                t.setDaemon(True)
                t.start()

                time.sleep(CRAWL_DELAY)
                break
            except Exception:
                print('Error : unable to start thread_crawl_contant')

shutdown_request = {}
shutdown_request[pc.MSG_TYPE] = pc.SHUTDOWN
shutdown_request[pc.CLIENT_ID] = cliend_id
socket_client.send(json.dumps(shutdown_request))
