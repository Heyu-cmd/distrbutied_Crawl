import socket
import sys
import threading

import signal

class ServerSocket:

    def __init__(self, callback, host='localhost', port=20011):
        """

        :param callback: callback function for handling received data
        :param host: Symbolic name meaning all available interface
        :param port: Arbitray non-privilaged port
        """
        self.threads = []
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.callback = callback
        # print("socket created")

        # bind socket to local host and port
        try:
            self.s.bind((host,port))
        except socket.error as msg:
            print(" Bind failed. Error code : "+str(msg[0]) + " Message " + str(msg[1]))
            sys.exit()

        # print("socket bind complete")

        # socket listening
        self.s.listen(10)
        print("socket now listening")
    def start_new_thread(self,function, args):
        t = threading.Thread(target=function, args=args)
        self.threads.append(t)
        t.setDaemon(True)
        t.start()
    def startlistening(self):
        # now keep tackling with the client
        while True:

            # 阻塞，监听新的请求过来
            conn, addr = self.s.accept()

            # 创建新的线程来处理请求
            self.start_new_thread(self.clientthread, (conn,))


    def clientthread(self, conn):
        """
        function for handling connection, this will be used to create thread
        :param conn:
        :return:
        """
        data = conn.recv(1024).decode()
        print("data from client ------"+ str(data))

        reply = self.callback(data)
        print('reply from server ------' + reply)

        conn.sendall(reply.encode())

        conn.close()
    def start(self):
        self.startlistening()


    def close(self):
        # self.s.shutdown(socket.SHUT_WR)
        self.s.close()

def msg_received(data):
    return 'Ack'

def exit_signal_handler(signal, frame):
    pass

if __name__ == '__main__':
    server = ServerSocket(msg_received)
    server.start()
    signal.signal(signal.SIGINT, exit_signal_handler)
    signal.pause()
    signal.close()
    sys.exit(1)

