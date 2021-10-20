import socket

class SocketClient:
    def __init__(self, server_ip="localhost", server_port=20011):
        self.server_ip = server_ip
        self.server_port = server_port

        self.families = self.get_contants('AF_')
        self.types = self.get_contants('SOCK_')
        self.protocals = self.get_contants('IPPROTO_')



    def get_contants(self, prefix):
        """
        create a dict mapping socket module constants to their names.
        :param prefix:
        :return:
        """
        return dict(
            (getattr(socket,n ),n)
            for n in dir(socket)
            if n.startswith(prefix)
        )

    def send(self, message):
        try:

            self.sock = socket.create_connection((self.server_ip, self.server_port))

            self.sock.sendall(message)

            data = self.sock.recv(1024)
            return data
        except socket.error as msg:
            print(" Bind failed. Error code : " + str(msg[0]) + " Message " + str(msg[1]))
            if msg[0] == 61:
                return None
        finally:
            if hasattr(self, 'sock'):
                self.sock.close()
