#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#

import sys
import zmq
import time

####################################################################
# Start REP server 
####################################################################
def start_repserver():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")

    print("REP Server startup...")

    while True:
        message = socket.recv_string()
        print("Received request: {}".format(message))
        time.sleep(1)
        socket.send_string("Hi, {}".format(message))
    # clean up
    socket.close()
    context.destroy()


if __name__ == '__main__':
    start_repserver()