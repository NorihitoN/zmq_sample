#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import sys
import zmq

def start_reqserver():

    context = zmq.Context()

    #  Socket to talk to server
    print("Connecting to hello world server…")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")

    while True: 
        print("Enter message:")
        message = sys.stdin.readline()
        socket.send_string(message)
        #  Get the reply.
        recv_message = socket.recv_string()
        print("Received reply {} : {}".format(message.replace('\n',''), recv_message))

    socket.close()
    context.destroy()

if __name__ == "__main__":
    start_reqserver()