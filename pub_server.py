#
#   Pub server in Python
#   

import zmq
import time

def start_pubserver():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5556")

    print("Pub server startup...")

    i = 0
    while True:
        i += 1
        for ch in range(1,4):
            data = ch * i
            socket.send_string("{0} {1}".format(ch, data))
            print("Ch {0} <- {1} sent".format(ch, data))
        time.sleep(1)

    socket.close()
    context.destroy()

if __name__ == "__main__":
    start_pubserver()