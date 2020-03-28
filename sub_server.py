import sys
import zmq

def start_subserver():

    if(len(sys.argv) != 2):
        print("Usage: # python {} <channel>".format(sys.argv[0]))
        sys.exit(1)

    ch = sys.argv[1]
    print("Connecting to pub server <channel> %s" % ch)

    context = zmq.Context()

    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5556")

    socket.setsockopt_string(zmq.SUBSCRIBE, ch)

    while True:
        string = socket.recv_string()
        ch, data = string.split()
        print("Ch {0} -> {1} recieved".format(ch, data))

    socket.close()
    context.destroy()

if __name__ == "__main__":
    start_subserver()