import socket
import pickle
from job import Job

# create an INET, STREAMing socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("localhost", 8040))
s.listen(1)

while True:
    # accept connections from outside
    (clientsocket, address) = s.accept()
    BUFFER_SIZE = clientsocket.recv(8)
    pickled_data = clientsocket.recv(int.from_bytes(BUFFER_SIZE, byteorder='big'))
    jobs_list = pickle.loads(pickled_data)
    for job in jobs_list:
        job.print_id()

