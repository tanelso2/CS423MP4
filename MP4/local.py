import socket
import pickle
from job import Job


TOTAL = 1024*1024*4
HALF = TOTAL / 2
JOB_COUNT = 512
SIZE_OF_JOB = int(TOTAL / JOB_COUNT)


def transfer_manager(jobs):
    half = int(JOB_COUNT / 2)
    first_half_jobs = jobs[0:half]
    second_half_jobs = jobs[half:JOB_COUNT]
    pickled_jobs = pickle.dumps(second_half_jobs)
    length = len(pickled_jobs).to_bytes(8, byteorder='big')
    s.send(length)
    s.send(pickled_jobs)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 8040))


# initialize the vector
A = [1.111111] * TOTAL
list_of_jobs = []
id_count = 0
start_index = 0

# create jobs
for i in range(JOB_COUNT):
    list_of_jobs.append(Job(id_count, start_index, A[start_index:(start_index + SIZE_OF_JOB)]))
    id_count += 1
    start_index += SIZE_OF_JOB

# process jobs
transfer_manager(list_of_jobs)




