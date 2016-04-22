import socket
import pickle
from collections import deque
from job import Job


TOTAL_ELEMENTS = 1024*1024*4
JOB_COUNT = 512
SIZE_OF_JOB = int(TOTAL_ELEMENTS / JOB_COUNT)


def process_jobs(jobs_list):
    for job in jobs_list:
        job.vector_add()


def transfer_manager(jobs):
    half = int(JOB_COUNT / 2)
    first_half_jobs = deque(jobs[0:half])
    second_half_jobs = deque(jobs[half:JOB_COUNT])

    # store job list as a byte stream
    pickled_jobs = pickle.dumps(second_half_jobs)
    # get the length of the byte stream
    length = len(pickled_jobs).to_bytes(8, byteorder='big')

    # send the size of the byte stream to the server so it knows how large the jobs are
    s.send(length)
    # send the jobs
    s.send(pickled_jobs)
    for job in second_half_jobs:
        print("Sending job with ID {}".format(job.id))


    # TODO: Process jobs and wait for the server to send the data back.


# create a socket and connect to the server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 8040))

# initialize the vector
A = [1.111111] * TOTAL_ELEMENTS
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




