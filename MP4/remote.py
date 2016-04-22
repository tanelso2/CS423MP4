import socket
import pickle
import threading
import time
import queue
import psutil
from state import State
from job import Job

get_current_time = lambda:int(round(time.time() * 1000))

TOTAL_ELEMENTS = 1024*1024*4
JOB_COUNT = 512
SIZE_OF_JOB = int(TOTAL_ELEMENTS / JOB_COUNT)


class RemoteNode:
    def __init__(self, remote):
        self.throttling = 0
        self.throttle_lock = threading.Lock()
        self.worker_event = threading.Event()
        self.job_queue = queue.Queue()
        self.finish_jobs = []
        self.cpu_use = psutil.cpu_percent()
        self.remote_state = None

        self.transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.state_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if remote:
            self.transfer_socket.bind(("localhost", 8040))
            self.transfer_socket.listen(1)
            self.state_socket.bind(("localhost", 8041))
            self.state_socket.listen(1)
            # Accept connections
            (clientsocket, address) = self.state_socket.accept()
            self.state_socket = clientsocket
            (clientsocket, address) = self.transfer_socket.accept()
            self.transfer_socket = clientsocket
        else:
            self.state_socket.connect(("localhost", 8040))
            self.transfer_socket.connect(("localhost", 8041))

        self.transfer_thread = threading.thread(target=self.transfer_manager)
        self.worker_thread = threading.Thread(target=self.worker_thread_manager)
        self.state_thread = threading.Thread(target=self.state_manager)
        self.hardware_thread = threading.Thread(target=self.hardware_monitor)
        self.input_thread = threading.Thread(target=self.input_manager)

        self.transfer_thread.start()
        self.worker_thread.start()
        self.state_thread.start()
        self.hardware_thread.start()
        self.input_thread.start()

        if not remote:
            self.bootstrap()


    def bootstrap(self):
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

        half = int(JOB_COUNT / 2)
        first_half_jobs = list_of_jobs[0:half]
        second_half_jobs = list_of_jobs[half:JOB_COUNT]

        for job in first_half_jobs:
            self.job_queue.put(job)

        self.transfer_manager_send(second_half_jobs)

    def worker_thread_manager(self):
        while not self.worker_event.isSet():
            job = self.job_queue.get()
            job.vector_add()
            self.finish_jobs.append(job)

        self.throttle_lock.acquire()
        timer = threading.Timer((100 - self.throttling) / 1000, self.start_worker)
        self.throttle_lock.release()
        timer.start()

    def terminate_worker(self):
        self.worker_event.set()

    def start_worker(self):
        self.worker_thread = threading.Thread(target=self.worker_thread_manager)
        self.throttle_lock.acquire()
        timer = threading.Timer(self.throttling / 1000, self.terminate_worker)
        self.throttle_lock.release()
        timer.start()

    def adaptor(self):
        self.transfer_manager_send()
        self.state_manager_send()

    def state_manager_receive(self):
        while True:
            BUFFER_SIZE = self.state_socket.recv(8)
            pickled_data = self.state_socket.recv(int.from_bytes(BUFFER_SIZE, byteorder='big'))
            self.remote_state = pickle.loads(pickled_data)


    def state_manager_send(self):
        state = State( self.job_queue.qsize(), self.throttling, self.cpu_use)
        pickled_data = pickle.dumps(state)
        length = len(pickled_data).to_bytes(8, byteorder='big')

        self.state_socket.send(length)
        self.state_socket.send(pickled_data)


    def hardware_monitor(self):
        self.cpu_use = psutil.cpu_percent()
        timer = threading.Timer(1, self.hardware_monitor)
        self.adaptor()

    def transfer_manager_receive(self):
        while True:
            # turn the byte stream into a list of jobs
            BUFFER_SIZE = self.transfer_socket.recv(8)
            pickled_data = self.transfer_socket.recv(int.from_bytes(BUFFER_SIZE, byteorder='big'))
            jobs_list = pickle.loads(pickled_data)
            for job in jobs_list:
                self.job_queue.put(job)
                print("Received job {}".format(job.id))

    def transfer_manager_send(self, data):
        pickled_data = pickle.dumps(data)
        length = len(pickled_data).to_bytes(8, byteorder='big')

        self.transfer_socket.send(length)
        self.transfer_socket.send(pickled_data)

    def input_manager(self):
        while 1:
            try:
                value = int(input("Enter a throttling value from 0 to 100."))
                if value < 0 or value > 100:
                    raise ValueError
                else:
                    self.throttle_lock.acquire()
                    self.throttling = value
                    self.throttle_lock.release()
            except ValueError:
                print("Value must be an integer between 0 and 100. Try again.")

