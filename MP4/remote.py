import socket
import pickle
import threading
import psutil

from state import State
from job import Job

TOTAL_ELEMENTS = 1024*1024*4
JOB_COUNT = 512
SIZE_OF_JOB = int(TOTAL_ELEMENTS / JOB_COUNT)

class Node:
    def __init__(self, remote):
        self.remote = remote
        self.throttling = 0
        self.job_queue = []
        self.processed_jobs = []
        self.cpu_use = psutil.cpu_percent()
        self.remote_state = None

        # Sockets
        self.transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.state_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if self.remote:
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

        # Threads
        self.throttle_lock = threading.Lock()
        self.job_queue_lock = threading.Lock()
        self.processed_jobs_lock = threading.Lock()
        self.worker_event = threading.Event()

        self.transfer_thread = threading.thread(target=self.transfer_manager)
        self.worker_thread = threading.Thread(target=self.worker_thread_manager)
        self.state_thread = threading.Thread(target=self.state_manager)
        self.hardware_thread = threading.Thread(target=self.hardware_monitor)
        self.input_thread = threading.Thread(target=self.input_manager)

        # Initialize jobs
        if not self.remote:
            self.bootstrap()

        self.transfer_thread.start()
        self.worker_thread.start()
        self.state_thread.start()
        self.hardware_thread.start()
        self.input_thread.start()

    def bootstrap(self):
        # initialize the vector
        A = [1.111111] * TOTAL_ELEMENTS

        # create jobs
        all_jobs = [Job(i, i * SIZE_OF_JOB, A[i * SIZE_OF_JOB : i * SIZE_OF_JOB + SIZE_OF_JOB]) for i in range(JOB_COUNT)]

        self.job_queue = all_jobs[0 : int(JOB_COUNT / 2)]

        self.transfer_manager_send(all_jobs[int(JOB_COUNT / 2) : JOB_COUNT])

    def worker_thread_manager(self):
        # Set the first cycle
        self.throttle_lock.acquire()
        sleep = threading.Timer(self.throttling / 1000.0, lambda: self.worker_event.set())
        self.throttle_lock.release()
        sleep.start()

        # Loop to process jobs/sleep/wake
        while True:
            # Throttling
            if self.worker_event.isSet():
                # Set timer to clear event in the time we should sleep
                self.throttle_lock.acquire()
                wake = threading.Timer((100 - self.throttling) / 1000.0, lambda: self.worker_event.clear())
                self.throttle_lock.release()

                # Wait for the event to wake up
                wake.start()
                self.worker_event.wait()

                # Set timer to sleep in the time we should throttle
                self.throttle_lock.acquire()
                sleep = threading.Timer(self.throttling / 1000.0, lambda: self.worker_event.set())
                self.throttle_lock.release()
                sleep.start()

            # Process a job
            self.job_queue_lock.acquire()
            job = self.job_queue.pop()
            self.job_queue_lock.release()

            job.vector_add()

            self.processed_jobs_lock.acquire()
            self.processed_jobs.append(job)
            self.processed_jobs_lock.release()

    def num_jobs_to_send(self):
        local_state = self.state()
        remote_state = self.remote_state
        return (local_state.num_jobs * local_state.throttle_value * local_state.cpu_use - remote_state.num_jobs * remote_state.throttle_value * remote_state.cpu_use) / (local_state.throttle_value * local_state.cpu_use + remote_state.throttle_value * remote_state.cpu_use)

    def adaptor(self):
        # Calculate ratio of loads
        num_jobs_to_send = self.num_jobs_to_send()
        if num_jobs_to_send > 0:
            # Send some of our jobs over
            self.job_queue_lock.acquire()
            jobs_to_send = self.job_queue[-num_jobs_to_send :]
            self.job_queue = self.job_queue[: len(self.job_queue) - num_jobs_to_send]
            self.job_queue_lock.release()
            self.transfer_manager_send(jobs_to_send)

        # Send processed jobs to local over transfer manager
        if self.remote:
            self.processed_jobs_lock.acquire()
            self.transfer_manager_send(self.processed_jobs)
            self.processed_jobs = []
            self.processed_jobs_lock.release()

        # Send our state
        self.state_manager_send()

    def state_manager_receive(self):
        while True:
            BUFFER_SIZE = self.state_socket.recv(8)
            pickled_data = self.state_socket.recv(int.from_bytes(BUFFER_SIZE, byteorder='big'))
            self.remote_state = pickle.loads(pickled_data)

    def state_manager_send(self):
        pickled_data = pickle.dumps(self.state())
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
            jobs_recvd = pickle.loads(pickled_data)

            # If the jobs are processed, put into finished jobs (local only)
            if jobs_recvd[0].complete is True:
                self.processed_jobs_lock.acquire()
                self.processed_jobs.extend(jobs_recvd)
                self.processed_jobs_lock.release()
            else:
                # Otherwise we are receiving jobs to process
                for job in jobs_recvd:
                    self.job_queue_lock.acquire()
                    self.job_queue.append(job)
                    self.job_queue_lock.release()

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

    def state(self):
        self.job_queue_lock.acquire()
        state = State(len(self.job_queue), self.throttling, self.cpu_use)
        self.job_queue_lock.release()
        return state