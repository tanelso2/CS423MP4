import socket
import pickle
import threading
import time
import queue
import psutil
from job import Job

get_current_time = lambda:int(round(time.time() * 1000))

class RemoteNode:
    def __init__(self):
        self.throttling = 0
        self.throttle_lock = threading.Lock()
        self.worker_event = threading.Event()
        self.job_queue = queue.Queue()
        self.finish_jobs = []
        self.cpu_use =

        self.transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.transfer_socket.bind(("localhost", 8040))
        self.transfer_socket.listen(1)

        self.state_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.state_socket.bind(("localhost", 8041))
        self.state_socket.listen(1)

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
        pass


    def state_manager_receive(self):
        while True:
            # accept connections
            (clientsocket, address) = self.state_socket.accept()
            self.throttle_lock.acquire()
            self.throttling = int.from_bytes(clientsocket.recv(8), byteorder='big')
            self.throttle_lock.release()

    def state_manager_send(self):
        pass

    def hardware_monitor(self):

        return

    def transfer_manager_receive(self):
        # accept connections
        (clientsocket, address) = self.transfer_socket.accept()
        while True:
            # turn the byte stream into a list of jobs
            BUFFER_SIZE = clientsocket.recv(8)
            pickled_data = clientsocket.recv(int.from_bytes(BUFFER_SIZE, byteorder='big'))
            jobs_list = pickle.loads(pickled_data)
            for job in jobs_list:
                self.job_queue.put(job)
                print("Received job {}".format(job.id))

    def transfer_manager_send(self):
        pass

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

