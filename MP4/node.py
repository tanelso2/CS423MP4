import socket
import pickle
import threading
import psutil
import logging
import time
from sys import argv
from state import State
from job import Job

TOTAL_ELEMENTS = 1024 * 1024 * 4
JOB_COUNT = 512
SIZE_OF_JOB = int(TOTAL_ELEMENTS / JOB_COUNT)

def recvall(sock, n):
    # Helper function for reading whole data from socket
    data = bytes()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


class Node:
    def __init__(self, remote):
        # Set instance variables
        self.remote = remote
        self.throttling = 70
        self.job_queue = []
        self.processed_jobs = []
        self.cpu_use = 0.3
        self.remote_state = None
        self.hardware_manager_started = False

        # Sockets
        self.transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.state_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.transfer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.state_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Server / client server connections
        if self.remote:
            # this is the remote server so listen for connections
            self.transfer_socket.bind(("", 8040))
            self.transfer_socket.listen(1)
            self.state_socket.bind(("", 8041))
            self.state_socket.listen(1)

            # Accept connections
            (clientsocket, address) = self.state_socket.accept()
            self.state_socket = clientsocket
            (clientsocket, address) = self.transfer_socket.accept()
            self.transfer_socket = clientsocket
        else:
            # we are the local server, connect to the remote server
            self.transfer_socket.connect(("sp16-cs423-g06.cs.illinois.edu", 8040))
            self.state_socket.connect(("sp16-cs423-g06.cs.illinois.edu", 8041))

        # Threads
        self.throttle_lock = threading.Lock()
        self.job_queue_lock = threading.Lock()
        self.processed_jobs_lock = threading.Lock()
        self.shutdown_event = threading.Event()

        self.transfer_thread = threading.Thread(target=self.transfer_manager_receive, daemon=True)
        self.worker_thread = threading.Thread(target=self.worker_thread_manager, daemon=True)
        self.state_thread = threading.Thread(target=self.state_manager_receive, daemon=True)
        self.hardware_thread = threading.Thread(target=self.hardware_monitor, daemon=True)
        self.input_thread = threading.Thread(target=self.input_manager, daemon=True)

        self.transfer_thread.start()
        self.worker_thread.start()
        self.state_thread.start()
        self.input_thread.start()

        # Initialize jobs
        if not self.remote:
            self.bootstrap()
            self.hardware_thread.start()
            self.hardware_manager_started = True

        # Wait for shutdown event (i.e. we have finished processing all jobs)
        self.shutdown_event.wait()

        # Write results to file
        if not self.remote:
            self.processed_jobs.sort(key=lambda j: j.id)
            A = [x for job in self.processed_jobs for x in job.data]
            with open('results.txt', 'w') as f:
                f.write(str(A))

    def __del__(self):
        self.transfer_socket.shutdown(socket.SHUT_RDWR)
        self.state_socket.shutdown(socket.SHUT_RDWR)
        self.transfer_socket.close()
        self.state_socket.close()

    def bootstrap(self):
        # initialize the vector
        A = [1.111111] * TOTAL_ELEMENTS

        # create jobs
        all_jobs = [Job(i, i * SIZE_OF_JOB, A[i * SIZE_OF_JOB: i * SIZE_OF_JOB + SIZE_OF_JOB]) for i in
                    range(JOB_COUNT)]

        # Delegate half the jobs to local machine
        self.job_queue = all_jobs[0: int(JOB_COUNT / 2)]

        # Delegate half the jobs to remote server
        logging.info("Delegating {} jobs to remote server.".format(JOB_COUNT // 2))
        self.transfer_manager_send(all_jobs[int(JOB_COUNT / 2): JOB_COUNT])

    def worker_thread_manager(self):
        # Loop to process jobs/sleep/wake
        while True:
            # Get time starting jobs
            start_time = time.time()

            # Pop a job
            self.job_queue_lock.acquire()
            if len(self.job_queue) == 0:
                self.job_queue_lock.release()
                continue
            job = self.job_queue.pop()
            self.job_queue_lock.release()

            # Process the job
            job.vector_add()
            logging.info("Processed job {}".format(job.id))
            self.processed_jobs_lock.acquire()
            self.processed_jobs.append(job)
            self.processed_jobs_lock.release()

            # Get end time after completing job
            end_time = time.time()

            # Sleep for time proportional to throttling value and job time
            self.throttle_lock.acquire()
            sleep_time = (100 - self.throttling) * (end_time - start_time) / self.throttling
            self.throttle_lock.release()
            time.sleep(sleep_time)

    def num_jobs_to_send(self):
        # References to states of machines
        local_state = self.state()
        remote_state = self.remote_state

        # If we don't know about the remote server, send nothing
        if remote_state is None:
            return 0

        # Calculate jobs to send using formula
        a = local_state.num_jobs
        b = local_state.cpu_use
        c = local_state.throttle_value
        d = remote_state.num_jobs
        f = remote_state.cpu_use
        g = remote_state.throttle_value
        return int((a * b * g - c * d * f) // (b * g + c * f))


    def adaptor(self):
        # Calculate ratio of loads
        num_jobs_to_send = self.num_jobs_to_send()
        if num_jobs_to_send > 10:
            logging.info("Load imbalance detected. Sending {} jobs.".format(num_jobs_to_send))
            # Send some of our jobs over
            self.job_queue_lock.acquire()
            jobs_to_send = self.job_queue[-num_jobs_to_send:]
            del self.job_queue[-num_jobs_to_send:]
            self.job_queue_lock.release()
            self.transfer_manager_send(jobs_to_send)

        # Send processed jobs to local over transfer manager
        self.processed_jobs_lock.acquire()
        if self.remote and len(self.processed_jobs) > 0:
            logging.info("Sending {} processed jobs to local machine.".format(len(self.processed_jobs)))
            self.transfer_manager_send(self.processed_jobs)
            self.processed_jobs = []
        self.processed_jobs_lock.release()

        # Send our state
        self.state_manager_send()

    def state_manager_receive(self):
        while True:
            try:
                # Receive remote state over network
                BUFFER_SIZE = self.state_socket.recv(8)
                pickled_data = recvall(self.state_socket, int.from_bytes(BUFFER_SIZE, byteorder='big'))
                self.remote_state = pickle.loads(pickled_data)
                if self.remote_state.shutdown:
                    self.shutdown_event.set()
            except EOFError:
                # In case socket reads 0 bytes, pass
                pass

    def state_manager_send(self):
        # Serialize the state
        pickled_data = pickle.dumps(self.state())
        length = len(pickled_data).to_bytes(8, byteorder='big')

        # Send along socket
        self.state_socket.sendall(length)
        self.state_socket.sendall(pickled_data)

    def hardware_monitor(self):
        # If shutdown event is set, return from thread
        if self.shutdown_event.isSet():
            return

        # Get CPU use and call function again on a timer
        self.cpu_use = psutil.cpu_percent()
        timer = threading.Timer(1, self.hardware_monitor)
        self.adaptor()
        timer.start()

    def transfer_manager_receive(self):
        while True:
            # turn the byte stream into a list of jobs
            try:
                BUFFER_SIZE = self.transfer_socket.recv(8)
                pickled_data = recvall(self.transfer_socket, int.from_bytes(BUFFER_SIZE, byteorder='big'))
                jobs_recvd = pickle.loads(pickled_data)

                # If the jobs are processed, put into finished jobs (local only)
                if jobs_recvd[0].complete is True:
                    logging.info("Received {} processed jobs".format(len(jobs_recvd)))
                    self.processed_jobs_lock.acquire()
                    self.processed_jobs.extend(jobs_recvd)
                    self.processed_jobs_lock.release()
                else:
                    # Otherwise we are receiving jobs to process
                    logging.info("Received {} jobs to process".format(len(jobs_recvd)))
                    self.job_queue_lock.acquire()
                    self.job_queue.extend(jobs_recvd)
                    self.job_queue_lock.release()
                    if not self.hardware_manager_started:
                        self.hardware_thread.start()
                        self.hardware_manager_started = True
            except EOFError:
                # If socket reads 0 bytes continue
                pass

    def transfer_manager_send(self, data):
        # turns data into byte stream and sends it to the server
        pickled_data = pickle.dumps(data)
        length = len(pickled_data).to_bytes(8, byteorder='big')

        # Send along socket
        self.transfer_socket.sendall(length)
        self.transfer_socket.sendall(pickled_data)

    def input_manager(self):
        # Prompts user to enter throttling value, if it is invalid they must try again
        while 1:
            try:
                # Ensure user enters a valid percentage
                value = int(input("Enter a throttling value from 1 to 100: "))
                if value < 1 or value > 100:
                    raise ValueError
                else:
                    logging.info("Setting throttling to {}".format(value))
                    self.throttle_lock.acquire()
                    self.throttling = value
                    self.throttle_lock.release()
            except ValueError:
                print("Value must be an integer between 1 and 100. Try again.")

    def state(self):
        # Lock queue
        self.job_queue_lock.acquire()
        state = State(len(self.job_queue), self.throttling, self.cpu_use)
        self.job_queue_lock.release()

        # If we have finished receiving and completing jobs on local
        self.processed_jobs_lock.acquire()
        if not self.remote and len(self.processed_jobs) == JOB_COUNT:
            state.shutdown = True
            self.shutdown_event.set()
        self.processed_jobs_lock.release()

        return state


if __name__ == "__main__":
    # Start unit as remote
    if argv[1] == "remote":
        logging.basicConfig(filename='logfile-remote.txt', level=logging.DEBUG,
                            format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s', datefmt='%Y-%M-%d %H:%M:%S')
        logging.info("Starting as remote...")
        remote_node = Node(True)
        logging.info("Shutting down...")

    # Start unit as local
    elif argv[1] == "local":
        logging.basicConfig(filename='logfile-local.txt', level=logging.DEBUG,
                            format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s', datefmt='%Y-%M-%d %H:%M:%S')
        logging.info("Starting as local...")
        local_node = Node(False)
        logging.info("Shutting down...")

    # Usage error
    else:
        print("Usage error.")
