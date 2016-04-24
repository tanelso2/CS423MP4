class State:
    def __init__(self, num_jobs, throttle_value, cpu_use):
        self.num_jobs = num_jobs
        self.throttle_value = throttle_value
        self.cpu_use = cpu_use
        self.shutdown = False
