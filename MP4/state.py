class State:
    def __init__(self, num_jobs, throttle_value, cpu_use):
        self.num_jobs = num_jobs
        self.throttle_value = throttle_value
        self.cpu_use = cpu_use

    def num_jobs_to_send(self, remote_state):
        return (self.num_jobs * self.throttle_value * self.cpu_use - remote_state.num_jobs * remote_state.throttle_value * remote_state.cpu_use) / (self.throttle_value * self.cpu_use + remote_state.throttle_value * remote_state.cpu_use)