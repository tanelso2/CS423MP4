class Job:
    def __init__(self, id, start, data):
        self.id = id
        self.start = start
        self.data = data
        self.complete = False

    def vector_add(self):
        for i in range(len(self.data)):
            for j in range(200):
                self.data[i] += 1.111111
        self.complete = True