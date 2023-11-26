import heapq
import asyncio
import time
import random

class Job:
    def __init__(self, id, arrival_time, service_time):
        self.id = id
        self.arrival_time = arrival_time
        self.service_time = service_time

    def __lt__(self, other):
        if self.arrival_time == other.arrival_time:
            return self.service_time < other.service_time
        return self.service_time < other.service_time


class Server:
    def __init__(self, id, capacity):
        self.id = id
        self.busy = False
        self.expected_finish_time = 0
        self.capacity = capacity
        self.number_of_jobs = 0
        self.virtual_time = 0

    def start_job(self, job): 
        if self.number_of_jobs < self.capacity:
            self.number_of_jobs += 1
        self.busy = True
        self.job = job
        self.virtual_time += job.service_time

    def finish_job(self):
        self.busy = False
        self.virtual_time = self.job.service_time
        self.expected_finish_time = 0

class FIFO:
    def __init__(self, num_servers, jobs, capacity):
        if num_servers < 1:
            raise ValueError("Number of servers must be greater than 0")

        if not jobs:
            raise ValueError("Jobs list must not be empty")

        self.servers = [Server(i, capacity) for i in range(num_servers)]
        self.job_queue = []
        self.jobs = jobs
        self.virtual_time = 0
        self.loop = asyncio.get_event_loop()
        
    
    def add_job(self, job):

        start_time = time.perf_counter()

        # Update virtual_time whenever a job is scheduled
        self.virtual_time = max(self.virtual_time, job.arrival_time)
        
        # Finds the first open server that is not at full capacity
        min_server = next((server for server in self.servers if not server.busy), None)
        

        # If there is no available server, then the job is added to the queue
        if min_server is None or min_server.busy:
            heapq.heappush(self.job_queue, job)
            return
        
        min_server.start_job(job)
        
        # Calculate finish time for job
        finish_time = max(self.virtual_time, job.arrival_time) + job.service_time
        min_server.expected_finish_time = finish_time

        # Print Job Assignment 
        print("Job {} assigned to server {} at {}".format(job.id, min_server.id, start_time))

        # Update virtual time to the server's expected finish time
        self.virtual_time = min_server.expected_finish_time

        asyncio.create_task(self.run_job(min_server, job))

        if self.job_queue:
            queued_job = heapq.heappop(self.job_queue)
            available_server = next((server for server in self.servers if not server.busy), None)
            if available_server:
                available_server.start_job(queued_job)
                asyncio.create_task(self.run_job(available_server, queued_job))
            else:
                heapq.heappush(self.job_queue, queued_job)

    
    async def distribute_jobs(self):
        while self.job_queue:
            job = heapq.heappop(self.job_queue)
            
            #Check for available server
            server = next((server for server in self.servers if not server.busy), None)
            if server:
                server.start_job(job)
                asyncio.create_task(self.run_job(server, job))
            else:
                heapq.heappush(self.job_queue, job)
                await asyncio.sleep(1)
            asyncio.create_task(self.run_job(server, job))
    
    async def run(self):
        for job in self.jobs:
            self.add_job(job)
        while self.job_queue:
            await self.distribute_jobs()
    
    async def run_job(self, server, job):
        await asyncio.sleep(job.service_time)

        server.busy = False
        server.finish_job()

        finish_time = time.perf_counter()

        print("Job {} completed at {}".format(job.id, finish_time))

        

    async def start_job(self, job):
        min_server = min(self.servers, key=lambda x: x.jobs[0].arrival_time, default=None)
        min_server.start_job(job)
        asyncio.create_task(self.run_job(min_server, job))


if __name__ == "__main__":
    jobs = []
    jobs.append(Job(0, 0, 1))
    jobs.append(Job(1, 1, 2))
    jobs.append(Job(2, 4, 3))
    jobs.append(Job(3, 0, 4))
    # for i in range(5):
    #     arrival_time = random.randint(0, 100)
    #     service_time = random.randint(1, 10)
    #     jobs.append(Job(i, arrival_time, service_time))

    fifo = FIFO(2, jobs, 10)
    asyncio.run(fifo.run())