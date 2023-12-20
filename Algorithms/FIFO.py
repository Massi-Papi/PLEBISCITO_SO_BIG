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
        self.number_of_jobs -= 1 
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
        self.finished_jobs = []

        self.f = open("FIFOData.txt", "w")

        # Start time
        self.start_time = time.time()

    def add_job(self, job):
        finish_time = job.service_time
        heapq.heappush(self.job_queue, (job.arrival_time, finish_time, job))
    
    async def distribute_jobs(self):

        while self.jobs_remaining():

            # Popping 
            arrival_time, service_time, job = heapq.heappop(self.job_queue)

            #Finds the server with the minimum number of jobs and that is not at full capacity
            server = next((s for s in self.servers if s.number_of_jobs < s.capacity), None)

            if server is None:
                heapq.heappush(self.job_queue, (arrival_time, service_time, job))
                await asyncio.sleep(.1)
                continue

            # Assign job to server and run it
            server.start_job(job)

            # Save current time
            current_time = time.time() - self.start_time 

            print(f"Job {job.id} assigned to server {server.id} at {current_time} seconds since the program started")


            # Print job completion 
            self.f.write(f"\n Job {job.id} \n Server: {server.id} \n Time: {server.virtual_time} \n")
            print(f"Job {job.id} completed in {job.service_time} seconds")

            # Continue sorting the remaining jobs
            self.job_queue.sort()

            asyncio.create_task(self.run_job(server, job))

            # Simulate the time of the job. Remove this line to get direct results.
            # await asyncio.sleep(job.service_time)
        self.f.close()

    def jobs_remaining(self):
        if len(self.job_queue) == 0:
            return False

        return (any(s.busy for s in self.servers)) or (any(j not in self.finished_jobs for j in self.jobs))
    
    def pending_jobs(self):
        return len([j for j in self.jobs if j not in self.finished_jobs])


    async def run(self):
        for job in self.jobs:
            self.add_job(job)

        await self.distribute_jobs()

    async def run_job(self,server, job):

        await asyncio.sleep(job.service_time)
    
        server.number_of_jobs -= 1
        self.finished_jobs.append(job)
    

if __name__ == "__main__":
    jobs = []
    for i in range(10000):
        arrival_time = random.randint(0, 100)
        service_time = random.randint(1, 10)
        jobs.append(Job(i, arrival_time, service_time))

    fifo = FIFO(400, jobs, 10)
    asyncio.run(fifo.run())
