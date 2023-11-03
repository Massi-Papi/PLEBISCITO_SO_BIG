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

# This class implements the DWF algorithm
class DWFA:
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
        # Calculate finish time for job
        finish_time = max(self.virtual_time, job.arrival_time) + job.service_time
        heapq.heappush(self.job_queue, (finish_time, job.service_time, job))

    async def distribute_jobs(self):
        while self.job_queue:
            job_time, service_time, next_job = heapq.heappop(self.job_queue)

            # Update virtual time
            if job_time > self.virtual_time:
                self.virtual_time = job_time

            # Finds the server with the minimum number of jobs and that is not at full capacity
            min_server = min((server for server in self.servers if server.number_of_jobs < server.capacity), key=lambda x: x.number_of_jobs, default=None)

            # Find the server with the shortest expected finish time
            min_server = min(self.servers, key=lambda x: x.expected_finish_time)
            
            # If the server is busy, then the job is added to the queue
            if min_server.busy:
                self.virtual_time = min_server.expected_finish_time
            else:
                self.virtual_time = max(self.virtual_time, next_job.arrival_time)

            # Assign job to server
            min_server.start_job(next_job)

            # Save current time
            assign_time = time.time()

            #Print assignment
            print(f"Job {next_job.id} assigned to server {min_server.id} at {assign_time}")

            # Calculate finish time for and assign it to the server
            finish_time = max(self.virtual_time, next_job.arrival_time) + next_job.service_time
            min_server.expected_finish_time = finish_time

            # Update virtual time to the server's expected finish time
            self.virtual_time = min_server.expected_finish_time

            #Print job completion
            print(f"Job {next_job.id} completed at {finish_time}")


            # Continue sorting the remaining jobs
            self.job_queue.sort()

            asyncio.create_task(self.run_job(min_server, next_job))

    

    async def run(self):
        for job in self.jobs:
            self.add_job(job)

        await self.distribute_jobs()


    async def run_job(self,server, job):
        finished_jobs = []
        # start_time = time.time()
        # print(f"Job {job.id} started at {start_time}")  
        # finished_jobs.append((job, time.time()))

        await asyncio.sleep(job.service_time)
        finished_jobs.sort(key=lambda x: x[1])

        for job, start_time in finished_jobs:
            # print(f"Job {job.id} finished at {start_time}")
            # print("Server {} busy: {}".format(server.id, server.busy))
            server.finish_job()
    
    async def start_job(self, job):
        # Finds the server with the minimum number of jobs and that is not at full capacity
        min_server = min((server for server in self.servers if server.number_of_jobs < server.capacity), key=lambda x: x.number_of_jobs, default=None)

        # Find the server with the shortest expected finish time
        min_server = min(self.servers, key=lambda x: x.expected_finish_time)
        
        # If the server is busy, then the job is added to the queue
        if min_server.busy:
            self.virtual_time = min_server.expected_finish_time
        else:
            self.virtual_time = max(self.virtual_time, job.arrival_time)

        min_server.start_job(job)

        # Calculate finish time for and assign it to the server
        finish_time = max(self.virtual_time, job.arrival_time) + job.service_time
        min_server.expected_finish_time = finish_time

        # Print Job assignment
        print("Job {} assigned to server {}".format(job.id, min_server.id))

        # Print server busy state
        print("Server {} busy: {}".format(job.id, min_server.busy))

        # Update virtual time
        self.virtual_time = min_server.expected_finish_time

        asyncio.create_task(self.run_job(min_server, job))


if __name__ == "__main__":
    #jobs = [Job(1, 0, 1), Job(2, 0, 5), Job(3, 0, 3), Job(4, 0, 2), Job(5, 0, 4)]
    jobs = []
    for i in range(10000):
        arrival_time = random.randint(0, 100)
        service_time = random.randint(1, 10)
        jobs.append(Job(i, arrival_time, service_time))
    dwfa = DWFA(400, jobs, 10)
    # dwfa = DWFA(3, jobs, 2)
    asyncio.run(dwfa.run())
