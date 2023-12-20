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


class SJF:
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

        self.f = open("sjf.txt", "w")

        # Start time
        self.start_time = time.time()


    def add_job(self, job):
        finish_time = job.service_time
        heapq.heappush(self.job_queue, (finish_time, job))

    async def distribute_jobs(self):
        
        while self.job_queue:
            service_time, next_job = heapq.heappop(self.job_queue)

            # Update virtual time
            self.virtual_time = max(self.virtual_time, next_job.arrival_time)

            # Find the server with the minimum number of jobs and that is not at full capacity
            min_server = min((server for server in self.servers if server.number_of_jobs < server.capacity), key=lambda x: x.number_of_jobs, default=None)

            # If the server is busy, then the job is added to the queue
            if min_server and min_server.busy:
                self.virtual_time = max(self.virtual_time, min_server.expected_finish_time)
                
            else:
                # Update virtual time to the server's expected finish time
                if min_server:
                    self.virtual_time = min_server.expected_finish_time 
                    min_server.start_job(next_job)
                else:
                    min_server = min(self.servers, key=lambda x: x.expected_finish_time)
                    self.virtual_time = min_server.expected_finish_time
                    min_server.start_job(next_job)
                
                    
            # Assign job to server
            min_server.start_job(next_job)

            # Save current time
            assign_time = time.time() - self.start_time

            print(f"Job {next_job.id} assigned to server {min_server.id} at {assign_time} seconds since the program started")

            # Calculate finish time for and assign it to the server
            finish_time = max(self.virtual_time, next_job.arrival_time) + next_job.service_time
            min_server.expected_finish_time = finish_time

            # Update virtual time to the server's expected finish time
            self.virtual_time = min_server.expected_finish_time

            # Print job completion
            self.f.write(f"\n Job: {next_job.id} \n Server: {min_server.id} \n Time: {finish_time} ")
            print(f"Job {next_job.id} completed in {finish_time} seconds")

            self.job_queue.sort()

            asyncio.create_task(self.run_job(min_server, next_job))
        self.f.close()

    async def run(self):
        for job in self.jobs:
            self.add_job(job)

        await self.distribute_jobs()

    async def run_job(self, server, job):
        finished_jobs = []

        await asyncio.sleep(job.service_time)
        finished_jobs.append(job)

        for job, start_time in finished_jobs:
            server.finish_job()

    async def start_job(self, job):
                    # Finds the server with the minimum number of jobs and that is not at full capacity
                    min_server = min((server for server in self.servers if server.number_of_jobs < server.capacity), key=lambda x: x.number_of_jobs, default=None)

                    # Find the server with the shortest expected finish time
                    min_server = min(self.servers, key=lambda x: x.expected_finish_time, default=None)
                    
                    # If there is no minimum server available, add the job to the queue
                    if min_server is None:
                        self.job_queue.append((job.service_time, job))
                        return

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
                    print(f"Job {job.id} assigned to server {min_server.id}")

                    # Print server busy state

if __name__ == "__main__":
    jobs = []
    for i in range(10000):
        arrival_time = random.randint(0, 100)
        service_time = random.randint(1, 10)
        jobs.append(Job(i, arrival_time, service_time))
    
    sjf = SJF(400, jobs, 10)
    asyncio.run(sjf.run())