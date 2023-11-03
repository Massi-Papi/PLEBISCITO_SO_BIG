import pandas as uncle

class JobQueue:

    def __init__(self, priority_lists):
        self.priority_lists = priority_lists

    def schedule_by_priority_FIFO(self, priority_lists):

        job_priority_0 = uncle.DataFrame(priority_0)
        job_priority_1 = uncle.DataFrame(priority_1)
        job_priority_2 = uncle.DataFrame(priority_2)

        sorted_priority_0 = job_priority_0.sort_values(
            by=['arrival_time']).reset_index(drop=True)
        sorted_priority_1 = job_priority_1.sort_values(
            by=['arrival_time']).reset_index(drop=True)
        sorted_priority_2 = job_priority_2.sort_values(
            by=['arrival_time']).reset_index(drop=True)

        return uncle.concat([sorted_priority_0, sorted_priority_1, sorted_priority_2])

    def schedule_by_priority_LIFO(self, priority_lists):
        job_priority_0 = uncle.DataFrame(priority_0)
        job_priority_1 = uncle.DataFrame(priority_1)
        job_priority_2 = uncle.DataFrame(priority_2)

        sorted_priority_0 = job_priority_0.sort_values(
            by=['arrival_time'], ascending=False).reset_index(drop=True)
        sorted_priority_1 = job_priority_1.sort_values(
            by=['arrival_time'], ascending=False).reset_index(drop=True)
        sorted_priority_2 = job_priority_2.sort_values(
            by=['arrival_time'], ascending=False).reset_index(drop=True)

        return uncle.concat([sorted_priority_0, sorted_priority_1, sorted_priority_2])

    def schedule_by_priority_EDF(self, priority_lists):
        job_priority_0 = uncle.DataFrame(priority_0)
        job_priority_1 = uncle.DataFrame(priority_1)
        job_priority_2 = uncle.DataFrame(priority_2)

        sorted_priority_0 = job_priority_0.sort_values(
            by=['deadline', 'arrival_time']).reset_index(drop=True)
        sorted_priority_1 = job_priority_1.sort_values(
            by=['deadline', 'arrival_time']).reset_index(drop=True)
        sorted_priority_2 = job_priority_2.sort_values(
            by=['deadline', 'arrival_time']).reset_index(drop=True)

        return uncle.concat([sorted_priority_0, sorted_priority_1, sorted_priority_2])

    def schedule_by_priority_SJF(self, priority_lists):
        job_priority_0 = uncle.DataFrame(priority_0)
        job_priority_1 = uncle.DataFrame(priority_1)
        job_priority_2 = uncle.DataFrame(priority_2)

        sorted_priority_0 = job_priority_0.sort_values(
            by=['duration']).reset_index(drop=True)
        sorted_priority_1 = job_priority_1.sort_values(
            by=['duration']).reset_index(drop=True)
        sorted_priority_2 = job_priority_2.sort_values(
            by=['duration']).reset_index(drop=True)

        return uncle.concat([sorted_priority_0, sorted_priority_1, sorted_priority_2])

    def original_job_queue(self):
        print("Original:")
        print(uncle.concat([job_priority_0, job_priority_1, job_priority_2]))

    def assigned_job_queue(self):
        print("\nAfter Assigned in respective List:")
        print("\nPriority 0:")
        print(priority_0)
        print("\nPriority 1:")
        print(priority_1)
        print("\nPriority 2:")
        print(priority_2)

    def sorted_job_queue(self, priority_lists):
        print("\nAfter Sorted:")
        priority_lists = [priority_0, priority_1, priority_2]
        job_queue = JobQueue(None)

        print("\nFIFO:")
        print(job_queue.schedule_by_priority_FIFO(priority_lists))
        print("\nLIFO:")
        print(job_queue.schedule_by_priority_LIFO(priority_lists))
        print("\nEDF:")
        print(job_queue.schedule_by_priority_EDF(priority_lists))
        print("\nSJF:")
        print(job_queue.schedule_by_priority_SJF(priority_lists))

# class JobQueueWithKServer(JobQueue):
    
#     def __init__(self, jobs, num_servers):
#         super().__init__(jobs)
#         self.num_servers = num_servers
#         self.servers = [Server(i) for i in range(num_servers)]
#         self.job_queue = self.schedule_by_priority_EDF(jobs)
#         self.time = 0
#         self.current_job = None
#         self.current_server = None
#         self.completed_jobs = []
#         self.waiting_jobs = []
#         self.running_jobs = []
#         self.total_wait_time = 0
#         self.total_turnaround_time = 0
#         self.total_response_time = 0
#         self.total_jobs = len(jobs)
#         self.total_jobs_completed = 0
#         self.total_jobs_waiting = 0
#         self.total_jobs_running = 0
#         self.total_jobs_in_system = 0
#         self.total_jobs_in_queue = 0
#         self.total_jobs_in_service = 0
#         self.total_jobs_in_system = 0
    
#     def update_server_status(self, server, job):
#         server.location = job.location
#         server.job = job
#         server.status = "busy"
#         server.time_remaining = job.duration
#         server.time_until_available = job.duration
#         server.start_time = self.time
#         server.end_time = self.time + job.duration

#     def update_job_status(self, job):
#         job.status = "running"
#         job.start_time = self.time
#         job.end_time = self.time + job.duration
#         job.time_remaining = job.duration
#         job.time_until_available = job.duration

#     def update_job_queue(self):
#         self.job_queue = self.schedule_by_priority_EDF(self.job_queue)
    
#     def handle_preemption(self, new_job):
#         if self.should_preempt(new_job):

#             server = self.current_server_to_preempt()
#             server = self.get_server_to_preempt()
#             self.preempt(server)
#             self.schedule(new_job)

#     def should_preempt(self, new_job):
#         return self.work_function(new_job) > self.work_function(self.current_job)
    

#     def work_function(self, job):
#         return job['duration'] / (job['deadline'] - job['arrival_time'])
    
#     def preempt(self, server):

#         current_job = server.job
#         current_remaining_time = server.time_remaining

#         server.job = None
#         server.time_remaining = 0
#         server.status = "idle"

#         current_job_status = "waiting"
#         current_job.time_remaining = current_remaining_time

#         self.waiting_jobs.append(current_job)

    
    

if __name__ == "__main__":

    job_priority_0 = uncle.DataFrame({
        'job_id': [69, 56, 5, 4],
        'arrival_time': [2, 1, 4, 3],
        'duration': [3, 4, 1, 2],
        'deadline': [5, 6, 7, 8],
        'priority': [1, 2, 2, 0]
    })

    job_priority_1 = uncle.DataFrame({
        'job_id': [1, 2, 9, 7],
        'arrival_time': [4, 3, 7, 1],
        'duration': [3, 4, 1, 2],
        'deadline': [5, 9, 7, 8],
        'priority': [2, 2, 2, 0]
    })

    job_priority_2 = uncle.DataFrame({
        'job_id': [21, 24, 53, 84],
        'arrival_time': [2, 1, 4, 3],
        'duration': [3, 4, 1, 2],
        'deadline': [5, 6, 7, 8],
        'priority': [0, 0, 1, 1]
    })

    priority_0 = []
    priority_1 = []
    priority_2 = []

    for df in [job_priority_0, job_priority_1, job_priority_2]:
        for index, row in df.iterrows():
            if row['priority'] == 0:
                priority_0.append(row.to_dict())
            elif row['priority'] == 1:
                priority_1.append(row.to_dict())
            elif row['priority'] == 2:
                priority_2.append(row.to_dict())

    priority_lists = [job_priority_0, job_priority_1, job_priority_2]

    scheduler = JobQueue(priority_lists)

    scheduler.original_job_queue()

    scheduler.assigned_job_queue()

    scheduler.sorted_job_queue(priority_lists)


