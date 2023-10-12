import pandas as uncle

job_priority_0 = uncle.DataFrame({
    'job_id': [69, 56, 5, 4], 
    'arrival_time': [2, 1, 4, 3],
    'duration': [3, 4, 1, 2],
    'deadline': [5, 6, 7, 8],
    'priority': [1, 2, 2, 0]
})

job_priority_1 = uncle.DataFrame({
    'job_id': [1, 2, 9, 7], 
    'arrival_time': [4, 3, 7,1],
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


def schedule_by_priority_FIFO(priority_lists):

        job_priority_0 = uncle.DataFrame(priority_0)
        job_priority_1 = uncle.DataFrame(priority_1)
        job_priority_2 = uncle.DataFrame(priority_2)
        
        sorted_priority_0 = job_priority_0.sort_values(by=['arrival_time']).reset_index(drop=True)
        sorted_priority_1 = job_priority_1.sort_values(by=['arrival_time']).reset_index(drop=True)
        sorted_priority_2 = job_priority_2.sort_values(by=['arrival_time']).reset_index(drop=True)

        return uncle.concat([sorted_priority_0, sorted_priority_1, sorted_priority_2])

def schedule_by_priority_LIFO(priority_lists):
        job_priority_0 = uncle.DataFrame(priority_0)
        job_priority_1 = uncle.DataFrame(priority_1)
        job_priority_2 = uncle.DataFrame(priority_2)
        
        sorted_priority_0 = job_priority_0.sort_values(by=['arrival_time'], ascending=False).reset_index(drop=True)
        sorted_priority_1 = job_priority_1.sort_values(by=['arrival_time'], ascending=False).reset_index(drop=True)
        sorted_priority_2 = job_priority_2.sort_values(by=['arrival_time'], ascending=False).reset_index(drop=True)

        return uncle.concat([sorted_priority_0, sorted_priority_1, sorted_priority_2])

def schedule_by_priority_EDF(priority_lists):
        job_priority_0 = uncle.DataFrame(priority_0)
        job_priority_1 = uncle.DataFrame(priority_1)
        job_priority_2 = uncle.DataFrame(priority_2)
        
        sorted_priority_0 = job_priority_0.sort_values(by=['deadline', 'arrival_time']).reset_index(drop=True)
        sorted_priority_1 = job_priority_1.sort_values(by=['deadline', 'arrival_time']).reset_index(drop=True)
        sorted_priority_2 = job_priority_2.sort_values(by=['deadline', 'arrival_time']).reset_index(drop=True)

        return uncle.concat([sorted_priority_0, sorted_priority_1, sorted_priority_2])

def schedule_by_priority_SJF(priority_lists):
        job_priority_0 = uncle.DataFrame(priority_0)
        job_priority_1 = uncle.DataFrame(priority_1)
        job_priority_2 = uncle.DataFrame(priority_2)
        
        sorted_priority_0 = job_priority_0.sort_values(by=['duration']).reset_index(drop=True)
        sorted_priority_1 = job_priority_1.sort_values(by=['duration']).reset_index(drop=True)
        sorted_priority_2 = job_priority_2.sort_values(by=['duration']).reset_index(drop=True)

        return uncle.concat([sorted_priority_0, sorted_priority_1, sorted_priority_2])


print("Original:")
print(uncle.concat([job_priority_0, job_priority_1, job_priority_2]))

print("\nAfter Assigned in respective List:")
print("\nPriority 0:")
print(priority_0)
print("\nPriority 1:")
print(priority_1)
print("\nPriority 2:")
print(priority_2)

print("\nAfter Sorted:")
priority_lists=[priority_0, priority_1, priority_2]

print("\nFIFO:")
print(schedule_by_priority_FIFO(priority_lists))
print("\nLIFO:")
print(schedule_by_priority_LIFO(priority_lists))
print("\nEDF:")
print(schedule_by_priority_EDF(priority_lists))
print("\nSJF:")
print(schedule_by_priority_SJF(priority_lists))

