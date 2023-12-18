job_data = {}

with open('dwfa.txt') as f:
    current_job_id = None
    current_job_data = {}

    for line in f:
        words = line.split()

        if not words:  # Skip empty lines
            continue

        key, value = words[0], words[1:]

        if key == 'Job:':
            if current_job_id:  # Save data for the previous job if exists
                job_data[current_job_id] = current_job_data

            current_job_id = value[0]
            current_job_data = {'server': None, 'time': None}
        elif key == 'Server:':
            current_job_data['server'] = value[0]
        elif key == 'Time:':
            current_job_data['time'] = value[0]

    if current_job_id:  # Save data for the last job if exists
        job_data[current_job_id] = current_job_data

print(job_data)
