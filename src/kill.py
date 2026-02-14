import os
import signal
import subprocess


# Get the list of all processes
processes = subprocess.check_output(['ps', 'aux']).decode('utf-8').split('\n')

# Initialize a counter for terminated processes
terminated_count = 0

# Find and terminate all processes named 'main.py'
for process in processes:
    if 'main.py' in process:
        pid = int(process.split()[1])
        os.kill(pid, signal.SIGTERM)
        print(f'Process main.py with PID {pid} has been terminated.')
        terminated_count += 1


if terminated_count == 0:
    print('No process found.')
else:
    print(
        f'Total {terminated_count} processes have been terminated.')
