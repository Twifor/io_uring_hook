import subprocess
import time
import matplotlib.pyplot as plt
import numpy as np
import re
import threading
import psutil


def extract_mean(input_string):
    pattern = r"Submit Info: Mean=([0-9]*\.?[0-9]+), Std=[0-9]*\.?[0-9]+"
    match = re.search(pattern, str(input_string))
    if match:
        mean_value = match.group(1)
        return float(mean_value)
    return None


def get_disk_stats(disk_name):
    stats = {}
    with open("/proc/diskstats", "r") as file:
        for line in file:
            parts = line.split()
            if parts[2] == disk_name:
                stats["reads_completed"] = int(parts[3])
                stats["sectors_read"] = int(parts[5])
                stats["writes_completed"] = int(parts[7])
                stats["sectors_written"] = int(parts[9])
                break
    return stats


def calculate_iops_and_speed(start_stats, end_stats, duration):
    reads_completed = end_stats["reads_completed"] - start_stats["reads_completed"]
    writes_completed = end_stats["writes_completed"] - start_stats["writes_completed"]
    sectors_read = end_stats["sectors_read"] - start_stats["sectors_read"]

    iops = (reads_completed + writes_completed) / duration
    read_speed = (sectors_read * 512) / duration / 1024 / 1024  # Convert to MB/s

    return iops, read_speed


def monitor_disk(disk_name, interval, stop_event, results):
    total_read_speed = 0
    num_intervals = 0

    start_stats = get_disk_stats(disk_name)
    start_time = time.time()

    while not stop_event.is_set():
        time.sleep(interval)
        end_stats = get_disk_stats(disk_name)
        end_time = time.time()
        duration = end_time - start_time

        _, read_speed = calculate_iops_and_speed(start_stats, end_stats, duration)

        total_read_speed += read_speed
        num_intervals += 1

        start_stats = end_stats
        start_time = end_time

    if num_intervals > 0:
        average_read_speed = total_read_speed / num_intervals
    else:
        average_read_speed = 0

    results["average_read_speed"] = average_read_speed


def monitor_cpu_usage(process: psutil.Process, interval, stop_event, cpu_usage_list):
    time.sleep(interval)  # Initial delay to allow the process to start
    process = process.children()[0]
    initial_cpu_percent = process.cpu_percent(
        interval=None
    )  # Initial call to set up the baseline
    while not stop_event.is_set() and process.is_running():
        try:
            cpu_usage = process.cpu_percent(interval=interval)
            # print(process.pid, cpu_usage)
            cpu_usage_list.append(cpu_usage)
        except psutil.NoSuchProcess:
            break
        except Exception as e:
            print(f"Exception in monitor_cpu_usage: {e}")
            break
    # Ensure to capture the last reading if the process has ended
    if not process.is_running():
        try:
            cpu_usage = process.cpu_percent(interval=None)
            cpu_usage_list.append(cpu_usage)
        except psutil.NoSuchProcess:
            pass
        except Exception as e:
            print(f"Exception in monitor_cpu_usage: {e}")


def run(hook, sqpoll, file, bthread_num, batch_log, disk_name, pthread):
    start_time = time.time()
    hook_str = ""
    sqpoll_str = ""
    cpu = "taskset -c 0,1,2,3"
    if hook:
        hook_str = "LD_PRELOAD=./build/liburinghook.so"
    if sqpoll:
        sqpoll_str = "IOURING_HOOK_SQPOLL=true"
        cpu = "taskset -c 0,1,2"
    command = f"{cpu} bash -c '{sqpoll_str} {hook_str} ./test {file} {bthread_num} {batch_log} {pthread}'"
    print(command)

    # Prepare for monitoring
    stop_event = threading.Event()
    results = {}
    monitor_thread = threading.Thread(
        target=monitor_disk, args=(disk_name, 0.1, stop_event, results)
    )
    monitor_thread.start()

    # Execute command and monitor CPU usage
    process = psutil.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    cpu_usage_list = []
    cpu_monitor_thread = threading.Thread(
        target=monitor_cpu_usage, args=(process, 0.1, stop_event, cpu_usage_list)
    )
    cpu_monitor_thread.start()

    stdout, stderr = process.communicate()
    end_time = time.time()
    execution_time = end_time - start_time

    # Stop monitoring
    stop_event.set()
    monitor_thread.join()
    cpu_monitor_thread.join()

    # Process results
    submitted = extract_mean(stdout)
    avg_read_speed = results.get("average_read_speed", 0)

    # Calculate average CPU usage
    avg_cpu_usage = sum(cpu_usage_list) / len(cpu_usage_list) if cpu_usage_list else 0

    return execution_time, submitted, avg_read_speed, avg_cpu_usage


File = ""
Batch = 12
Pan = "sdc"


HOOK = []
HOOK_SQPOLL = []
VANILLA = []
VANILLA_PTHREAD = []

HOOK_SPEED = []
HOOK_SQPOLL_SPEED = []
VANILLA_SPEED = []
VANILLA_PTHREAD_SPEED = []

HOOK_CPU = []
HOOK_SQPOLL_CPU = []
VANILLA_CPU = []
VANILLA_PTHREAD_CPU = []

HOOK_SUBMIT = []
HOOK_SQPOLL_SUBMIT = []

TASKS = [1, 4, 16, 64, 128, 256]

# for x in TASKS:
#     tm, sb, speed, cpu = run(True, False, File, x, Batch, Pan, 0)
#     HOOK.append(tm)
#     HOOK_CPU.append(cpu)
#     HOOK_SUBMIT.append(sb)
#     HOOK_SPEED.append(speed)

#     tm, sb, speed, cpu = run(True, True, File, x, Batch, Pan, 0)
#     HOOK_SQPOLL.append(tm)
#     HOOK_SQPOLL_CPU.append(cpu)
#     HOOK_SQPOLL_SUBMIT.append(sb)
#     HOOK_SQPOLL_SPEED.append(speed)

#     tm, sb, speed, cpu = run(False, False, File, x, Batch, Pan, 0)
#     VANILLA.append(tm)
#     VANILLA_CPU.append(cpu)
#     VANILLA_SPEED.append(speed)

#     tm, sb, speed, cpu = run(False, False, File, x, Batch, Pan, 1)
#     VANILLA_PTHREAD.append(tm)
#     VANILLA_PTHREAD_CPU.append(cpu)
#     VANILLA_PTHREAD_SPEED.append(speed)

vanilla_color = "#1f77b4"  # Blue
vanilla_pthread_color = "#9467bd"  # Purple
iouring_color = "#2ca02c"  # Green
libaio_sqpoll_color = "#d62728"  # Red


plt.figure(figsize=(10, 6))
plt.subplots_adjust(left=0.08, right=0.99, top=0.95, bottom=0.1)
plt.plot(TASKS, VANILLA, marker="o", label="vanilla (bthread)", color=vanilla_color)
plt.plot(
    TASKS,
    VANILLA_PTHREAD,
    marker="o",
    label="vanilla (pthread)",
    color=vanilla_pthread_color,
)
plt.plot(TASKS, HOOK, marker="o", label="io_uring hook", color=iouring_color)
plt.plot(
    TASKS,
    HOOK_SQPOLL,
    marker="o",
    label="io_uring hook (sqpoll)",
    color=libaio_sqpoll_color,
)
plt.title("Overall Execution Time")
plt.xlabel("Number of Bthreads")
plt.ylabel("Execution Time (s)")
plt.legend()
plt.grid(True)
plt.savefig("time.jpg")

plt.figure(figsize=(10, 6))
plt.subplots_adjust(left=0.08, right=0.99, top=0.95, bottom=0.1)
plt.plot(TASKS, HOOK_SUBMIT, marker="o", label="io_uring hook", color=iouring_color)
plt.plot(
    TASKS,
    HOOK_SQPOLL_SUBMIT,
    marker="o",
    label="io_uring hook (sqpoll)",
    color=libaio_sqpoll_color,
)
plt.title("Number of Submitted Tasks per Syscall")
plt.xlabel("Number of Bthreads")
plt.ylabel("Number of Tasks (Mean)")
plt.legend()
plt.grid(True)
plt.savefig("submit.jpg")

plt.figure(figsize=(10, 6))
plt.subplots_adjust(left=0.08, right=0.99, top=0.95, bottom=0.1)
plt.plot(
    TASKS, VANILLA_SPEED, marker="o", label="vanilla (bthread)", color=vanilla_color
)
plt.plot(
    TASKS,
    VANILLA_PTHREAD_SPEED,
    marker="o",
    label="vanilla (pthread)",
    color=vanilla_pthread_color,
)
plt.plot(TASKS, HOOK_SPEED, marker="o", label="io_uring hook", color=iouring_color)
plt.plot(
    TASKS,
    HOOK_SQPOLL_SPEED,
    marker="o",
    label="io_uring hook (sqpoll)",
    color=libaio_sqpoll_color,
)
plt.title("I/O Performance")
plt.xlabel("Number of Bthreads")
plt.ylabel("Speed (MB/s)")
plt.legend()
plt.grid(True)
plt.savefig("speed.jpg")

plt.figure(figsize=(10, 6))
plt.subplots_adjust(left=0.08, right=0.99, top=0.95, bottom=0.1)
plt.plot(TASKS, VANILLA_CPU, marker="o", label="vanilla (bthread)", color=vanilla_color)
plt.plot(
    TASKS,
    VANILLA_PTHREAD_CPU,
    marker="o",
    label="vanilla (pthread)",
    color=vanilla_pthread_color,
)
plt.plot(TASKS, HOOK_CPU, marker="o", label="io_uring hook", color=iouring_color)
plt.plot(
    TASKS,
    HOOK_SQPOLL_CPU,
    marker="o",
    label="io_uring hook (sqpoll)",
    color=libaio_sqpoll_color,
)
plt.title("CPU Usage")
plt.xlabel("Number of Bthreads")
plt.ylabel("CPU Usage (%)")
plt.legend()
plt.grid(True)
plt.savefig("cpu.jpg")
