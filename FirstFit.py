import pickle
import math
import random
import threading
import Machine
import time

threads = []
jobc_obj = pickle.load(open("jobc_obj.p", "r"))
machinec_obj = pickle.load(open("machinec_obj.p", "r"))

free_machines = []
for j in machinec_obj.keys():
    free_machines.extend(machinec_obj[j])

random.shuffle(free_machines)
print "LEN: ", free_machines[0]
job_q = []
for i in jobc_obj.keys():
    job_q.extend(jobc_obj[i])
job_q.sort(key=lambda x: x.getStartTime())
pickle.dump(job_q, open("final_q.p", "wb"))

jobCentroids = pickle.load(open("centroids_job.p", "r"))
# machineCentroids = pickle.load(open("centroids_machine.p", "r"))


dominates = ""
# all of them get scheduled on machine cluster 2
count = 0
i = 0

def worker(job, dominates, mac):
    time_machine_allocation.append((job.endTime - job.startTime) / 10 ** 10)
    time.sleep((job.endTime - job.startTime) / 10 ** 10)  # job processing time
    free_machines.append(mac)



done_jobs_count = 1  # CHECK THIS ONCE
sum_time = 0.0
machine_allocated = []
time_machine_allocation = []
start_time = time.time()
for job in job_q:  # each job is a job object
    # global free_machines
    # get the avg running time for all the done jobs
    sum_time += (job.endTime - job.startTime)
    print "sum time: ", sum_time
    # THIS IS Wrong
    avg_time = sum_time / done_jobs_count
    print "avg_time: ", avg_time
    print "done jobs count", done_jobs_count
    # cluster centroid metrics
    c_num = job.getClusterNumber()
    c_mem = jobCentroids[c_num][0]
    c_cpu = jobCentroids[c_num][1]

    min_mem = 999
    min_cpu = 999
    # check which dominates
    if c_mem > c_cpu:
        dominates = "m"
    else:
        dominates = "c"
    if free_machines == []:
        time.sleep(avg_time / 10 ** 10)  # delta
        # free_machines_temp = free_machines[:]
        mac = free_machines[0]
    else:
        # free_machines_temp = free_machines[:]
        mac = free_machines[0]
    print "call function"
    global free_machines
    free_machines_temp = []
    for tempMac in free_machines:  # since we are considering only cluster number 2
        free_machines_temp.append(tempMac)
        diff_cpu = abs(tempMac.getCurrCpu() - job.getRemCpu())
        diff_mem = abs(tempMac.getCurrMem() - job.getRemMem())  # mem < 0
        if dominates == "m":
            if diff_mem < diff_cpu and diff_mem < min_mem:
                min_mem = diff_mem
                mac = tempMac
        elif dominates == "c":
            if diff_cpu < diff_mem and diff_cpu < min_cpu:
                min_cpu = diff_cpu
                mac = tempMac
    free_machines = free_machines_temp
    print "returning from call function"
    # now we have a best fit free machine
    # full block taken outside
    machine_allocated.append(mac)
    print "MachineID:", mac.machineID
    print "Job Resource:", job.getCpuUtil()
    print "total resource used: ", mac.getCurrCpu() - job.getCpuUtil()
    # lock.acquire()
    free_machines_temp.remove(mac)
    # lock.release()

    # in case job requires more than one machine:
    if (mac.getCurrCpu() - job.getRemCpu()) < 0:
        count += 1
        job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
        # job.setRemMem(job.getRemMem() - mac.getCurrMem())
        ic = 1
        continue
    if (mac.getCurrMem() - job.getRemMem()) < 0:
        count += 1
        # job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
        job.setRemMem(job.getRemMem() - mac.getCurrMem())
        im = 1
        continue
    t = threading.Thread(target=worker, args=(job, dominates, mac))
    threads.append(t)
    t.start()
    # t.join()
    # if ic == 1 or im == 1:
    # print "continued....."
    # callFunction_incomplete(job)
    i += 1
    done_jobs_count += 1
    print i
    ic = 0
    im = 0
    free_machines = free_machines_temp

sum_time = sum(time_machine_allocation)
final_time_taken = sum_time  # * len(machine_allocated)
print "FINAL: ", final_time_taken
print "COUNT", count
