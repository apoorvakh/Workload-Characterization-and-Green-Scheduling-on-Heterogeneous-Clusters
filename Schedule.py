import pickle
import math
import threading
from threading import Lock
import Machine
import time

threads = []
lock = Lock()
jobc_obj = pickle.load(open("jobc_obj.p", "r"))
machinec_obj = pickle.load(open("machinec_obj.p", "r"))
free_machines = machinec_obj[2][:]
job_q = []
for i in jobc_obj.keys():
    job_q.extend(jobc_obj[i])

job_q.sort(key=lambda x: x.getStartTime())
pickle.dump(job_q, open("final_q.p", "wb"))

# sort based on the startTime of the Jobs(incoming jobs)
# tempCluster.sort(key=lambda x:x.getStartTime())
jobCentroids = pickle.load(open("centroids_job.p", "r"))
machineCentroids = pickle.load(open("centroids_machine.p", "r"))

max_dist_list = []
max_j = 0
max_m = 0
for p, j in enumerate(jobCentroids):
    max_dist = 999
    for q, m in enumerate(machineCentroids):
        dist = math.sqrt((j[0] - m[0]) ** 2 + (j[1] - m[1]) ** 2)
        if dist < max_dist:
            max_dist = dist
            max_j = j
            max_m = m
    max_dist_list.append([p, q, max_j, max_m])

max_dist_list1 = []
for p, j in enumerate(jobCentroids):
    jRatio = j[0] / j[1]
    min_dist = 0
    for q, m in enumerate(machineCentroids):
        mRatio = m[0] / m[1]
        if (jRatio - mRatio < min_dist):
            min_dist = jRatio - mRatio
            max_j = j
            max_m = m
    max_dist_list1.append([p, q, max_j, max_m])

print max_dist_list1
#####################################################################################
dominates = ""
# all of them get scheduled on machine cluster 2
count = 0
i = 0




def worker(job, ic, im, dominates,mac):
    print "in worker thread",job.getJobID(),"\n"
    print "in oworker: ",mac.machineID,"\n"
    # mac.setCurrMem(mac.getCurrMem() - job.getMemUtil()) # this part needs to be checked once!
    # mac.setCurrCpu(mac.getCurrCpu() - job.getCpuUtil())
    # if ic == 1:  # case when the job is not fully processed by one machine only
    # mac.setCurrCpu(mac.getCurrCpu() - job.getCpuUtil())
    # if dominates == 'm':
    # mac.setCurrMem(mac.getCurrMem() - job.getMemUtil())  # this part needs to be checked once!
    if im == 1:
        time_machine_allocation.append((job.endTime - job.startTime) * mac.getCurrMem() / job.getMemUtil())
        time.sleep((job.endTime - job.startTime) * mac.getCurrMem() / (job.getMemUtil() * (10 ** 10)))  # VALIDATE THIS PART ONCE
    if ic == 1:
        # mac.setCurrMem(mac.getCurrMem() - job.getMemUtil())  # this part needs to be checked once!
        # mac.setCurrCpu(mac.getCurrCpu() - job.getCpuUtil())
        print "IC=1", job.getCpuUtil(), "********"
        # exit(0)
        time_machine_allocation.append((job.endTime - job.startTime) * mac.getCurrCpu() / job.getCpuUtil())
        time.sleep((job.endTime - job.startTime) * mac.getCurrCpu() / (job.getCpuUtil() * (10 ** 10)))
    if im == 0 and ic == 0:
        # mac.setCurrMem(mac.getCurrMem() - job.getMemUtil())  # this part needs to be checked once!
        # mac.setCurrCpu(mac.getCurrCpu() - job.getCpuUtil())
        time_machine_allocation.append((job.endTime - job.startTime) / 10 ** 6)
        time.sleep((job.endTime - job.startTime) / 10 ** 6)  # job processing time
    lock.acquire()
    print "lock acqure"
    free_machines.append(mac)
    lock.release()


def callFunction(job, mac, min_mem, min_cpu):
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
    return mac


def callFunction_incomplete(job):
    print "call function incomplete"
    global free_machines
    min_mem = 999
    min_cpu = 999
    global free_machines
    free_machines_temp = free_machines[:]
    print "FREE MACHINES: "
    for m in free_machines:
        print m.machineID
    for tempMac in free_machines:  # since we are considering only cluster number 2
        #free_machines_temp.append(tempMac)
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
                # now we have a best fit free machine
        # full block taken outside
        machine_allocated.append(mac)
        lock.acquire()
        try:
            print "MAC SELECTED: ", mac.machineID, "\n", "*************"
            print "acquired in incomplete"
            free_machines_temp.remove(mac)
        finally:
            lock.release()
            print "released in incomplete"
        # in case job requires more than one machine:
        if (mac.getCurrCpu() - job.getRemCpu()) < 0:
            # count += 1
            job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
            # job.setRemMem(job.getRemMem() - mac.getCurrMem())
            ic = 1
        else:
            ic = 0
        if (mac.getCurrMem() - job.getRemMem()) < 0:
            # count += 1
            # job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
            job.setRemMem(job.getRemMem() - mac.getCurrMem())
            im = 1
        else:
            im = 0
        t = threading.Thread(target=worker, args=(job, ic, im, dominates,mac))
        threads.append(t)
        t.start()
        if ic == 1 or im == 1:
            print "continued....."
            callFunction_incomplete(job)
    free_machines = free_machines_temp


done_jobs_count = 1  # CHECK ThiS ONCE
sum_time = 0.0
ic = 0
im = 0
machine_allocated = []
time_machine_allocation = []
start_time = time.time()
for job in job_q:  # each job is a job object

    # get the avg running time for all the done jobs
    sum_time += (job.endTime - job.startTime)
    print "sum time: ", sum_time
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
        time.sleep(avg_time / 10 ** 10)  # delta  # ##
    else:
        free_machines_temp = free_machines[:]
    mac1 = free_machines[0]
    mac = callFunction(job, mac1, min_mem, min_cpu)
    # now we have a best fit free machine
    # full block taken outside
    machine_allocated.append(mac)
    lock.acquire()
    free_machines_temp.remove(mac)
    lock.release()

    # in case job requires more than one machine:
    if (mac.getCurrCpu() - job.getRemCpu()) < 0:
        count += 1
        job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
        # job.setRemMem(job.getRemMem() - mac.getCurrMem())
        ic = 1
    if (mac.getCurrMem() - job.getRemMem()) < 0:
        count += 1
        # job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
        job.setRemMem(job.getRemMem() - mac.getCurrMem())
        im = 1
    t = threading.Thread(target=worker, args=(job, ic, im, dominates,mac))
    threads.append(t)
    t.start()
    # t.join()
    if ic == 1 or im == 1:
        print "continued....."
        callFunction_incomplete(job)
    i += 1
    done_jobs_count += 1
    print i
    ic = 0
    im = 0
    free_machines = free_machines_temp
    # print "COUNT: ",count

# for i in threads:
# i.join()
sum_time = sum(time_machine_allocation)
final_time_taken = sum_time * len(machine_allocated)
print "FINAL: ", final_time_taken
