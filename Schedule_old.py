import pickle
import math
import threading
import time

machine_dict = {}
threads = []
jobc_obj = pickle.load(open("jobc_obj.p", "r"))
machinec_obj = pickle.load(open("machinec_obj.p", "r"))
free_machines = machinec_obj[2]
for i in free_machines:
    machine_dict[i] = []
    machine_dict[i].append(0)

job_q = []
for i in jobc_obj.keys():
    job_q.extend(jobc_obj[i])

job_q.sort(key=lambda x: x.getStartTime())
pickle.dump(job_q, open("final_q.p", "wb"))

# sort based on the startTime of the Jobs(incoming jobs)
# tempCluster.sort(key=lambda x:x.getStartTime())
jobCentroids = pickle.load(open("centroids_job.p", "r"))
machineCentroids = pickle.load(open("centroids_machine.p", "r"))


# ## For pre-selecting a mC for a jC
max_dist_list = []
max_j = 0
max_m = 0
for p, j in enumerate(jobCentroids):
    max_dist = 999
    for q, m in enumerate(machineCentroids):
        dist = math.sqrt((j[0] - m[0]) ** 2 + (j[1] - m[1]) ** 2)  # ## Euclidean distance - to find dist btw a jobC and a machineC
        if dist < max_dist:
            max_dist = dist
            max_j = j
            max_m = m
    max_dist_list.append([p, q, max_j, max_m])  # ## for each jobC - a machineC is assigned ( along with jobC-id and machineC-id )

max_dist_list1 = []
for p, j in enumerate(jobCentroids):  # ## Ratio distance - btw a jobC and machineC
    jRatio = j[0] / j[1] # ## ratio - mem : cpu
    min_dist = 0
    for q, m in enumerate(machineCentroids):
        mRatio = m[0] / m[1]
        if (jRatio - mRatio < min_dist):  # ## !!! what if just ratio is bigger !!!
            min_dist = jRatio - mRatio
            max_j = j # ##can be put outside inner loop
            max_m = m
    max_dist_list1.append([p, q, max_j, max_m]) # ## for every jobC

# ## check it!!!
# eg : jRatio = 2
#
#           mRatio = 4
#           min_dist = -2
#
#           mRatio = 3
#           min_dist = -1 ( its not lesser than -2, so it wont go into for loop
#


# ## what if??? (check!)
#   eg :  specification of mem and cpu of -  j ( 0.6, 0.3 ) and m ( 1 , 1 )
#           jRatio = 2 and mRatio = 1
#           Here, jRatio - mRatio < min_dist = False, even though the machine is capable
# so, may be ratio is a bad idea
#

# ## we have to change the way we are taking the ratio ::
#
#   1) take ratio separately for cpu and mem [ cpuRatio(job:machine) and memRatio(j:m) ; we need one value to compare(we have 2 dimensions),i.e., combine the 2 ratios to one value to compare with min value ]
#   2) get one value for job by taking euclidian dist btw mem and cpu; similarly get a value for machine; then take ratio of these values (job:machine) .
#
#

print max_dist_list1
#####################################################################################
dominates = ""
# all of them get scheduled on machine cluster 2
count = 0
i = 0


def worker(job, dominates, mac):
    '''
    if im == 1:
        time_machine_allocation.append((job.endTime - job.startTime) * mac.getCurrMem() / job.getMemUtil())
        time.sleep((job.endTime - job.startTime) * mac.getCurrMem() / (
        job.getMemUtil() * (10 ** 10)))  # VALIDATE THIS PART ONCE
    if ic == 1:
        # mac.setCurrMem(mac.getCurrMem() - job.getMemUtil())  # this part needs to be checked once!
        # mac.setCurrCpu(mac.getCurrCpu() - job.getCpuUtil())
        print "IC=1", job.getCpuUtil(), "********"
        # exit(0)
        time_machine_allocation.append((job.endTime - job.startTime) * mac.getCurrCpu() / job.getCpuUtil())
        time.sleep((job.endTime - job.startTime) * mac.getCurrCpu() / (job.getCpuUtil() * (10 ** 10)))
    '''

    # mac.setCurrMem(mac.getCurrMem() - job.getMemUtil())  # this part needs to be checked once!
    # mac.setCurrCpu(mac.getCurrCpu() - job.getCpuUtil())
    time_machine_allocation.append((job.endTime - job.startTime) / 10 ** 10)
    time.sleep((job.endTime - job.startTime) / 10 ** 10)  # job processing time

    free_machines.append(mac)


done_jobs_count = 1  # CHECK ThiS ONCE
sum_time = 0.0
machine_allocated = []
time_machine_allocation = []
start_time = time.time()
for job in job_q:  # each job is a job object
    # get the avg running time for all the done jobs
    sum_time += (job.endTime - job.startTime)
    #print "sum time: ", sum_time
    #avg_time = sum_time / done_jobs_count # ## can be put outside the loop
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
        mac = free_machines[0]
        machine_dict[mac].append((job.endTime - job.startTime)/10 ** 10)
    else:
        # free_machines_temp = free_machines[:]
        mac = free_machines[0]
        machine_dict[mac].append((job.endTime - job.startTime)/10 ** 10)
    #print "call function"
    global free_machines
    free_machines_temp = []
    # ##
    for tempMac in free_machines:  # since we are considering only cluster number 2
        free_machines_temp.append(tempMac)
        diff_cpu = abs(tempMac.getCurrCpu() - job.getRemCpu())
        diff_mem = abs(tempMac.getCurrMem() - job.getRemMem())  # mem < 0
        if dominates == "m":
            if diff_mem <= min_mem and diff_cpu < min_cpu: # or diff_mem
                min_mem = diff_mem
                min_cpu = diff_cpu
                mac = tempMac
        elif dominates == "c":
            if diff_cpu <= min_cpu and diff_mem < min_mem:
                min_cpu = diff_cpu
                min_mem = diff_mem
                mac = tempMac
    free_machines = free_machines_temp
    #print "returning from call function"
    # now we have a best fit free machine
    # full block taken outside
    machine_allocated.append(mac)
    # lock.acquire()
    free_machines_temp.remove(mac)
    # lock.release()

    # in case job requires more than one machine:
    if (mac.getCurrCpu() - job.getRemCpu()) < 0:
        count += 1
        #job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
        # job.setRemMem(job.getRemMem() - mac.getCurrMem())
        ic = 1
        #continue
    elif (mac.getCurrMem() - job.getRemMem()) < 0:
        count += 1
        # job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
        # job.setRemMem(job.getRemMem() - mac.getCurrMem())
        im = 1
        continue
    else:
        t = threading.Thread(target=worker, args=(job, dominates, mac))
        threads.append(t)
        t.start()
        print "MachineID:", mac.machineID
        print "Job Resource:", job.getCpuUtil()
        print "total resource unused: ", mac.getCurrCpu() - job.getCpuUtil()
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

avg_time = sum_time / done_jobs_count
sum_time = sum(time_machine_allocation)
final_time_taken = sum_time  # * len(machine_allocated)
print "FINAL: ", final_time_taken
print "COUNT", count
machine_dict_final = {}
for mach in machine_dict.keys():
    temp_time = sum_time
    for index,time in enumerate(machine_dict[mach]):
        temp_time = temp_time-time
    machine_dict[mach][0] = temp_time


print "final time taken by machine 0: ",machine_dict
