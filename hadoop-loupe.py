#!/usr/bin/python


# Extracts the list of succeeded jobs from the history server

# imports
import sys
import os
import urllib2
import xml.etree.ElementTree as ET
from xml import etree
import argparse
import pygal

class TimeStats:
    def __init__(self):
        self.avgMapTime = 0
        self.avgReduceTime = 0
        self.avgShuffleTime = 0
        self.avgMergeTime = 0
        
        self.totalMapTime = 0
        self.totalReduceTime = 0
        self.totalShuffleTime = 0
        self.totalMergeTime = 0
        
        self.submitTime = float("inf")
        self.startTime = float("inf")
        self.finishTime = 0
        self.duration = 0
    
    def load_from_xml(self,jobxml):
        self.avgMapTime = long(jobxml.find('avgMapTime').text)
        self.avgReduceTime = long(jobxml.find('avgReduceTime').text)
        self.avgShuffleTime = long(jobxml.find('avgShuffleTime').text)
        self.avgMergeTime = long(jobxml.find('avgMergeTime').text)
        
        self.totalMapTime = long(jobxml.find('avgMapTime').text) * long(jobxml.find('mapsCompleted').text)
        self.totalReduceTime = long(jobxml.find('avgReduceTime').text) * long(jobxml.find('reducesCompleted').text)
        self.totalShuffleTime = long(jobxml.find('avgShuffleTime').text) * long(jobxml.find('reducesCompleted').text)
        self.totalMergeTime = long(jobxml.find('avgMergeTime').text) * long(jobxml.find('reducesCompleted').text)
        
        self.submitTime = long(jobxml.find('submitTime').text)
        self.startTime = long(jobxml.find('startTime').text)
        self.finishTime = long(jobxml.find('finishTime').text)
        self.duration = self.finishTime - self.startTime
        
    
    def getDict(self):
        return {
        "avgMapTime" : self.avgMapTime,
        "avgReduceTime" : self.avgReduceTime,
        "avgShuffleTime" : self.avgShuffleTime,
        "avgMergeTime" : self.avgMergeTime,
        "totalMapTime" : self.totalMapTime,
        "totalReduceTime" : self.totalReduceTime,
        "totalShuffleTime" : self.totalShuffleTime,
        "totalMergeTime" : self.totalMergeTime,
        #"submitTime" : self.submitTime,
        #"startTime" : self.startTime,
        #"finishTime" : self.finishTime,
        "duration" : self.duration,}
    
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        d = self.getDict();
        d['startTime'] = self.startTime
        return "\t=== Time ===\n" + "\n".join(map(lambda x : "\t" + x + ": " + str(d[x]),d)) + "\n"
     
    def correctAVGs(self, maps, reduces):
        self.avgMapTime = self.totalMapTime / max(maps,1)
        self.avgReduceTime = self.totalReduceTime / max(reduces,1)
        self.avgShuffleTime = self.totalShuffleTime / max(reduces,1)
        self.avgMergeTime = self.totalMergeTime / max(reduces,1)
           
    def add(self,t):
        self.avgMapTime += t.avgMapTime
        self.avgReduceTime += t.avgReduceTime
        self.avgShuffleTime += t.avgShuffleTime
        self.avgMergeTime += t.avgMergeTime
        
        self.totalMapTime = self.totalMapTime + t.totalMapTime
        self.totalReduceTime = self.totalReduceTime + t.totalReduceTime
        self.totalShuffleTime = self.totalShuffleTime + t.totalShuffleTime
        self.totalMergeTime = self.totalMergeTime + t.totalMergeTime 
        
        self.submitTime = min(self.submitTime,t.submitTime)
        self.startTime = min(self.startTime,t.startTime)
        self.finishTime = max(self.finishTime,t.finishTime)
        self.duration += t.duration
        

class NumberStats:
    def __init__(self):
        self.mapsTotal = 0
        self.mapsCompleted = 0
        self.reducesTotal = 0
        self.reducesCompleted = 0
    
    def load_from_xml(self,jobxml):
        self.mapsTotal = long(jobxml.find('mapsTotal').text)
        self.mapsCompleted = long(jobxml.find('mapsCompleted').text)
        self.reducesTotal = long(jobxml.find('reducesTotal').text)
        self.reducesCompleted = long(jobxml.find('reducesCompleted').text)
       
    def getDict(self):
        return {
        #"mapsTotal" : self.mapsTotal,
        "mapsCompleted" : self.mapsCompleted,
        #"reducesTotal" : self.reducesTotal,
        "reducesCompleted" : self.reducesCompleted,
        }

    def __repr__(self):
        return str(self)
        
    def __str__(self):
        d = self.getDict();
        return "\t=== Totals ===\n" + "\n".join(map(lambda x : "\t" + x + ": " + str(d[x]),d))+ "\n"
        
    def add(self,t):
        self.mapsTotal = self.mapsTotal + t.mapsTotal
        self.mapsCompleted = self.mapsCompleted + t.mapsCompleted
        self.reducesTotal = self.reducesTotal + t.reducesTotal
        self.reducesCompleted = self.reducesCompleted + t.reducesCompleted 
        


class Counter:
    
    def __init__(self, name, counterGroupName, mapCounterValue, reduceCounterValue, totalCounterValue):
        self.name = name
        self.counterGroupName = counterGroupName
        self.mapCounterValue = long(mapCounterValue)
        self.reduceCounterValue = long(reduceCounterValue)
        self.totalCounterValue = long(totalCounterValue)
        
        
    def getDict(self):
        return {
        "name" : self.name,
        "counterGroupName" : self.counterGroupName,
        "mapCounterValue" : self.mapCounterValue,
        "reduceCounterValue" : self.reduceCounterValue,
        "totalCounterValue" : self.totalCounterValue,
        }
        
    def add(self,c):
         self.mapCounterValue += c.mapCounterValue
         self.reduceCounterValue += c.reduceCounterValue
         self.totalCounterValue += c.totalCounterValue
         
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        # d = self.getDict();
        #return "\t=== Counter ===\n" + "\n".join(map(lambda x : "\t" + x + ": " + str(d[x]),d))+ "\n"
        return "\t%s: %s (%s,%s)\n"%(self.name,self.totalCounterValue,self.mapCounterValue,self.reduceCounterValue)


class CounterStats:
    def __init__(self):
        self.counters = {}
    
    def load_from_xml(self,counterxml):
        counterGroups = counterxml.findall('.//counterGroup')
        for group in counterGroups:
            groupName = group.find('./counterGroupName').text
            counters = group.findall('.//counter')
            for counter in counters:
                name = counter.find('name').text
                c = Counter(name, self.get_group_name(groupName, name), counter.find('mapCounterValue').text, counter.find('reduceCounterValue').text, counter.find('totalCounterValue').text)
                self.counters[groupName+"."+name] = c
    
    def get_group_name(self, groupName, name):
        if not "org.apache.hadoop" in groupName:
               return groupName
        
        if "bytes" in name.lower():
            return "Bytes"
            
        if "millis" in name.lower() and not "mb_millis" in name.lower():
            return "Millis"
            
        if "records" in name.lower():
            return "Records"
            
        return groupName
        
        
    def get_counter_names(self):
            return set(map(lambda x: (x.counterGroupName,x.name) , self.counters.values()))
            
    def get_counter_groupnames(self):
            return set(map(lambda x: x.counterGroupName , self.counters.values()))
            
    def get_counter_value(self,groupname, name):
            ctrs = filter(lambda x: x.counterGroupName == groupname and x.name == name, self.counters.values())
            if len(ctrs) == 0:
                return None
            else:
                return long(ctrs[0].totalCounterValue)
    
    def get_counter_names_by_groupname(self,groupname):
            return set(map(lambda x : x.name, filter(lambda x: x.counterGroupName == groupname, self.counters.values())))
        
         
    def add(self,s):
        for key in sorted(s.counters):
            c = s.counters[key]
            if key in self.counters:
                self.counters[key].add(c)
            else:
                self.counters[key] = Counter(c.name, c.counterGroupName, c.mapCounterValue, c.reduceCounterValue, c.totalCounterValue)
        
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        
        s = "\t=== Counters ===\n"
        for key in sorted(self.counters):
            s += str(self.counters[key])
        return s
    
class Application:
    
    def __init__(self, id):
        self.id = id
        
    def set_job_list(self, jobs):
        self.jobs = jobs
        
    def get_job_list(self):
        return self.jobs
                
    def get_counter_names(self):
        return set([item for job in self.jobs for item in job.counterStats.get_counter_names()])

        
    def aggregate(self):
        agg_job = Job(0,"aggregate")
        for job in self.jobs:
            agg_job.add(job)
            
        agg_job.timeStats.correctAVGs(agg_job.numberStats.mapsCompleted,agg_job.numberStats.reducesCompleted)
        
        return agg_job
     
class Task:
        
    def __init__(self, taskid):
        self.id = taskid
        self.type = "UNKNOWN"
        self.rack = "unknown"
        
        self.elapsedTime = 0
        self.elapsedShuffleTime = 0
        self.elapsedMergeTime = 0
        self.elapsedReduceTime = 0
        
    def load_from_xml(self, taskroot):
        

        self.type = taskroot.find('./type').text
        self.rack = taskroot.find('./nodeHttpAddress').text

        self.elapsedTime = long(taskroot.find('./elapsedTime').text)
        if self.type == "REDUCE":
            self.elapsedShuffleTime = long(taskroot.find('./elapsedShuffleTime').text)
            self.elapsedMergeTime = long(taskroot.find('./elapsedMergeTime').text)
            self.elapsedReduceTime = long(taskroot.find('./elapsedReduceTime').text)
        
    def getDict(self):
        return {
        "id" : self.id,
        "type" : self.type,
        "rack" : self.type,
        "elapsedTime" : self.elapsedTime,
        "elapsedShuffleTime" : self.elapsedShuffleTime,
        "elapsedMergeTime" : self.elapsedMergeTime,
        "elapsedReduceTime" : self.elapsedReduceTime,
        }
         
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        d = self.getDict();
        return "\t=== Task ===\n" + "\n".join(map(lambda x : "\t" + x + ": " + str(d[x]),d))+ "\n"


class Job:
        
    def __init__(self, id, name):
        self.id = id
        self.name = name
        
        self.timeStats = TimeStats()
        self.numberStats = NumberStats()
        
        self.counterStats = CounterStats()
        
        self.tasks = []
        
    def load_stats_from_xml(self,jobxml):
        self.timeStats.load_from_xml(jobxml)
        self.numberStats.load_from_xml(jobxml)
        
    def load_counters_from_xml(self,counterxml):   
        self.counterStats.load_from_xml(counterxml)
       
        
    def __repr__(self):
        return str(self)
        
    def __str__(self):
        return "\njob %s - name:%s\n"%(self.id,self.name) + str(self.timeStats) + str(self.numberStats) #+ str(self.counterStats) #+ str(self.tasks)
        
    def add(self, job):
            
        self.timeStats.add(job.timeStats)
        self.numberStats.add(job.numberStats)
        self.counterStats.add(job.counterStats)
        
    def add_task(self,t):
        self.tasks.append(t)

def get(url,headers):
    request = urllib2.Request(url, headers=headers)
    contents = urllib2.urlopen(request).read()
    return contents


# main retrieve function

def get_hadoop_application_stats(node, filter_fragment,filter_username, filter_start = "", filter_end = ""):
    historyserver = "%s:19888"%node
    headers = {'accept': 'application/xml'}
    # assemble job list
    url = 'http://%s/ws/v1/history/mapreduce/jobs?user=%s&status=SUCCEEDED&startedTimeBegin=%s&startedTimeEnd=%s'%(historyserver,filter_username,filter_start,filter_end)
    
    jobs = get(url,headers=headers)
    jobroot = ET.fromstring(jobs)
    
    # get counters and job stats for each job
    joblist = []
    for job in jobroot.findall('.//job'):
        j = Job(job.find('id').text,job.find('name').text)
        joblist.append(j)
    
    
    # filter jobs
    joblist = filter(lambda x: filter_fragment in x.name, joblist)


    # assemble job details
    for job in joblist:
        jobinfo = get('http://%s/ws/v1/history/mapreduce/jobs/%s'%(historyserver,job.id),headers=headers)
        jobroot = ET.fromstring(jobinfo)
        job.load_stats_from_xml(jobroot)
    
        jobcounters = get('http://%s/ws/v1/history/mapreduce/jobs/%s/counters'%(historyserver,job.id),headers=headers)
        # print jobcounters
        counterroot = ET.fromstring(jobcounters)
        job.load_counters_from_xml(counterroot)
        
        
        fetch_task_attempts(historyserver, headers, job)
    


    app = Application("app")
    app.set_job_list(joblist)
    #print "\n".join(map(lambda (x,y): x + "." + y, app.get_counter_names()))
    
    return app

def fetch_task_attempts(historyserver, headers, job):
    taskresult = get('http://%s/ws/v1/history/mapreduce/jobs/%s/tasks'%(historyserver,job.id),headers=headers)
    taskroot = ET.fromstring(taskresult)
    
    taskids = []
    for task in taskroot.findall('.//id'):
        taskids.append(task.text)
    
    taskids = sorted(taskids)
    # print taskids
    
    for taskid in taskids:
        attempts = get('http://%s/ws/v1/history/mapreduce/jobs/%s/tasks/%s/attempts'%(historyserver,job.id,taskid),headers=headers)
        attemptroot = ET.fromstring(attempts)
        for attempt in attemptroot.findall(".//taskAttempt"):
            # check if state is SUCCEEDED
            
            # create task from this attempt
            t = Task(taskid)
            t.load_from_xml(attempt)
            job.add_task(t)
           # print t
        
    

def jobs_time_data(joblist):
    
    keys = []
    totalData = []
    for job in joblist:
        timeData = job.timeStats.getDict()
        keys = sorted(timeData)
        data = []
        for key in keys:
            data.append(timeData[key])
        totalData.append((job.name, map(lambda x: x/1000, data)))
    
    return (keys,totalData)
    
def jobs_number_data(joblist):
    
    keys = []
    totalData = []
    for job in joblist:
        timeData = job.numberStats.getDict()
        keys = sorted(timeData)
        data = []
        for key in keys:
            data.append(timeData[key])
        totalData.append((job.name, data))
    
    return (keys,totalData)
 
def jobs_counter_data(joblist):
    
    # assemble all possible groups
    allgroups = set([])
    for job in joblist:
        allgroups.update(job.counterStats.get_counter_groupnames())
      
    allgroups = sorted(allgroups)
    
    result = []
    for group in allgroups:
        allnames = set([]) 
        # determine all counter names
        for job in joblist:
            allnames.update(job.counterStats.get_counter_names_by_groupname(group))
        
        # fixed ordered label set
        allnames = sorted(allnames)
        keys = allnames
        totalData = []
        
        # get counter data based on labels
        for job in joblist:
            data = []
            for name in keys:
                data.append(job.counterStats.get_counter_value(group,name))
            totalData.append((job.name, data))
                
        result.append((group,(keys,totalData)))
        
    return result
            
def get_task_data(job):
    keys = []
    totalData = [['elapsed',[]],['shuffle',[]],['merge',[]],['reduce',[]]]
    for task in job.tasks:
        data = []
        keys.append(task.type+"-"+task.id+"-"+task.rack)
        totalData[0][1].append(task.elapsedTime/1000)
        totalData[1][1].append(task.elapsedShuffleTime/1000)
        totalData[2][1].append(task.elapsedMergeTime/1000)
        totalData[3][1].append(task.elapsedReduceTime/1000)
    
    return (keys,totalData)    


def render(data, filename, title):
    labels = data[0]
    valuesets = data[1]

    bar_chart = pygal.Bar(x_label_rotation=45,label_font_size=14, major_label_font_size=16, human_readable=True,)
    bar_chart.title = title
    bar_chart.x_labels = map(str, labels)

    for valueset in valuesets:
        bar_chart.add(valueset[0],valueset[1])

    bar_chart.render_to_file(filename)
    


# pygal binding
def export_html(dirname, app_stats):
    

    joblist = app_stats.get_job_list()
    counterdata = jobs_counter_data(joblist)

    # create directory
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    # create images
    render(jobs_time_data(joblist),dirname+"/"+dirname+"_time_jobs.svg", "Job Time Breakdown")
    render(jobs_number_data(joblist),dirname+"/"+dirname+"_numbers_jobs.svg", "Job Number Breakdown")

    render(jobs_time_data([app_stats.aggregate()]),dirname+"/"+dirname+"_time_total.svg", "Overall Times")
    render(jobs_number_data([app_stats.aggregate()]),dirname+"/"+dirname+"_numbers_total.svg", "Overall Number")


    # job counters
    i = 0
    counterhtml = ""
    for group in counterdata:
        # print group[1]
        render(group[1],dirname+"/"+dirname+"_counters_"+str(i)+".svg", "Counters " + group[0])
        counterhtml += '    <embed type="image/svg+xml" src="${app_name}_counters_${n}.svg" class="halfsize" />'
        counterhtml = counterhtml.replace("${n}",str(i))
        i += 1


    # task times
    taskhtml = ""
    i = 0
    for job in joblist:
        
        # convert tasks to data
        data = get_task_data(job)
        # print data
        
        # make graph
        render(data,dirname+"/"+dirname+"_tasks_job_"+str(i)+".svg", "Tasks for " + job.name)
        
        # create html
        taskhtml += '    <embed type="image/svg+xml" src="${app_name}_tasks_job_${n}.svg" class="halfsize" />'
        taskhtml = taskhtml.replace("${n}",str(i))
        
        i += 1
        

    # create html page
    html = """
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    body 
    {
        background-color: black;
        color: white;
    }
    h1, h2   
    {
        text-align:center;
    }
    
    .halfsize {
        width: 45%;
    }
    </style>
    </head>
    <body>

    <h1>
    Application ${app_name}
    </h1>


    <h2>
    Overall stats
    </h2>
    <figure>
        <embed type="image/svg+xml" src="${app_name}_time_total.svg" class="halfsize" />
        <embed type="image/svg+xml" src="${app_name}_numbers_total.svg" class="halfsize" />
    </figure>


    <h2>
    Job stats
    </h2>
    <figure>
        <embed type="image/svg+xml" src="${app_name}_time_jobs.svg" class="halfsize" />
        <embed type="image/svg+xml" src="${app_name}_numbers_jobs.svg" class="halfsize" />
    </figure>


    <h2>
    Job Counters
    </h2>
    ${counterhtml}


    <h2>
    Task stats per job
    </h2>

    ${taskhtml}
    

    </body>
    </html>
    """
    html = html.replace("${counterhtml}", counterhtml)
    html = html.replace("${taskhtml}", taskhtml)
    html = html.replace("${app_name}", dirname)


    f = open(dirname+'/index.html', 'w')
    f.write(html)
    f.close()

def main():
    
    parser = argparse.ArgumentParser(description='Hadoop Job History Extractor.')
    parser.add_argument("-s", "--server", action='store',required=True,\
        help="the address of the job history server")
    parser.add_argument("-o", "--output", action='store', required=True,\
        help="folder where to put the output")
    parser.add_argument("-u", "--user", action='store', required=False, default="",\
        help="filter jobs based on username")
    parser.add_argument("-b", "--startTimeBegin", action='store', default="",\
        help="lower bound for job start time")
    parser.add_argument("-e", "--startTimeEnd", action='store', default="",\
        help="upper bound for job start time")
    parser.add_argument("-j", "--jobname", action='store', default="",\
        help="jobs names are required to contain this substring")
    parser.add_argument("-v", "--verbose", action='store_true', default=False,\
        help="display settings an job details in output")
    args = parser.parse_args()
    # print args
    #print args.accumulate(args.integers)
    
    
    # get parameters
    params = vars(args)
    
    verbose = params['verbose']
    # print params
    if verbose:
        print reduce(lambda x,y: x  + "\n\t" + y + ": " + str(vars(args)[y]),vars(args), "Settings:")
    

    app_stats = get_hadoop_application_stats(params['server'], params['jobname'], params['user'], params['startTimeBegin'], params['startTimeEnd'])
    # app_name = filter_name
    #
    joblist = app_stats.get_job_list()
    if verbose:
        print joblist
    print "found %s jobs"%len(joblist)
    #
    # print app_stats.aggregate()
    #
    # print jobs_time_data(joblist)
    # print jobs_number_data(joblist)
    export_html(params['output'], app_stats)
    
    if verbose:
        print "done."
    

        
if __name__ == '__main__':
    main()



