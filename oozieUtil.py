	#!/usr/bin/python
import subprocess
import sys
import os
import shlex
from subprocess import PIPE
import requests
import common
import time

def submit_oozie(url, propertyFile):
	if url != "" and propertyFile != "":
			#print subprocess.Popen("oozie job -oozie http://b-bdata-r02g7-prod.phx2.symcpe.net:11000/oozie/ -submit -config job.properties", shell=True, stdout=subprocess.PIPE).stdout.read()
		print subprocess.Popen("oozie job -oozie %s -submit -config %s" % (url, propertyFile), shell=True, stdout=subprocess.PIPE).stdout.read()
	else:
		print "Invalid Input"
		sys.exit(1)
		
#def get_running_jobs(oozieHost):
#	cmd = "oozie jobs -oozie https://%s/oozie -localtime -filter status=RUNNING" %(oozieHost)
#	exit_code, res, std_err = exec_command(cmd)
#	print res
def is_oozie_job_running(oozieHost, appName):
	cmd = u'https://%s/oozie/v2/jobs?filter=name=%s;status=RUNNING' %(oozieHost, appName)
	res = requests.get(cmd)
	if res.status_code != 200:
		print "oozie webservice returned status code "+res.status_code
		return -1
	json_data = res.json()
	if len(json_data['workflows']) > 0:
		return True
	else:
		return False

def is_oozie_job_prep(oozieHost, appName):
	cmd = u'https://%s/oozie/v2/jobs?filter=name=%s;status=PREP' %(oozieHost, appName)
	res = requests.get(cmd)
	if res.status_code != 200:
		print "oozie webservice returned status code "+res.status_code
		return -1
	json_data = res.json()
	if len(json_data['workflows']) > 0:
		return True
	else:
		return False

def get_oozie_job_status(oozieHost, jobId):
	cmd = u'https://%s/oozie/v1/job/%s?show=info&timezone=localtime' %(oozieHost, jobId)
	res = requests.get(cmd)
	if res.status_code != 200:
		print "oozie webservice returned status code "+res.status_code
		return -1
	json_data = res.json()
	return json_data['status']

def start_oozie_job(oozieHost, jobId):
	if get_oozie_job_status(oozieHost, jobId) == 'PREP':
		cmd = u'oozie job -oozie https://%s/oozie -start %s' %(oozieHost, jobId)
		exit_code, stdo, stderr = exec_command(cmd)
		if exit_code != 0:
			print "Unable to start the job %s due to reason %s" %(jobId, exit_code)
			return -1
		return True
	else:
		return False
		#===========================================================================
	# 	cmd = u'https://%s/oozie/v1/job/%s?action=start&timezone=localtime' %(oozieHost, jobId)
	# 	res = requests.put(cmd)
	# 	res.
	# 	if res.status_code != 200:
	# 		print "oozie webservice returned status code "+res.status_code
	# 		return -1
	# 	json_data = res.json()
	# 	return json_data
	# return -1
	#===========================================================================

def get_oozie_job_id(oozieHost, appName):
	cmd = u'https://%s/oozie/v1/jobs?filter=name=%s' %(oozieHost, appName)
	res = requests.get(cmd)
	if res.status_code != 200:
		print "Failed to get job id due to:- %s: %s" %(res.status_code, res.reason)
		return -1
	json_data = res.json()
	if len(json_data['workflows']) > 0:
		return json_data['workflows'][0]['id']
	else:
		return ""
	
def poll_oozie_job(oozieHost, jobId, timeInterval=5, timeOut=-1):
	#===========================================================================
	# cmd = u'oozie job -oozie https://%s/oozie -poll %s -interval %s -timeout %s -verbose' %(oozieHost, jobId, timeInterval, timeOut)
	# exit_code, stdo, stderr = exec_command(cmd)
	# if exit_code != 0:
	# 	print "Unable to poll the job %s due to reason %s" %(jobId, exit_code)
	# 	return -1
	# 
	# else:
	# 	return True
	#===========================================================================
	timespent = 0
	while True:
		status = get_oozie_job_status(oozieHost, jobId)
		time.sleep(timeInterval)
		timespent += timeInterval
		# Monitor oozie-job
		if status in ('PENDING', 'ACCEPTED', 'RUNNING'):
			continue
		
		if status == 'SUSPENDED':
			print "Oozie job %s Suspended", jobId
			break
		
		if status == "SUCCEEDED":
			break
		
		if status in ('FAILED', 'KILLED'):
			print "Oozie workflow failed for job %s!!", jobId
			break
		
		if timeOut and timeOut not in ('-1', -1, '0') and timespent > timeOut:
			print "Oozie Workflow for job %s timed out!!", jobId
			break
		
	return status
	
def rerun_oozie_job(oozieHost, jobId):
	if get_oozie_job_status(oozieHost, jobId) in ('KILLED', 'SUCCCEDED', 'FAILED'):
		cmd = u'oozie job -oozie https://%s/oozie -rerun %s' %(oozieHost, jobId)
		exit_code, stdo, stderr = exec_command(cmd)
		if exit_code != 0:
			print "Unable to rerun the job %s due to reason %s" %(jobId, exit_code)
			return -1
		return True
	else:
		return False
		
def push_data_to_hdfs(sourcePath, destinationPath):
    #Rename folder#
    if not check_path_exist(sourcePath):
    	print sourcePath + " does not exist"
    	raise SystemExit()
    if check_hdfs_path_exist(destinationPath, 'd') != 0:
    	cmd = """hdfs dfs -mkdir %s""" %destinationPath
    	run_status = subprocess.Popen(shlex.split(cmd), shell=False, stdout=subprocess.PIPE).stdout.read()
    cmd = """hdfs dfs -put %s %s""" %(sourcePath, destinationPath) 	
    run_status =  subprocess.Popen(shlex.split(cmd), shell=False, stdout=subprocess.PIPE).stdout.read()
    
    if run_status == 0:
        print "Unable to copy file to destination!!!"
        raise SystemExit()

def check_path_exist(path):
	return os.path.exists(path)

def check_hdfs_path_exist(path, test='e'):
	"""
    Test the url.
        
    parameters
    ----------
    hdfs_url: string
        The hdfs url
    test: string, optional
        'e': existence
        'd': is a directory
        'z': zero length

        Default is an existence test, 'e'
    returns 0 if path exist
    else returns 1
    """
	cmd = """hadoop fs -test -%s %s""" %(test, path)
	#process = subprocess.Popen("""hadoop fs -test -%s %s""" %(test, path), shell=True, stdout=subprocess.PIPE)
	process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, shell=False)
	proc_stdout = process.communicate()[0].strip()
	return process.returncode

def exec_command(command):
    """
    Execute the command and return the exit status.
    """
    pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    stdo, stde = pobj.communicate()
    exit_code = pobj.returncode
    return exit_code, stdo, stde

