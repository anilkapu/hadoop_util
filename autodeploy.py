set +x
cat >/tmp/autodeploy.py <<EOF
# This library requires "fabric" module to be installed

from fabric.api import *
from fabric.contrib.files import cd
from StringIO import StringIO
from fabric.contrib.files import exists

import os, urllib2
import json
import requests
import time

os.environ["http_proxy"]=''
env.cluster_host = '${ClusterHost}'
env.hosts = '${VM_ClusterIP}'
env.user = '${ClusterUserName}'
env.password = '${ClusterPassword}'

def push_data_to_hdfs(sourcePath, destinationPath):
    #Rename folder#
    if not check_path_exist(sourcePath):
    	print sourcePath + " does not exist"
    	raise SystemExit()

    with settings(warn_only=True):
		if check_hdfs_path_exist(destinationPath, 'd') != 0:
			run_status = run('sudo -u hdfs hdfs dfs -mkdir ' + destinationPath)

    run_status =  run("""sudo -u hdfs hdfs dfs -put %s %s""" %(sourcePath, destinationPath))

    if run_status == 0:
        print "Unable to copy file to destination!!!"
        raise SystemExit()

def check_path_exist(path):
	return exists(path, use_sudo=True)

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
	run("""sudo -u hdfs hadoop fs -test -%s %s""" %(test, path))
	return run('echo $?')

def cleanup_existing_build(cur_path, hdfs_path):
	run_status =  run('rm -r -f cur_path')
       	if run_status == 0:
            print "Not able to delete Piisub targets from jumpbox!!!"
            raise SystemExit()

	run_status =  run('hdfs dfs -rm -r -f cur_path hdfs_path')
       	if run_status == 0:
            print "Not able to delete Piisub targets from HDFS!!!"
            raise SystemExit()


def put_build(jenkins_loc, local_loc):
	run_status =  put('jenkins_loc, 'local_loc')
       	if run_status == 0:
            print "Not able to push Piisub targets!!!"
            raise SystemExit()


def extract_build(build_path):
	run_status =  run("tar xzvf {}".format(build_path))
       	if run_status == 0:
            print "Not able to extract Piisub targets!!!"
            raise SystemExit()

def push_build_to_hdfs(cur_path, hdfs_path):

    #Rename folder#
    run_status =  run("mv {} {}".format(cur_path, hdfs_path))
   	if run_status == 0:
        print "Unable to rename directory!!!"
        raise SystemExit()

        #Copy local hive-site.xml#
	run_status =  run("cp //etc//hive//conf//hive-site.xml converter_bulkmode")
   	if run_status == 0:
        print "Not able to copy local hive-site.xml!!!"
        raise SystemExit()

#Replace Dev Cluster config file with the one from the build#
    run_status =  run('mv {}/config/job-dev.properties {}/config/job.properties'.format(cur_path, hdfs_path))

        #put build to HDFS#
	run_status =  run("hdfs dfs -put converter_bulkmode")
  	if run_status == 0:
        print "Not able to extract Piisub targets!!!"
        raise SystemExit()


def create_hive_tables(hdfs_path):
    with cd("{}/hive/".format(hdfs_path)):
        output =  run("ls -p create*.hql | grep -v /")
        if output == 0:
            print "Cannot list Hive scripts"
            raise SystemExit()
        files = output.split()
        for file in files:
            execute_hive_script(file)

def execute_hive_script(file):
	run_status = run('hive -f %s' % file)
	if run_status == 0:
		print "Not able to execute Hive script %s " % file
		raise SystemExit()

def execute_oozie(hdfs_path):
    with cd(hdfs_path):
        output = run("oozie job -oozie http://{}:11000/oozie/ -config {}/config/job.properties -run".format(env.ClusterHost, hdfs_path))
        if output == 0:
            print "Error executing Oozie Workflow"
            raise SystemExit()

                #split output to get job_id#
            job_id = output[5:]
                #print "Job_id: ", job_id
                #Check oozie job status periodically till completion#
            job_result = poll(job_id)
            print "Oozie workflow ", job_id, job_result

def poll(jobid, timeout=-1, poll_interval=10):
    timespent = 0

    while True:
        status = check_oozie_job_status(jobid)

        time.sleep(poll_interval)
        timespent += poll_interval

        # Monitor oozie-job
        if status in ('PENDING', 'ACCEPTED'):
            continue

        if status == 'SUSPENDED':
            print "Oozie job %s Suspended", jobid
			break

        if status == "SUCCEEDED":
            break

        if status in ('FAILED', 'KILLED'):
            print "Oozie workflow failed for job %s!!", jobid
            break

        if timeout and timeout not in ('-1', -1, '0') and timespent > timeout:
            print "Oozie Workflow for job %s timed out!!", jobid
            break

    return status

def check_oozie_job_status(jobid):

    print "Checking Oozie job status"
    print jobid
    payload=jobid + " | grep Status | head -n 1"
    job_status = run('oozie job -oozie http://{}:11000/oozie -info {}'.format(env.ClusterHost, payload))
    job_status_parts = job_status.split(':')
    job_status = job_status_parts[1].strip()

    print job_status
    if job_status == 0:
		print "Not able to check status for Oozie job %s " % jobid


    return job_status


EOF

echo "################## Delete Existing Build Piiping Targets ##################"
fab -f /tmp/autodeploy.py cleanup_existing_build

echo "################## Copy Piiping Targets ##################"
fab -f /tmp/autodeploy.py put_build

echo "################## Extract Piiping Targets ##################"
fab -f /tmp/autodeploy.py extract_build

echo "################## Push Piiping Targets To HDFS ##################"
fab -f /tmp/autodeploy.py push_build_to_hdfs

echo "################## Create Hive Tables ##################"
fab -f /tmp/autodeploy.py create_hive_tables

echo "################## Execute Oozie ##################"
fab -f /tmp/autodeploy.py execute_oozie
