'''
Created on Dec 1, 2015

@author: AnilKumar_Kapu
'''
import subprocess

def get_hive_record_count(tblName):
    if tblName == "":
        print "Invalid database/tablename - please provide a valid database/table"
    
    recCount = subprocess.Popen("""hive -e 'select count(*) from %s'""" %(tblName), shell=True, stdout=subprocess.PIPE).stdout.read()
    return recCount

