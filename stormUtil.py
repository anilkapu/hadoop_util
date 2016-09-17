#!/bin/env python2.6

import json
import pycurl
from pycurl import *
import cStringIO
import pycurl

auth_url=""
username=""
password=""
topology_url=""

def get_token(auth_url, username, password):
    """
        Authenticate user and return generated token
        auth_url: Url to be used for BDSE
        Username: Username for BDSE
        Password: Password for the user
    """
    buf = cStringIO.StringIO()
    curl = pycurl.Curl()
    curl.setopt(curl.URL, auth_url)
    curl.setopt(pycurl.HTTPHEADER, ['Content-Type: application/json', 'Accept: application/json'])
    curl.setopt(curl.POSTFIELDS, '{"username":"%s", "password":"%s"}' %(username, password))
    curl.setopt(curl.VERBOSE, True)
    curl.setopt(curl.WRITEFUNCTION, buf.write)
    try:
        curl.perform()
        response = buf.getvalue()
        status = curl.getinfo(pycurl.HTTP_CODE)
        #response = json.loads(response)
        return response
    except Exception, e:
        print e.message



def get_topology_info(topologyName):
    """
        Returns topology information
    """
    token = get_token(auth_url, username, password)
    if(token):
        buf = cStringIO.StringIO()
        curl = pycurl.Curl()
        curl.setopt(curl.URL, topology_url)
        curl.setopt(pycurl.HTTPHEADER, ['Content-Type: application/json', '%s:%s' %("X-Auth-Token", token)])
        curl.setopt(curl.VERBOSE, True)
        curl.setopt(curl.WRITEFUNCTION, buf.write)
        try:
            curl.perform()
            response = buf.getvalue()
            status = curl.getinfo(pycurl.HTTP_CODE)
            response = json.loads(response)
            for item in response['topologies']:
                if (item['name'] == topologyName):
                    return item
            return ""
        except Exception,e:
            print e.message

def is_topology_status_active(objTopology):
    if(objTopology and objTopology != ""):
        if(objTopology['status'] == "ACTIVE"):
            return True
    return False
