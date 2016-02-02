#!/usr/bin/env python
from ovirtsdk.xml import params
from ovirtsdk.api import API
from getHost_getVM import getHost_getVM
import sys
import os



def _getServerListInput():
    # Mainternace Moded Server List : mainTServerList
    #  Server List VM will be migrated List : migraTfile
    print 'Please enter the file name which you would like to set mainternance mode'
    mainTfile = raw_input(">")
    with open(mainTfile,"r") as f:
       mainTServerList = f.read().splitlines()
    print 'Please enter the file name which you would like to migrate VMs to'
    migraTfile = raw_input(">")
    with open(migraTfile,"r") as f:
       migraTServerList = f.read().splitlines()
    simulTVM = raw_input("How many VMs would you like to migrate at the same time : ")
    return mainTServerList, migraTServerList, simulTVM

if __name__ == '__main__':

   mainTServerList, migraTServerList, simultaneousVM  = _getServerListInput()
   vms_ids = []
   args_map = {}
   hmb=getHost_getVM()
   hmb.do_balance(vms_ids,mainTServerList,migraTServerList, simultaneousVM, args_map)
   #print "host is ", host.name
   #print "vm is ", vm.name
   #  migrateVm(vm,host)
