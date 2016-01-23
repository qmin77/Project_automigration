#!/usr/bin/env python
from ovirtsdk.xml import params
from ovirtsdk.api import API
from getHost_getVM import getHost_getVM
import sys
import os



def _getServerListInput():
    # Mainternace Moded Server List : mainTServerList
    #  Server List VM will be migrated List : migraTfile
    print 'Please enter the file name which would like to set mainternance mode'
    mainTfile = raw_input(">")
    with open(mainTfile,"r") as f:
       mainTServerList = f.read().splitlines()
    print 'Please enter the file name which would like to migrate VMs to'
    migraTfile = raw_input(">")
    with open(migraTfile,"r") as f:
       migraTServerList = f.read().splitlines()
    return mainTServerList, migraTServerList

def migrateVm(vm, host):
    """
    Migrate vm.
    Parameters:
     * vm - vm to be migrated
     * host - host where the vm should be migrated
    """
    vm.migrate(params.Action(host=host))
    waitForState(vm, states.vm.up, timeout=240)
    LOGGER.info("Migrated VM '%s' to host '%s'" % (vm.get_name(), host.get_name()))


if __name__ == '__main__':

   mainTServerList, migraTServerList = _getServerListInput()
   print mainTServerList, migraTServerList
   vms_ids = []
   args_map = {}
   hmb=getHost_getVM()
   vm, host = hmb.do_balance(vms_ids,mainTServerList,migraTServerList, args_map)
   print "host is ", host.name
   print "vm is ", vm.name
   migrateVm(vm,host)
