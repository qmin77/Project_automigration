from time import sleep
import logging
import ovirtsdk.api
from ovirtsdk.api import API
from ovirtsdk.xml import params
from ovirtsdk.infrastructure import errors
from ovirtsdk.infrastructure import contextmanager
from functools import wraps
logging.basicConfig(filename='messages.log',level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)
import sys
import time 
import operator 

class getHost_getVM(object):
    '''migrate vms from over utilized hosts'''

    #What are the values this module will accept, used to present
    #the user with options
    properties_validation = 'minimum_host_memoryMB=[0-9]*;safe_selection=True|False'
    TO_BYTES = 1024 * 1024
    MINIMUM_MEMORY_DEFAULT = 500
    free_memory_cache = {}

    def inputid(self):
        print "please, enter rhev administrator's ID and Password"
        user = raw_input('Enter your username: ')
        passwd = getpass.getpass()
        return user, passwd
        print 'id : ', user
        print 'Password : ', passwd

    def _get_connection(self):
        #open a connection to the rest api
        
        connection = None
        try:
             connection = API(url='https://rhev-m.vsix.info:443',username='admin@internal',password='rhev',ca_file='ca.crt')
        except BaseException as ex:
            #letting the external proxy know there was an error
            print >> sys.stderr, ex
            return None

        return connection



    def _get_hosts(self, host_ids, connection):
        engine_hosts = connection.hosts.list(query=" or ".join(["name=%s" % u for u in host_ids]))
        return engine_hosts
    
    def _get_vms(self, host_ids, connection):
           vmslists = connection.vms.list(query=" or ".join(["host.name=%s" % u for u in host_ids]))
           return vmslists 


    def _get_migratingfromVMs(self,conn):
	migratingfromVMs = conn.vms.list(query='status=migratingfrom')
	#print "migrating VMs are",  " and ".join(["%s".name % u for u in migratingfromVMs])
        return migratingfromVMs
    
    def _get_migratingtoVMs(self,conn):
	migratingtoVMs = conn.vms.list(query='status=migratingto')
        return migratingtoVMs
    
    def _get_migratingfromHosts(self,conn):
	migratingfromHosts = conn.hosts.list(query='vms.status=migratingfrom')
        return migratingfromHosts
    
    def _get_migratingtoHosts(self,conn):
	migratingtoHosts = conn.hosts.list(query='vms.status=migratingto')
        return migratingtoHosts

    def _getFreeMemory(self, host):
        try:
              self.free_memory_cache.clear()
              self.free_memory_cache[host.id] = host.get_statistics().get('memory.free').get_values().get_value()[0].get_datum()
        except Exception:
                self.free_memory_cache[host.id] = -1
        return self.free_memory_cache[host.id]


    def _getMaximumVmMemory(self, host, minimum_host_memory):
        maximum_vm_memory = 0
        available_memory = self._getFreeMemory(host) - minimum_host_memory
        available_memory = min(available_memory, host.get_max_scheduling_memory())
        if available_memory > maximum_vm_memory:
            maximum_vm_memory = available_memory
        #print "available_memory is %s, host.get_max_scheduling_memory is %s" % (available_memory,host.get_max_scheduling_memory()) 

        return maximum_vm_memory

    def vm_select(self, vms,maximum_vm_memory,conn):
        sorted_selected_vm = {} 
        selected_vm = {}
        for vm in vms:
              #print "%s's vm.memory %s and maximum_vm_memory is %s" % (vm.name, vm.memory, maximum_vm_memory)
              if vm.memory > maximum_vm_memory:
                    continue
              #x=self._get_migratingfromVMs(conn)
              #y=self._get_migratingtoVMs(conn)
              #w=self._get_migratingfromHosts(conn)
              #z=self._get_migratingtoHosts(conn)
              ##print "(self._get_migratingfromVMs : %s, self._get_migratingtoVMs : %s)" % (x,y) 
              #print "(self._get_migratingfromHosts: %s, self._get_migratingtoHosts : %s)" % (w,z) 
              if vm.status.state == "migrating": 
                  #print "vm.status : " , vm.status.state
                  continue 
              selected_vm.update({vm.name:vm.memory})
              sorted_selected_vm = sorted(selected_vm,key=selected_vm.__getitem__,reverse=True)
              if sorted_selected_vm[0] is None:
                 break
        return sorted_selected_vm[0]

        



    def _getOverUtilizedMainTHostList(self,mainTServerList,minimum_host_memory,conn):
        over_utilizedmaintenance_host = {}
        under_utilizedmaintenance_host = None
        sorted_over_utilizedmaintenance_host = {} 
        dicted_over_utilizedmaintenance_host = {} 
        for host in mainTServerList:
            if not host:
                continue
            if host.status.state != 'up':
                continue 
            if len(conn.hosts.list(query='vms.status=migratingfrom and name='+host.name))>0:
                print "Currently, %s is migrating VMs to some hypervisor\nThe script will choose other hypervisor \n" % (host.name)
                continue 
            vmscount = conn.vms.list(query ='hosts.status=up' and 'host=' + host.name)
            if vmscount<=0:
                continue 
            free_memory = self._getFreeMemory(host)
            print "_getOverUtilizedMainTHostList %s %d" % (host.name, free_memory)
            if(free_memory <= 0):
                continue
            if free_memory < minimum_host_memory:
                continue
            print "hosts.status.state: ", host.status.state

            over_utilizedmaintenance_host.update({host:free_memory})
            sorted_over_utilizedmaintenance_host = sorted(over_utilizedmaintenance_host.items(), key=operator.itemgetter(1))
            dicted_over_utilizedmaintenance_host = dict(sorted_over_utilizedmaintenance_host)
        try:
             return dicted_over_utilizedmaintenance_host.keys()[0]
        except KeyError, e:   
             print "There is no proper hypervisor for vm migration from mainTServerList. ther script will do another looping\n"
             return None 
        except IndexError, e:   
             print "There is no proper hypervisor for vm migration from mainTServerList. ther script will do another looping\n"
             return None 
    def migrateVm(self,vm,host):
        """
        Migrate vm.
        Parameters:
        * vm - vm to be migrated
        * host - host where the vm should be migrated
        """
        vm.migrate(params.Action(host=host))
       # vm.waitForState(vm, states.vm.migrating, timeout=240)
    #   LOGGER.info("Migrated VM '%s' to host '%s'" % (vm.get_name(), host.get_name()))


    def _getUnderUtilizedMigraTHostList(self,migraTServerList,minimum_host_memory,conn):
        under_utilizedmigrate_host = {}
        sorted_under_utilizedmigrate_host = {} 
        dicted_under_utilizedmigrate_host = {}
        for host in migraTServerList:
            if not host:
                continue
            if host.status.state != 'up':
                continue 
            if host.get_max_scheduling_memory()<=0:
                continue 
            if len(conn.hosts.list(query='vms.status=migratingto and name='+host.name))>0:
                    print "currently, The %s is having VMs from some hypervisor.\nThe script will choose other hypervisor \n" % (host.name)
                    continue
            free_memory = self._getFreeMemory(host)
            print "_getUnderUtilizedMigraTHostList %s %d" % (host.name, free_memory)
            if(free_memory <= 0):
                continue
            if free_memory < minimum_host_memory:
                continue 
            under_utilizedmigrate_host.update({host:free_memory})
            sorted_under_utilizedmigrate_host = sorted(under_utilizedmigrate_host.items(), key=operator.itemgetter(1),reverse=True) 
            dicted_under_utilizedmigrate_host = dict(sorted_under_utilizedmigrate_host)
          #  print dicted_under_utilizedmigrate_host.keys()[0]
        try: 
            return dicted_under_utilizedmigrate_host.keys()[0]
        except KeyError, e:
            print "There are no proper hypervisor for vm migration from migraTServerList. the script will do another looping\n"
            return None
        except IndexError, e:
            print "There are no proper hypervisor for vm migration from migraTServerList. the script will do another looping\n"
            return None

    def do_balance(self, vms_ids, mahosts_ids,mihosts_ids, simultaneousVM, args_map):
        conn = self._get_connection()
        if conn is None:
            return
        minimum_host_memory = long(args_map.get('minimum_host_memoryMB',self.MINIMUM_MEMORY_DEFAULT))
        minimum_host_memory = minimum_host_memory * self.TO_BYTES
        migraTServerList = self._get_hosts(mihosts_ids, conn)
        mainTServerList = self._get_hosts(mahosts_ids, conn)
        while len(self._get_vms(mahosts_ids,conn)) > 0: 
            while len(self._get_migratingfromVMs(conn)) <=  simultaneousVM and len(self._get_vms(mahosts_ids,conn)) > 0:
                      under_utilizedmigrate_host = (self._getUnderUtilizedMigraTHostList(migraTServerList,minimum_host_memory,conn))
                      if under_utilizedmigrate_host is not None:
                         maximum_vm_memory = self._getMaximumVmMemory(under_utilizedmigrate_host,minimum_host_memory)
                      else:
                         continue  
                      over_utilizedmaintenance_host = (self._getOverUtilizedMainTHostList(mainTServerList,minimum_host_memory,conn))
                      if over_utilizedmaintenance_host is not None:
                          host_vms = conn.vms.list(query ='hosts.status=up' and 'host=' + over_utilizedmaintenance_host.name)
                      else:
                          continue                       
                      print "host_vms is", host_vms 
                      if not host_vms:
                          continue  
                      selected_vm = self.vm_select(host_vms,maximum_vm_memory,conn)
                      if not selected_vm:
                          continue  
                      kselected_vm= conn.vms.list('name=' + selected_vm)
                      if not kselected_vm:
                           continue 
                      print "Migrating vm will be %s on %s\n" % (kselected_vm[0].name,over_utilizedmaintenance_host.name)
                      print "The VM is migrating to %s\n" % (under_utilizedmigrate_host.name)
                      self.migrateVm(kselected_vm[0],under_utilizedmigrate_host)
                      #time.sleep(1)
        conn.disconnect()
        print "VM Migration has completed\n"
