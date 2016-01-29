from ovirtsdk.xml import params
from ovirtsdk.api import API
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
        print "migrating engine_hosts", engine_hosts
        return engine_hosts
    
    def _get_vms(self, host_ids, connection):
#          vmslists = connection.vms.list(" or ".join(["hosts= +%s" % u for u in host_ids]))
#         print (' or '.join(["'host=%s'" % u for u in host_ids]))
#         vmslists = connection.vms.list(query='host=rhevh-3.vsix.info')
          print "_get_vms list =", vmslists
          return vmslists 


    def _get_migratingfromVMs(self,conn):
	migratingfromVMs = conn.vms.list(query='status=migratingfrom')
        return migratingfromVMs
    
    def _get_migratingtoVMs(self,conn):
	migratingtoVMs = conn.vms.list(query='status=migratingto')
        return migratingtoVMs
    
    def _get_migratingfromHosts(self,conn):
	migratingfromHosts = conn.hosts.list(query='status=migratingfrom')
        return migratingfromHosts
    
    def _get_migratingtoHosts(self,conn):
	migratingtoHosts = conn.hosts.list(query='status=migratingto')
        return migratingtoHosts

    def _getFreeMemory(self, host):
        try:
              self.free_memory_cache.clear()
              self.free_memory_cache[host.id] = host.get_statistics().get('memory.free').get_values().get_value()[0].get_datum()
        except Exception:
                self.free_memory_cache[host.id] = -1
        #print "self.free_memory_cache[host.id]", self.free_memory_cache[host.id]
        return self.free_memory_cache[host.id]


    def _getMaximumVmMemory(self, host, minimum_host_memory):
        maximum_vm_memory = 0
        available_memory = self._getFreeMemory(host) - minimum_host_memory
        available_memory = min(available_memory, host.get_max_scheduling_memory())
        if available_memory > maximum_vm_memory:
            maximum_vm_memory = available_memory

        return maximum_vm_memory

    def vm_select(self, vms,maximum_vm_memory,conn):
        selected_vm = {}
        ####on going editing 
        ####on going editing 
        for vm in vms:
              if vm.memory > maximum_vm_memory:
                    continue
              if vm in self._get_migratingfromVMs(conn):
                    print "migrating VM is ", vm
                    del selected_vm[vm]
                    print "after deleting selected_vm : ", selected_vm
                    continue 
              selected_vm.update({vm:vm.memory})
              sorted_selected_vm = sorted(selected_vm.items(), key=operator.itemgetter(1))
              dicted_selected_vm = dict(sorted_selected_vm)
              if selected_vm.keys()[0] is None:
                break
        print "selected_vm is ", selected_vm.keys()[0]
        return selected_vm.keys()[0]
        



    def _getOverUtilizedMainTHostList(self,mainTServerList,minimum_host_memory,conn):
        over_utilizedmaintenance_host = {}
        under_utilizedmaintenance_host = None
        for host in mainTServerList:
            if not host:
                continue
            free_memory = self._getFreeMemory(host)
            print "_getOverUtilizedMainTHostList %s %d" % (host.name, free_memory)
            if(free_memory <= 0):
                 continue
            if free_memory < minimum_host_memory:
                 continue
            if host in self._get_migratingfromHosts(conn):
                 del over_utilizedmaintenance_host[host]
                 print "after deleting host: ", over_utilizedmaintenance_host
                 continue 
            over_utilizedmaintenance_host.update({host:free_memory})
            sorted_over_utilizedmaintenance_host = sorted(over_utilizedmaintenance_host.items(), key=operator.itemgetter(1))
            dicted_over_utilizedmaintenance_host = dict(sorted_over_utilizedmaintenance_host)
        print "over_utilizedmaintenance_host is ", dicted_over_utilizedmaintenance_host.keys()[0].name

        return dicted_over_utilizedmaintenance_host.keys()[0]

    def migrateVm(self,vm,host):
        """
        Migrate vm.
        Parameters:
        * vm - vm to be migrated
        * host - host where the vm should be migrated
        """
        vm.migrate(params.Action(host=host))
    #   waitForState(vm, states.vm.up, timeout=240)
    #   LOGGER.info("Migrated VM '%s' to host '%s'" % (vm.get_name(), host.get_name()))


    def _getUnderUtilizedMigraTHostList(self,migraTServerList,minimum_host_memory,conn):
        under_utilizedmigrate_host = {}
        for host in migraTServerList:
            if not host:
                continue
            free_memory = self._getFreeMemory(host)
            print "_getUnderUtilizedMigraTHostList %s %d" % (host.name, free_memory)
            if(free_memory <= 0):
                 continue
            if free_memory < minimum_host_memory:
                 continue 
            if host in self._get_migratingtoHosts(conn):
                 del under_utilizedmigrate_host[host]
	         print "corrently the host is VM migrating to is", host.name 
                 continue 
            under_utilizedmigrate_host.update({host:free_memory})
            #sorted_under_utilizedmigrate_host = sorted(under_utilizedmigrate_host.items(), key=operator.itemgetter(1),reverse=True) 
            sorted_under_utilizedmigrate_host = sorted(under_utilizedmigrate_host.items(), key=operator.itemgetter(1)) 
            dicted_under_utilizedmigrate_host = dict(sorted_under_utilizedmigrate_host)
        print "under_utilizedmigrate_host is " , dicted_under_utilizedmigrate_host.keys()[0].name
        return dicted_under_utilizedmigrate_host.keys()[0]

    def do_balance(self, vms_ids, mahosts_ids,mihosts_ids, args_map):
        conn = self._get_connection()
        if conn is None:
            return
        minimum_host_memory = long(args_map.get('minimum_host_memoryMB',self.MINIMUM_MEMORY_DEFAULT))
        minimum_host_memory = minimum_host_memory * self.TO_BYTES
        migraTServerList = self._get_hosts(mihosts_ids, conn)
        mainTServerList = self._get_hosts(mahosts_ids, conn)
        # need to have rutin that check if vm or host migrating any VM to or from, if so, need to remove the list.  
        while len(self._get_vms(mahosts_ids,conn)) > 0: 
             while len(self._get_migratingfromVMs(conn)) < 4:
                  print len(self._get_migratingfromVMs(conn))
                  under_utilizedmigrate_host = (self._getUnderUtilizedMigraTHostList(migraTServerList,minimum_host_memory,conn))
                  maximum_vm_memory = self._getMaximumVmMemory(under_utilizedmigrate_host,minimum_host_memory)
                  print maximum_vm_memory
                  over_utilizedmaintenance_host = (self._getOverUtilizedMainTHostList(mainTServerList,minimum_host_memory,conn))
                  host_vms = conn.vms.list('host=' + over_utilizedmaintenance_host.name)
                  if host_vms is None:
                      continue  
                  selected_vm = self.vm_select(host_vms,maximum_vm_memory,conn)
                  if selected_vm is None:
                      continue 
                  print "vm is ", selected_vm.name
                  print "under_utilizedmigrate_host is ", under_utilizedmigrate_host.name
                  self.migrateVm(selected_vm,under_utilizedmigrate_host)
                  time.sleep(10)
        conn.disconnect()
