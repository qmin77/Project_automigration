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
    MINIMUM_MEMORY_DEFAULT = 50
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
        print " or ".join(["name=%s" % u for u in host_ids])
        engine_hosts = connection.hosts.list(query=" or ".join(["name=%s" % u for u in host_ids]))
#        print "engine_hosts", engine_hosts
        return engine_hosts
    
    def _get_vms(self, host_ids, connection):
 #          vmslists = connection.vms.list(query=" or ".join(["host=%s" % u for u in host_ids]))
           vmslists = connection.vms.list('host=rhevh-1.vsix.info or rhevh-2.vsix.info')
           print "vmslists : ", vmslists
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
        sorted_selected_vm = {} 
        dicted_selected_vm = {} 
        selected_vm = {}
        for vm in vms:
              print " 98 VM is ", vm 
              if vm.memory > maximum_vm_memory:
                    continue
              x=self._get_migratingfromVMs(conn)
              y=self._get_migratingtoVMs(conn)
              print "(self._get_migratingfromVMs : %s, self._get_migratingtoVMs : %s)" % (x,y) 
              #for mvm in self._get_migratingfromVMs(conn):
              #    print "vm name comparing %s %s" % (mvm.name, vm.name) 
              #    if mvm.name == vm.name:
              #      #del selected_vm[vm.name]
              #      print "currently, VM is migrating from is ", vm.id
              #      #selected_vm.pop("vm",None)
              #      print "after remove self._get_migratingfromVMs : ",x
              #      continue 
              if vm.status.state == "migrating": 
                  print "vm.status : " , vm.status.state
                  continue 
              selected_vm.update({vm.name:vm.memory})
              print "Line 107 selected vm{}: " , selected_vm 
              #sorted_selected_vm = sorted(selected_vm.items(), key=operator.itemgetter(1),reverse=True)
              sorted_selected_vm = sorted(selected_vm,key=selected_vm.__getitem__,reverse=True)
              #dicted_selected_vm = dict(sorted_selected_vm)
              #if selected_vm.keys()[0] is None:
              if sorted_selected_vm[0] is None:
                break
        print "113 line, dicted_selected_vm is" , (sorted_selected_vm)
        #print "114 line, dected_selected_vm is %s" % (sorted_selected_vm[0])
        #return dicted_selected_vm.keys()[0]
        return sorted_selected_vm[0]
        



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
            if host.name in self._get_migratingfromHosts(conn):
                del over_utilizedmaintenance_host[host]
                print "currently, the host is VM migrating from is", host.name 
                continue 
            over_utilizedmaintenance_host.update({host:free_memory})
            sorted_over_utilizedmaintenance_host = sorted(over_utilizedmaintenance_host.items(), key=operator.itemgetter(1))
            dicted_over_utilizedmaintenance_host = dict(sorted_over_utilizedmaintenance_host)
        #print "dicted_over_utilizedmaintenance_host", dicted_over_utilizedmaintenance_host
#        print "over_utilizedmaintenance_host is ", dicted_over_utilizedmaintenance_host.keys()[0].name

        return dicted_over_utilizedmaintenance_host.keys()[0]
        #return sorted_over_utilizedmaintenance_host[0](1)

    def migrateVm(self,vm,host):
        """
        Migrate vm.
        Parameters:
        * vm - vm to be migrated
        * host - host where the vm should be migrated
        """
        vm.migrate(params.Action(host=host))
    #    vm.waitForState(vm, states.vm.up, timeout=240)
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
            if host.name in self._get_migratingtoHosts(conn):
                del under_utilizedmigrate_host[host]
	        print "currently the host is VM migrating to is", host.name 
                continue 
            under_utilizedmigrate_host.update({host:free_memory})
            sorted_under_utilizedmigrate_host = sorted(under_utilizedmigrate_host.items(), key=operator.itemgetter(1),reverse=True) 
            dicted_under_utilizedmigrate_host = dict(sorted_under_utilizedmigrate_host)
#        print "under_utilizedmigrate_host is " , dicted_under_utilizedmigrate_host.keys()[0].name
        return dicted_under_utilizedmigrate_host.keys()[0]

    def do_balance(self, vms_ids, mahosts_ids,mihosts_ids, simultaneousVM, args_map):
        conn = self._get_connection()
        if conn is None:
            return
        minimum_host_memory = long(args_map.get('minimum_host_memoryMB',self.MINIMUM_MEMORY_DEFAULT))
        minimum_host_memory = minimum_host_memory * self.TO_BYTES
        migraTServerList = self._get_hosts(mihosts_ids, conn)
        mainTServerList = self._get_hosts(mahosts_ids, conn)
        # need to have rutin that check if vm or host migrating any VM to or from, if so, need to remove the list.  
        while len(self._get_vms(mahosts_ids,conn)) > 0: 
             while len(self._get_migratingfromVMs(conn)) <  simultaneousVM:
                  print len(self._get_migratingfromVMs(conn))
                  under_utilizedmigrate_host = (self._getUnderUtilizedMigraTHostList(migraTServerList,minimum_host_memory,conn))
                  maximum_vm_memory = self._getMaximumVmMemory(under_utilizedmigrate_host,minimum_host_memory)
                  over_utilizedmaintenance_host = (self._getOverUtilizedMainTHostList(mainTServerList,minimum_host_memory,conn))
                  host_vms = conn.vms.list('host=' + over_utilizedmaintenance_host.name)
                  if host_vms is None:
                      continue  
                  selected_vm = self.vm_select(host_vms,maximum_vm_memory,conn)
                  kselected_vm= conn.vms.list('name=' + selected_vm)
                  if kselected_vm is None:
                      continue 
                  print "vm is %s, %s " % (kselected_vm[0].id,kselected_vm[0].name)
                  print "under_utilizedmigrate_host is ", under_utilizedmigrate_host.name
                  self.migrateVm(kselected_vm[0],under_utilizedmigrate_host)
                  time.sleep(3)
             time.sleep(3)
        conn.disconnect()