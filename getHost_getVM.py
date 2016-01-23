from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys

class getHost_getVM(object):
    '''migrate vms from over utilized hosts'''

    #What are the values this module will accept, used to present
    #the user with options
    properties_validation = 'minimum_host_memoryMB=[0-9]*;safe_selection=True|False'
    TO_BYTES = 1024 * 1024
    MINIMUM_MEMORY_DEFAULT = 500
    SAFE_SELECTION_DEFAULT = 'True'
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
        return engine_hosts

    def _getFreeMemory(self, host):
        if not host.id in self.free_memory_cache:
            try:
                self.free_memory_cache[host.id] = host.get_statistics().get('memory.free').get_values().get_value()[0].get_datum()
            except Exception:
                self.free_memory_cache[host.id] = -1
        return self.free_memory_cache[host.id]


    def _getMaximumVmMemory(self, hosts, minimum_host_memory):
        maximum_vm_memory = 0
        for host in hosts:
            available_memory = self._getFreeMemory(host) - minimum_host_memory
            available_memory = min(available_memory, host.get_max_scheduling_memory())
            if available_memory > maximum_vm_memory:
                maximum_vm_memory = available_memory

        return maximum_vm_memory

    def vm_select(self, vms,maximum_vm_memory):
        selected_vm = None
        best_effort_vm = None
        for vm in vms:
               if vm.memory > maximum_vm_memory:
                  continue
               if (selected_vm is None or vm.memory >= selected_vm.memory):
                  selected_vm =vm
        return selected_vm



    def _getUnderUtilizedMainTHostList(self,mainTServerList,minimum_host_memory):
        over_utilizedmaintenance_host = None
        under_utilizedmaintenance_host = None
        for host in mainTServerList:
            if not host:
                continue
            free_memory = self._getFreeMemory(host)
            if(free_memory <= 0):
                continue
            if free_memory <= minimum_host_memory:
                continue
            if (over_utilizedmaintenance_host is None or self._getFreeMemory(over_utilizedmaintenance_host) > free_memory):
               over_utilizedmaintenance_host = host
        return over_utilizedmaintenance_host



    def _getUnderUtilizedMigraTHostList(self,migraTServerList,minimum_host_memory):
        over_utilizedmigrate_host = None
        under_utilizedmigrate_host = None
        for host in migraTServerList:
            if not host:
                continue
            free_memory = self._getFreeMemory(host)
            if(free_memory <= 0):
                continue
            if free_memory > minimum_host_memory:
               over_utilizedmigrate_host = host
            if (under_utilizedmigrate_host is None or self._getFreeMemory(under_utilizedmigrate_host) < free_memory):
               under_utilizedmigrate_host = host
        return under_utilizedmigrate_host
    def do_balance(self, vms_ids, mahosts_ids,mihosts_ids, args_map):
        conn = self._get_connection()
        if conn is None:
            return
        minimum_host_memory = long(args_map.get('minimum_host_memoryMB',self.MINIMUM_MEMORY_DEFAULT))
        minimum_host_memory = minimum_host_memory * self.TO_BYTES
        migraTServerList = self._get_hosts(mihosts_ids, conn)
        under_utilizedmigrate_host = (self._getUnderUtilizedMigraTHostList(migraTServerList,minimum_host_memory))
        mainTServerList = self._get_hosts(mahosts_ids, conn)
        maximum_vm_memory = self._getMaximumVmMemory(migraTServerList,minimum_host_memory)
        print maximum_vm_memory
        over_utilizedmaintenance_host = (self._getUnderUtilizedMainTHostList(mainTServerList,minimum_host_memory))
        host_vms = conn.vms.list('host=' + over_utilizedmaintenance_host.name)
        if host_vms is None:
            return
        selected_vm = self.vm_select(host_vms,maximum_vm_memory)
        return selected_vm, under_utilizedmigrate_host
        conn.disconnect()
