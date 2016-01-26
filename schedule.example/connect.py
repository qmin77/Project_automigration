#!/usr/bin/env python
from ovirtsdk.api import API
from ovirtsdk.xml import params
#from time import sleep
#import logging
#from ovirtsdk.infrastructure import errors
#from ovirtsdk.infrastructure import contextmanager
#from functools import wraps

class host_memory_balance(object):
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
            connection = API(url='http://host:port',
                             username='user@domain', password='')
        except BaseException as ex:
            #letting the external proxy know there was an error
            print >> sys.stderr, ex
            return None

        return connection

    def _get_hosts(self, host_ids, connection):
        #get all the hosts with the given ids
        engine_hosts = connection.hosts.list(
            query=" or ".join(["id=%s" % u for u in host_ids]))

        return engine_hosts

    def getFreeMemory(self, host):
        #getiing free memory requires a REST call, so cache results
        if not host.id in self.free_memory_cache:
            try:
                self.free_memory_cache[host.id] = host.get_statistics().get(
                    'memory.free').get_values().get_value()[0].get_datum()
            except Exception:
                self.free_memory_cache[host.id] = -1

        return self.free_memory_cache[host.id]

    def getOverUtilizedHostAndUnderUtilizedList(self, engine_hosts, minimum_host_memory):
        '''return the most over utilized host,
        and a list of under utilized hosts'''
        over_utilized_host = None
        under_utilized_hosts = []
        for host in engine_hosts:
            if not host:
                continue

            free_memory = self.getFreeMemory(host)
            if(free_memory <= 0):
                continue
            if free_memory > minimum_host_memory:
                    under_utilized_hosts.append(host)
                    continue
                #take the host with least amount of free memory
            if (over_utilized_host is None or
                    self.getFreeMemory(over_utilized_host)
                    > free_memory):
                    over_utilized_host = host
        return over_utilized_host, under_utilized_hosts

    def getMaximumVmMemory(self, hosts, minimum_host_memory):
        '''the maximum amount of memory that a migrated vm can have
        without sending the other hosts over the threshold'''
        maximum_vm_memory = 0
        for host in hosts:
            available_memory = self.getFreeMemory(host) - minimum_host_memory

            available_memory = min(available_memory,
                                   host.get_max_scheduling_memory())
            if available_memory > maximum_vm_memory:
                maximum_vm_memory = available_memory

        return maximum_vm_memory

    def getBestVmForMigration(self, vms, maximum_vm_memory, memory_delta, safe):
        #safe -> select the smallest vm
        #not safe -> try and select the smallest vm larger then the delta,
        #   if no such vm exists take the largest one

        #migrating a small vm is more likely to succeeded and puts less strain
        #on the network
        selected_vm = None
        best_effort_vm = None
        for vm in vms:
                if vm.memory > maximum_vm_memory:
                    #never select a vm that will send all the under
                    #utilized hosts over the threshold
                    continue
                if safe:
                    if (selected_vm is None or
                            vm.memory < selected_vm.memory):
                        selected_vm = vm
                else:
                    if vm.memory > memory_delta:
                        if (selected_vm is None or
                                vm.memory < selected_vm.memmory):
                            selected_vm = vm
                if (best_effort_vm is None or
                        vm.memory > best_effort_vm.memory):
                    best_effort_vm = vm

        if not safe and selected_vm is None:
            selected_vm = best_effort_vm

        return selected_vm

    def do_balance(self, hosts_ids, args_map):
        '''selects a vm from the most over utilized vm to migrate.
        if safe_selection is true selects the smallest vm from the host
        if safe_selection is false try and take a vm larger then the amount of memory the host is missing'''
        conn = self._get_connection()
        if conn is None:
            return

        #get our parameters from the map
        minimum_host_memory = long(args_map.get('minimum_host_memoryMB',
                                                self.MINIMUM_MEMORY_DEFAULT))
        minimum_host_memory = minimum_host_memory * self.TO_BYTES
        safe = bool(args_map.get('safe_selection',
                                 self.SAFE_SELECTION_DEFAULT))

        #get all the hosts with the given ids
        engine_hosts = self._get_hosts(hosts_ids, conn)

        over_utilized_host, under_utilized_hosts = (
            self.getOverUtilizedHostAndUnderUtilizedList(engine_hosts,
                                                         minimum_host_memory))

        if over_utilized_host is None:
            return

        maximum_vm_memory = self.getMaximumVmMemory(under_utilized_hosts,
                                                    minimum_host_memory)

        #amount of memory the host is missing
        memory_delta = (
            minimum_host_memory -
            self.getFreeMemory(over_utilized_host))

        host_vms = conn.vms.list('host=' + over_utilized_host.name)
        if host_vms is None:
            return

        #get largest/smallest vm that will
        selected_vm = self.getBestVmForMigration(host_vms, maximum_vm_memory,
                                                 memory_delta, safe)
        # try another host?
        if selected_vm is None:
            return

        under_utilized_hosts_ids = [host.id for host in under_utilized_hosts]
        print (selected_vm.id, under_utilized_hosts_ids)


def migrateVm(vm, host):
     	vm.migrate(params.Action(host=host))
#	waitForState(vm, states.vm.up, timeout=240)
     	LOGGER.info("Migrated VM '%s' to host '%s'" % (vm.get_name(), host.get_name()))
try:
	api = API(url="https://rhev-m.vsix.info:443",username="admin@internal", password="rhev",ca_file="ca.crt")
	print "Connected to %s successfully!" % api.get_product_info().name
	dc_list = api.datacenters.list()
	c_list = api.clusters.list()
	h_list = api.hosts.list()
	v_list = api.vms.list()
	vmm1=api.vms.get("vm1")
	print vmm1
	hvv1=api.hosts.get('rhevh-1.vsix.info') 
	print hvv1
        migrateVm(vmm1,hvv1)
#	for dc in dc_list:
#		print "%s (%s)" % (dc.get_name(), dc.get_id())
#	for c in c_list:
#		print "%s (%s)" % (c.get_name(), c.get_id())
#	for h in h_list:
#		print "%s (%s)" % (h.get_name(), h.get_id())
#	for v in v_list:
#		print "%s (%s)" % (v.get_name(), v.get_id())
	api.disconnect()

except Exception as ex:
	print "Unexpected error: %s" % ex
