from cloudfiles import get_connection
from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys


class max_vms():
    '''returns only hosts with less running vms then the maximum'''

    #What are the values this module will accept, used to present
    #the user with options
    properties_validation = 'maximum_vm_count=[0-9]*'

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

    def do_filter(self, hosts_ids, vm_id, args_map):
        conn = self._get_connection()
        if conn is None:
            return

        #get our parameters from the map
        maximum_vm_count = int(args_map.get('maximum_vm_count', 100))
        engine_hosts = self._get_hosts(hosts_ids, conn)

        #iterate over them and decide which to accept
        accepted_host_ids = []
        for engine_host in engine_hosts:
            if(engine_host and
                    engine_host.summary.active < maximum_vm_count):
                accepted_host_ids.append(engine_host.id)
        print accepted_host_ids
