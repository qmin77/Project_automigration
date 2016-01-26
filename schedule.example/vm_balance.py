from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys


class vm_balance():
    '''moves a vm from a host with to many'''

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

    def do_balance(self, hosts_ids, args_map):
        conn = self._get_connection()
        if conn is None:
            return

        #get our parameters from the map
        maximum_vm_count = int(args_map.get('maximum_vm_count', 100))

        #get all the hosts with the given ids
        engine_hosts = self._get_hosts(hosts_ids, conn)

        #iterate over them and decide which to balance from
        over_loaded_host = None
        white_listed_hosts = []
        for engine_host in engine_hosts:
            if(engine_host):
                if (engine_host.summary.active < maximum_vm_count):
                    white_listed_hosts.append(engine_host.id)
                    continue
                if(not over_loaded_host or
                        over_loaded_host.summary.active
                        < engine_host.summary.active):
                    over_loaded_host = engine_host

        if(not over_loaded_host):
            return

        selected_vm = None
        #just pick the first we find
        host_vms = conn.vms.list('host=' + over_loaded_host.name)
        if host_vms:
            selected_vm = host_vms[0].id
        else:
            return

        print (selected_vm, white_listed_hosts)
