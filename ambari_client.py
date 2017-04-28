
'''
Author: Daniel O'Keeffe
Company: Bluemetrix
Company Website: https://www.bluemetrix.com/

Description:
A class containing some simple functions that simplify the interaction
between you and the Ambari API.

TODO:

1. Add curl -u admin:admin -H "X-Requested-By: ambari" \
     http://dok31.northeurope.cloudapp.azure.com:8080/api/v1/clusters/dokcl3/alert_definitions/
    Add method to add alert definition

2. Add `def query([get|post|delete]):`

3. Add ConfigParser (py2.6+) / configparser (py3.+) functionality to the programme
    # Import ConfigParser
    # from configparser import ConfigParser

4. Add a proper command-line option parser like argparse
    # import argparse

5. Consider separating the more basic functionality from the complex stuff
    Two objects or a sub object

6. Simplify a lot of the functions by using decorators to improve the syntax

7. You should replace requests with urllib or urllib2

8. Add try/except/else/finally clauses to the script to handle errors,
    and provide the user with stack traces of the error.

'''
import sys
import time
import random
import json
import itertools

import requests


# Handle python 2 and python 3 versions
if sys.version_info.major == 2:
    input = raw_input


class AmbariClient(object):
    """ A class containing some simple functions that simplify the interaction
        between you and the Ambari API.

        Attributes:
            namenode: The namenode name in a string.
            port: An integer with the port to ambari listens on .
            cluster_name: The name of the cluster as a sting.
            auth: A tuple containing two strings, the username and password.
            hdrs: A dictionary containing the http headers.
            endpoint: The base url that requests are submitted to.
            services: A list of services on the hadoop cluster
            components: A list of components on the hadoop cluster
    """

    def __init__(self, namenode, port, cluster_name, auth=None, headers=None):
        super(AmbariClient, self).__init__()
        self.namenode = namenode
        self.port = port
        self.cluster_name = cluster_name
        self.auth = auth
        self.hdrs = headers
        self.endpoint = "http://{}:{}/api/v1/clusters/{}/".format(self.namenode,
                                                                  self.port,
                                                                  self.cluster_name)
        self.services = self.get_services()
        self.components = self.get_components()

    def get_services(self):
        """Return a list of available services."""
        url = self.endpoint + "services/"
        response = requests.get(url, headers=self.hdrs, auth=self.auth)
        services = [i["ServiceInfo"]["service_name"] for i in response.json()["items"]]

        return(services)

    def get_components(self, service=""):
        """Return a list of available components."""
        if service is not "":
            # Security Check
            self._has_service(service)

        url = self.endpoint + "components/"

        response = requests.get(url, headers=self.hdrs, auth=self.auth)
        # If a service is specified, filter for relevant components
        if service:
            components = []
            for i in response.json()["items"]:
                if i["ServiceComponentInfo"]["service_name"] == service:
                    components.append(i["ServiceComponentInfo"]["component_name"])
            return(components)

        components = [i["ServiceComponentInfo"]["component_name"] for i in response.json()["items"]]
        return(components)

    def query(self, rtype):
        pass

    def get(self, url):
        response = requests.get(url, auth=self.auth, headers=self.hdrs)
        return(response)

    def put(self, url, payload):
        payload = json.dumps(payload) if isinstance(payload, dict) else payload
        response = requests.put(url, auth=self.auth, headers=self.hdrs, data=payload)
        return(response)

    def post(self, url, payload=None):
        payload = json.dumps(payload) if isinstance(payload, dict) else payload
        response = requests.post(url, auth=self.auth, headers=self.hdrs, data=payload)
        return(response)

    def delete(self, url):
        response = requests.delete(url, auth=self.auth, headers=self.hdrs)
        return(response)

    def update_components(self):
        """Update the list of currently installed components."""
        self.components = self.get_components()

    def update_services(self):
        """Update the list of currently installed services."""
        self.services = self.get_services()

    def delete_component(self, component, host):
        url = self.endpoint + "hosts/{}/host_components/{}".format(host, component)
        res = self.delete(url)
        return(res)

    def add_component(self, component, host):
        url = self.endpoint + "hosts/{}/host_components/{}".format(host, component)
        res = self.post(url)
        return(res)

    def delete_service(self, service):
        pass

    def change_component_state(self, component, host, new_state):
        url = self.endpoint + "hosts/{}/host_components/{}".format(host, component)
        data = {"RequestInfo": {"context": "Change {} state".format(component)},
                "HostRoles": {"state": new_state}}
        res = self.put(url, data)
        return(res)

    def start_component(self, component, host):
        res = self.change_component_state(component, host, "STARTED")
        return(res)

    def stop_component(self, component, host):
        res = self.change_component_state(component, host, "INSTALLED")
        return(res)

    def component_on_host(self, component_name, host):
        """
        NOTE: Temporary method
        Check is the component `component_name` on a given host.

        Parameters
        ----------
        component_name : str
            The name of the component to be moved
            (Examples: WEBHCAT_SERVER, NFS_GATEWAY).
        host : str
            The hostname or ip address of the Host

        Returns
        -------
        on_host : bool
            bool indicating if the component is on the given host
        """
        on_host = True
        url = self.endpoint + "hosts/{}/host_components/".format(host)
        res = self.get(url)
        host_components = [i["HostRoles"]["component_name"] for i in res.json()["items"]]
        on_host = component_name in host_components
        return(on_host)

    def move_component(self, component_name, old_host, new_host):
        """
        Given a Hadoop Component, `component_name`, move that component from `old_host` to
        `new_host`.

        Parameters
        ----------
        component_name : str
            The name of the component to be moved
            (Examples: WEBHCAT_SERVER, NFS_GATEWAY).
        old_host : str
            Hostname or ip address of the current host the component is installed on.
        new_host : str
            Hostname or ip address of the new host the component is to be installed on.

        Returns
        -------
        status : bool,
            A value representing success (True) or failure (False) of the move.
        """
        # Check is the component on the cluster
        if component_name not in self.components:
            msg = "Component {} not found.\n"
            msg += "Please run the `update_components method` or check the spelling."
            raise(ValueError(msg.format(component_name)))
        # Check is the component on the old_host
        if not self.component_on_host(component_name, old_host):
            msg = "Component {} not found on host {}"
            raise(ValueError(msg.format(component_name, old_host)))
        input("Process completed?")

        # Stop the service before deleting the component
        res = self.stop_component(component_name, old_host)
        if not res.ok:
            print("Failed to stop component")
            return(False)
        input("Process completed?")

        #  Add the component
        install = self.add_component(component_name, new_host)
        input("Finished Adding component?")
        state_change = self.change_component_state(component_name, new_host, "INSTALLED")
        input("Process completed?")
        #  Delete the component
        delete = self.delete_component(component_name, old_host)
        input("Finished Deleting component?")
        state_change = self.start_component(component_name, new_host)

        return(delete.ok and install.ok)

    def _has_component(self, component):
        """
        Checks `component` is in self.components,
        if not found raises a ValueError
        """
        if component not in self.components:
            error_msg = "{} is not found in components.".format(component)
            raise(ValueError(error_msg))

    def _has_service(self, service):
        """
        Checks `service` is in self.services,
        if not found raises a ValueError
        """
        if service not in self.services:
            error_msg = "{} is not found in services.".format(service)
            raise(ValueError(error_msg))

    def get_service_info(self, service):
        """Return the all the info for a service"""
        # Security Check
        self._has_service(service)

        url = self.endpoint + "services/{}".format(service)
        response = requests.get(url, headers=self.hdrs, auth=self.auth)

        return(response.json())

    def get_service_state(self, service):
        """Return the current state of a service as a string."""
        state = self.get_service_info(service)["ServiceInfo"]["state"]

        return(state)

    def get_services_states(self):
        """
        Return a list of states for all the services installed on the cluster
        """
        return([self.get_service_state(s) for s in self.services])

    def get_component_info(self, component):
        """Return the all the info for a component"""
        # Security Check
        self._has_component(component)

        url = self.endpoint + "components/{}".format(component)
        response = requests.get(url, headers=self.hdrs, auth=self.auth)

        return(response.json())

    def get_component_state(self, component):
        """Return the current state of a component as a string."""
        state = self.get_component_info(component)["ServiceComponentInfo"]["state"]

        return(state)

    def stop_service(self, service):
        """
        Stop a service. The service name must be capital letters.
        Use get_Services if in doubt.
        """
        # Security Check
        self._has_service(service)

        payload = json.dumps({"RequestInfo": {
                              "context": "Stopping {}".format(service)},
                              "Body": {"ServiceInfo": {"state": "INSTALLED"}}})
        url = self.endpoint + "services/{}".format(service)
        response = requests.put(url,
                                data=payload,
                                headers=self.hdrs,
                                auth=self.auth)

        print("Stopping {}...".format(service))

        return(response)

    def start_service(self, service):
        """
        Start a service. The service name must be capital letters.
        Use get_Services if in doubt.
        """
        # Security Check
        self._has_service(service)

        payload = json.dumps({"RequestInfo": {
                              "context": "Starting {}".format(service)},
                              "Body": {"ServiceInfo": {"state": "STARTED"}}})
        url = self.endpoint + "services/{}".format(service)
        response = requests.put(url,
                                data=payload,
                                headers=self.hdrs,
                                auth=self.auth)

        print("Starting {}...".format(service))
        return(response)

    def stop_all_services(self):
        """Stops all services not currently in the INSTALLED state"""
        msg = "Stopping all services"
        payload = json.dumps({"RequestInfo": {"context": msg},
                              "Body": {"ServiceInfo": {"state": "INSTALLED"}}})
        url = self.endpoint + "services/"

        response = requests.put(url,
                                auth=self.auth,
                                headers=self.hdrs,
                                data=payload)
        print(msg)
        return(response)

    def start_all_services(self):
        """Starts all services not currently in the STARTED state"""
        msg = "Starting all services"
        payload = json.dumps({"RequestInfo": {"context": msg},
                              "Body": {"ServiceInfo": {"state": "STARTED"}}})
        url = self.endpoint + "services/"

        response = requests.put(url,
                                auth=self.auth,
                                headers=self.hdrs,
                                data=payload)
        print(msg)
        return(response)

    def restart_all_services(self):
        """
        A function to stop all the services,
        and once they are all stopped it will
        start them again.
        """
        res = self.stop_all_services()

        states = self.get_services_states()

        while not all([state == "INSTALLED" for state in states]):
            time.sleep(10)
            states = self.get_services_states()

        time.sleep(120)  # Wait two minutes
        res = self.start_all_services()
        print("Services are now being started. Please be patient...")
        return(res)

    @staticmethod
    def make_conf_note(**kwargs):

        template = ", ".join(["{} -> {}"] * len(kwargs))
        vals = list(itertools.chain(*[[k, kwargs[k]] for k in kwargs]))
        note = template.format(*vals)
        return(note)

    def modify_configurations(self, conf_name, **kwargs):
        """
        Returns the configurations of a given congfiguration group name,
        e.g. hdfs-site, zoo.cfg.
        This function can be use to set parameters by passing them as kwargs.

        Parameters
        ----------
        conf_name : string,
            A configuration group name (hdfs-site, zoo.cfg).

        Returns
        -------
        conf : dict,
            A dictionary with all the configurations and values.
        """
        tag = self.get_current_tag(conf_name)
        conf = self.get_configurations(conf_name, tag)

        properties = conf["properties"]
        for k in kwargs:
            if k in properties:
                properties[k] = kwargs[k]
            else:
                print("WARNING: Key, {}, not found in {}. Skipping.".format(k, conf_name))

        return(properties)

    def get_current_tag(self, conf_name):
        """
        Return the tag for current configurations of `conf_name`.

        Parameters
        ----------
        conf_name : string
            The configurations group to get the tag of from ambari.
            Example, hive-site, zoo.cfg.

        Returns
        -------
        tag : string
            The tag for the current configurations version.
        """
        payload = {"fields": "Clusters/desired_configs/{}".format(conf_name)}

        response = requests.get(self.endpoint,
                                auth=self.auth,
                                headers=self.hdrs,
                                params=payload)
        tag = response.json()["Clusters"]["desired_configs"][conf_name]["tag"]
        return(tag)

    def get_configurations(self, conf_name, tag):
        """
        Get the `conf_name` configurations identified by tag.

        Parameters
        ----------
        conf_name : string
            The configurations group to get the tag of from ambari.
            Example, hive-site, zoo.cfg.
        tag : string
            The tag for the current configurations version.

        Returns
        -------
        confs : dict
            A json object with the configurations for `conf_name` `tag`.
        """
        payload = {"type": conf_name, "tag": tag}
        response = requests.get(self.endpoint + "configurations",
                                auth=self.auth,
                                headers=self.hdrs,
                                params=payload)
        confs = response.json()["items"][0]
        return(confs)

    def put_new_conf(self, conf_name, **kwargs):
        """
        Make a put request to the ambari server
        to update a configuration group with new values.
        """
        config_note = self.make_conf_note(**kwargs)
        curr_time = int(time.time())

        payload = json.dumps([{"Clusters": {
            "desired_config": [{
                "tag": "version{}".format(curr_time),
                "type": conf_name,
                "properties": self.modify_configurations(conf_name, **kwargs),
                "service_config_version_note": config_note}]}}])

        # print(payload)
        # response = None
        response = requests.put(self.endpoint,
                                headers=self.hdrs,
                                auth=self.auth,
                                data=payload)

        return(config_note, response)

    def put_hdfs_site(self):
        """Change the replication factor of hdfs at random"""
        conf_group = "hdfs-site"
        replication_vals = [1, 2, 3]
        replication = random.choice(replication_vals)

        to_change = {"dfs.replication": replication}

        note, response = self.put_new_conf(conf_group, **to_change)
        print(note)

        return(note, response)

    def put_hive_site(self):
        """Change the hive-site configurations"""
        conf_group = "hive-site"
        vals = ["true", "false"]
        hvee, hvere = random.choice(vals), random.choice(vals)
        to_change = {"hive.vectorized.execution.enabled": hvee,
                     "hive.vectorized.execution.reduce.enabled": hvere}

        note, response = self.put_new_conf(conf_group, **to_change)
        print(note)

        return(note, response)

    def put_yarn_site(self):
        """Change the yarn-site configurations"""
        conf_group = "yarn-site"
        memory_vals = [("2048", "682"),
                       ("2048", "1024"),
                       ("2816", "1280"),
                       ("2816", "1664")]
        max_memory, min_memory = random.choice(memory_vals)
        to_change = {"yarn.nodemanager.resource.memory-mb": max_memory,
                     "yarn.scheduler.minimum-allocation-mb": min_memory}

        note, response = self.put_new_conf(conf_group, **to_change)
        print(note)

        return(note, response)

    def get_blueprint(self):
        """Return the Hadoop Blueprint as a json object."""
        payload = {"format": "blueprint"}
        response = requests.get(self.endpoint,
                                auth=self.auth,
                                headers=self.hdrs,
                                params=payload)

        return(response.json())

    def get_jmx(self, host=None, port=8080, params=None):
        """Query Ambari Metrics"""
        host = host if host else self.namenode
        payload = {"qry": params} if params else None
        url = "http://{}:{}/jmx".format(host, port)

        response = requests.get(url,
                                auth=self.auth,
                                headers=self.hdrs,
                                params=payload)

        return(response)

    def get_live_nodes(self):
        """
        Get the list of live Nodes from the cluster.
        Requires port 50070 to be open.
        """
        payload = "Hadoop:service=NameNode,name=NameNodeInfo"
        response = self.get_jmx(port=50070, params=payload)

        live_nodes = response.json()["beans"][0]["LiveNodes"]
        return([nodename.rstrip(":50010") for nodename in json.loads(live_nodes)])


def get_components_states(client, service):
    """
    List the states of each component for a given service.
    List contains tuples of component name, and state.
    """
    states = []
    for c in client.get_components(service):
        states.append((c, client.get_component_state(c)))
    return(states)


if __name__ == '__main__':
    #  Insert your ambari server hostname here
    # nnode = <hostname>
    nnode = "dok31.northeurope.cloudapp.azure.com"
    # Add port number for ambari
    # p = 8080  # 8080 is default
    p = 8080
    # Add the name of the cluster (assigned during installation)
    clr_name = "dokcl3"
    # Provide the password
    cred = ("admin", input("Ambari password: "))
    # HEader information required by ambari
    hdrs = {"X-Requested-By": "ambari"}

    try:
        filename = sys.argv[1]
    except IndexError as e:
        raise(ValueError("Usage: python ambari_client.py <filename>"))
        sys.exit(1)

    amc = AmbariClient(nnode, p, clr_name, cred, hdrs)

    with open(filename, "a") as f:
        # Change the configurations and restart the services
        note, _ = amc.put_hive_site()
        f.write(note + "\n")

        # Changing a configuration in HDFS has the knock on affect of
        # requiring a restart of YARN and MAPREDUCE2
        note, _ = amc.put_yarn_site()
        f.write(note + "\n")

        note, _ = amc.put_hdfs_site()
        f.write(note + "\n")

    amc.restart_all_services()
