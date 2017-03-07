from ambari_client import AmbariClient
import getpass

nnode = "dok31.northeurope.cloudapp.azure.com"
p = 8080
clr_name = "dokcl3"
cred = ("admin", getpass.getpass("Ambari password: "))
hdrs = {"X-Requested-By": "ambari"}

amc = AmbariClient(nnode, p, clr_name, cred, hdrs)
