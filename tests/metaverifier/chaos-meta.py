#!/usr/bin/python3
import sys, getopt
import os
import re

DEFAULT_PORT = ":9191,"

class MetaChaos:
  def __init__(self, mode, namespace):
    self.mode = mode
    self.namespace = namespace
    self.get_node_list()
    print("node: ", self.node_list)

  def get_node_list(self):
    self.node_list = []
    namespace = self.namespace
    cmd = "kubectl get pods -o wide --namespace " + namespace + "| grep -v NAME | grep Running"
    output = os.popen(cmd).read()
    result = re.findall(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", output)
    self.node_list = result

  def get_current_leader(self):
    pass
    
  def run():
    if self.mode == "io-delay":
      pass


def main(argv):
  try:
    opts, args = getopt.getopt(argv,"h",["sleep=","mode=","namespace="])
  except getopt.GetoptError:
    print ('chaos-meta.py')
    sys.exit(2)

  sleep = 10
  mode = ""
  namespace = ""
  for opt, arg in opts:
    arg = arg.strip()
    if opt == '-h':
      print ('chaos-meta.py --prefix --client')
      sys.exit()
    elif opt in ("--sleep"):
      if len(arg) > 0:
        sleep = arg
    elif opt in ("--mode"):
      if len(arg) > 0:
        mode = arg
    elif opt in ("--namespace"):
      if len(arg) > 0:
        namespace = arg

  if len(namespace) == "" or mode == "":
    print("namespace or mode is empty")
    return

  chaos = MetaChaos(mode, namespace)

if __name__ == "__main__":
  main(sys.argv[1:])