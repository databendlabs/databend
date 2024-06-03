#!/usr/bin/python3
import sys, getopt
import os
import re
import random
import time

CHAOS_FILE = "/tmp/chaos.yaml"

class ChaosParams:
  def __init__(self):
    self.params = {}
    self.template = ""
    self.template_map = {}

  def init(self, params):
    for k,v in params.items():
      self.params[k] = v

  def generate(self, node):
    content = self.template
    for template_key, key in self.template_map.items():
      content = content.replace(template_key, self.params[key])

    content = content.replace('${NODE}', node)
    return content

class IoDelayParams(ChaosParams):
  def __init__(self):
    ChaosParams.__init__(self)
    # default params
    self.params['delay'] = '500ms'
    self.params['percent'] = '100'
    self.template_map = {'${DELAY}':'delay','${PENCENT}':'percent'}
    self.template = open("./tests/metaverifier/templates/node-disk-io-delay-template.yaml", 'r').read()
    #print('template: ', self.template)
    #self.generate('hah')

class MetaChaos:
  def __init__(self, mode, namespace,internal,nodes,total):
    self.mode = mode
    self.namespace = namespace
    self.total = total
    self.internal = int(internal)
    self.split_nodes(nodes)

  def split_nodes(self, nodes):
    nodes = nodes.split(",")
    self.node_port_map = {}
    for node in nodes:
      ary = node.split(".")
      self.node_port_map[ary[0]] = node

  def get_node(self, get_leader):
    nodes = []
    cmd = "kubectl exec -i databend-metaverifier -n databend -- "
    for node, addr in self.node_port_map.items():
      curl_cmd = cmd + "curl " + addr + '/v1/cluster/status'
      content = os.popen(curl_cmd).read()
      print("curl cmd output:", content)
      if get_leader:
        if content.find('"state":"Leader"') != -1:
          nodes.append(node)
          return nodes
      else:
        if content.find('"state":"Leader"') == -1:
          nodes.append(node)

    if len(nodes) == 0:
      nodes = list(self.node_port_map.keys())
      print("cannot find nodes, get_leader: ", get_leader, nodes)
      return nodes
    return nodes

  def get_inject_chaos_node(self, node_mode):
    return random.sample(self.get_node(node_mode == "leader"), 1)[0]
    
  def exec_cat_meta_verifier(self):
      cmd = "kubectl exec -i databend-metaverifier -n databend -- cat /tmp/meta-verifier"
      content = os.popen(cmd).read()
      print("exec cat meta-verifier: ", content)

      return content

  def wait_verifier(self):
    # first start meta-verifier
    # make sure meta-verifier has been started
    count = 0
    while count < 10:
      content = self.exec_cat_meta_verifier()
      if content == "START":
        print('databend-metaverifier has started')
        return
      count += 1
      time.sleep(1)

    print('databend-metaverifier has not started, exit')
    sys.exit(-1)

  def is_verifier_end(self):
    content = self.exec_cat_meta_verifier()
    if content != "END" and content != "START":
      print("cat /tmp/meta-verifier return " + str(content) + ", exit")
      cmd = "kubectl get pods -n databend -o wide"
      content = os.popen(cmd).read()
      print("kubectl get pods -n databend -o wide:\n", content)
      sys.exit(-1)
    return content == "END"

  def run(self):
    self.wait_verifier()

    mode = self.mode.split("/")
    #print("mode: ", mode)
    typ,subtype = mode[0],mode[1]
    params_vec = mode[2].split(",")
    params = {}
    for param in params_vec:
      param = param.split("=")
      params[param[0]] = param[1]
    #print("params:", params)

    if typ == "io" and subtype == "delay":
      chaos_params = IoDelayParams()
      chaos_params.init(params)
      self.chaos_params = chaos_params
    
    start = int(time.time())
    node_modes = ["leader", "follower"]
    while True:
      # random select node mode, leader or follower
      node_mode = random.sample(node_modes, 1)[0]
      node = self.get_inject_chaos_node(node_mode)
      print("inject chaos rule to " + node_mode + " node: ", node)

      # generate chaos yaml
      chaos_yaml = self.chaos_params.generate(node)
      chaos_file = open(CHAOS_FILE, 'w')
      chaos_file.write(chaos_yaml)
      chaos_file.close()

      # apply chaos
      cmd = "kubectl apply -f " + CHAOS_FILE
      os.system(cmd)

      # wait some time
      time.sleep(self.internal)

      # delete chaos
      cmd = "kubectl delete -f " + CHAOS_FILE
      os.system(cmd)

      # wait some time until next loop
      time.sleep(3)

      current = int(time.time())
      diff = current - start
      if self.is_verifier_end():
        print("databend-metaverifier has completed, cost:" + diff + "s, exit")
        sys.exit(0)
      
      if diff > self.total:
        print('databend-metaverifier is not completed in total time, exit -1')
        sys.exit(-1)
      
# mode = type/subtype/mode params
# ex: mode = io/delay/leader/delay=300,percent=100
# nodes = node-pod-name:node-port[,node-pod-name:node-port]
# internal: apple chaos for how long(in second)
# namespace: databend meta k8s namespace
# total: test total time
def main(argv):
  try:
    opts, args = getopt.getopt(argv,"h",["internal=","mode=","namespace=","nodes=","total="])
  except getopt.GetoptError:
    print ('chaos-meta.py')
    sys.exit(-1)

  internal = 10
  mode = ""
  namespace = ""
  nodes = ""
  total = 600
  for opt, arg in opts:
    arg = arg.strip()
    if opt == '-h':
      print ('chaos-meta.py --prefix --client')
      sys.exit()
    elif opt in ("--internal"):
      if len(arg) > 0:
        internal = arg
    elif opt in ("--mode"):
      if len(arg) > 0:
        mode = arg
    elif opt in ("--namespace"):
      if len(arg) > 0:
        namespace = arg
    elif opt in ("--nodes"):
      if len(arg) > 0:
        nodes = arg
    elif opt in ("--total"):
      if len(arg) > 0:
        total = int(arg)

  if len(namespace) == "" or mode == "" or nodes == "":
    print("namespace or mode or nodes is empty")
    return

  chaos = MetaChaos(mode, namespace,internal,nodes,total)
  chaos.run()

if __name__ == "__main__":
  main(sys.argv[1:])