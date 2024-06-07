#!/usr/bin/python3
import sys, getopt
import os
import re
import random
import time
import logging

CHAOS_FILE = "/tmp/chaos.yaml"

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s.%(msecs)06d %(levelname)s %(message)s",
                    datefmt = '%Y-%m-%d,%H:%M:%S'
                    )

class ChaosParams:
  def __init__(self):
    self.params = {}
    self.template = ""
    self.template_map = {}

  def to_string(self):
    return ""

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

  def to_string(self):
    return "type: IoDelay, params delay: " + str(self.params['delay']) + ", percent:" + str(self.params['percent'])

class MetaChaos:
  def __init__(self, mode, namespace,nodes,total):
    self.mode = mode
    self.namespace = namespace
    self.total = total
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
      logging.debug("curl cmd output:" + str(content))
      if get_leader:
        if content.find('"state":"Leader"') != -1:
          nodes.append(node)
          return nodes
      else:
        if content.find('"state":"Leader"') == -1:
          nodes.append(node)

    if len(nodes) == 0:
      nodes = list(self.node_port_map.keys())
      logging.debug("cannot find nodes, get_leader: " + str(get_leader) + " " + str(nodes))
      return nodes
    return nodes

  def get_inject_chaos_node(self, node_mode):
    #return random.sample(self.get_node(node_mode == "leader"), 1)[0]
    return random.sample(list(self.node_port_map.keys()), 1)[0]
    
  def exec_cat_meta_verifier(self):
      cmd = "kubectl exec -i databend-metaverifier -n databend -- cat /tmp/meta-verifier"
      content = os.popen(cmd).read().strip()
      logging.debug("exec cat meta-verifier: " + str(content))

      return content

  def wait_verifier(self):
    # first start meta-verifier
    # make sure meta-verifier has been started
    count = 0
    while count < 10:
      content = self.exec_cat_meta_verifier()
      if content == "START":
        logging.debug('databend-metaverifier has started')
        return
      count += 1
      time.sleep(1)

    logging.error('databend-metaverifier has not started, exit')
    sys.exit(-1)

  def is_verifier_end(self):
    cmd = "kubectl logs databend-metaverifier -n databend | tail -10"
    content = os.popen(cmd).read()
    logging.debug("kubectl logs databend-metaverifier -n databend:\n" + str(content))
    content = self.exec_cat_meta_verifier()
    if content == "ERROR":
      logging.error("databend-metaverifier return error")
      sys.exit(-1)

    return content == "END"

  def run(self, apply_second, recover_second):
    logging.info("run with apply_second: " + str(apply_second) + ", recover_second" + str(recover_second))

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
    
    logging.info("run with chaos: " + self.chaos_params.to_string())
    self.wait_verifier()

    start = int(time.time())
    node_modes = ["leader", "follower"]
    count = 0 
    while True:
      count += 1

      # random select node mode, leader or follower
      node_mode = random.sample(node_modes, 1)[0]
      node = self.get_inject_chaos_node(node_mode)
      logging.debug("loop " + str(count) + " inject chaos rule to  node: " + str(node))

      # generate chaos yaml
      chaos_yaml = self.chaos_params.generate(node)
      chaos_file = open(CHAOS_FILE, 'w')
      chaos_file.write(chaos_yaml)
      chaos_file.close()

      # apply chaos
      cmd = "kubectl apply -f " + CHAOS_FILE
      os.system(cmd)
      logging.debug("apply chaos..")

      # wait some time
      time.sleep(apply_second)

      # delete chaos
      cmd = "kubectl delete -f " + CHAOS_FILE
      os.system(cmd)
      logging.debug("remove chaos..")

      # wait some time
      time.sleep(recover_second)

      current = int(time.time())
      diff = current - start
      if self.is_verifier_end():
        logging.debug("databend-metaverifier has completed, cost:" + str(diff) + "s, exit")
        sys.exit(0)
      
      if diff > self.total:
        logging.error('databend-metaverifier is not completed in total time, exit -1')
        sys.exit(-1)
      
# mode = type/subtype/mode params
# ex: mode = io/delay/delay=300,percent=100
# nodes = node-pod-name:node-port[,node-pod-name:node-port]
# namespace: databend meta k8s namespace
# total: test total time
# apply_second: apply chaos second
# recover_second: recover from chaos second
def main(argv):
  try:
    opts, args = getopt.getopt(argv,"h",["mode=","namespace=","nodes=","total=","apply_second=", "recover_second="])
  except getopt.GetoptError:
    print ('chaos-meta.py')
    sys.exit(-1)

  mode = ""
  namespace = ""
  nodes = ""
  total = 600
  apply_second = 5
  recover_second = 10
  for opt, arg in opts:
    arg = arg.strip()
    if opt == '-h':
      print ('chaos-meta.py --prefix --client')
      sys.exit()
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
    elif opt in ("--apply_second"):
      if len(arg) > 0:
        apply_second = int(arg)
    elif opt in ("--recover_second"):
      if len(arg) > 0:
        recover_second = int(arg)

  if len(namespace) == "" or mode == "" or nodes == "":
    print("namespace or mode or nodes is empty")
    return

  chaos = MetaChaos(mode, namespace,nodes,total)
  chaos.run(apply_second, recover_second)

if __name__ == "__main__":
  main(sys.argv[1:])