#!/usr/bin/python3

import sys, getopt
import os
import re

def main(argv):
  inputfile = ''
  outputfile = ''
  try:
    opts, args = getopt.getopt(argv,"h",["path=","prefix=","client=","remove-percent=","number=","grpc-api-address=","namespace="])
  except getopt.GetoptError:
    print ('start_from_k8s.py -i <inputfile> -o <outputfile>')
    sys.exit(2)
  
  prefix = ''
  client = '10'
  time = '600'
  remove_percent = '10'
  number = '10000'
  grpc_api_address = ''
  binary = ""
  start_cmd = []
  for opt, arg in opts:
    arg = arg.strip()
    if opt == '-h':
      print ('test.py --prefix --client')
      sys.exit()
    elif opt in ("--path"):
      if len(arg) > 0:
        binary = arg
    elif opt in ("--prefix"):
      if len(arg) > 0:
        start_cmd.append("--prefix")
        start_cmd.append(arg)
    elif opt in ("--client"):
      if len(arg) > 0:
        start_cmd.append("--client")
        start_cmd.append(arg)
    elif opt in ("--time"):
      if len(arg) > 0:
        start_cmd.append("--time")
        start_cmd.append(arg)
    elif opt in ("--remove-percent"):
      if len(arg) > 0:
        start_cmd.append("--remove-percent")
        start_cmd.append(arg)
    elif opt in ("--number"):
      if len(arg) > 0:
        start_cmd.append("--number")
        start_cmd.append(arg)
    elif opt in ("--grpc-api-address"):
      if len(arg) > 0:
        start_cmd.append("--grpc-api-address")
        start_cmd.append(arg)
    elif opt in ("--namespace"):
      namespace = arg
      port = ":9191,"
      cmd = "kubectl get pods -o wide --namespace " + namespace + "| grep -v NAME | grep Running"
      output = os.popen(cmd).read()
      result = re.findall(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", output)
      if len(result) > 0:
        grpc_api_address = port.join(result) + ":9191"
        start_cmd.append("--grpc-api-address")
        start_cmd.append(grpc_api_address)

  if len(binary) == 0:
    print("binary path is empty, use --path option")
    return
  start_cmd.insert(0, binary)
  cmd = ' '.join(start_cmd)

  print("cmd: ", cmd)
  os.system(cmd)

if __name__ == "__main__":
  main(sys.argv[1:])
