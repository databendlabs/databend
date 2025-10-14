#!/usr/bin/env python3
# coding: utf-8

import socket
import argparse
import time
import sys


def tcp_ping(port, timeout):
    now = time.time()

    while time.time() - now < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(("0.0.0.0", port))
                print("OK :{} is listening".format(port))
                sys.stdout.flush()
                return
        except Exception:
            print("... connecting to :{}".format(port))
            sys.stdout.flush()
            time.sleep(1)

    raise Exception("fail to connect to :{}".format(port))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="block until successfully connecting to a local tcp port"
    )
    parser.add_argument("-p", "--port", type=int, help="local tcp port")
    parser.add_argument("-t", "--timeout", type=int, default=10, help="time to wait.")

    args = parser.parse_args()

    tcp_ping(args.port, args.timeout)
