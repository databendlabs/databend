import os
import sys
import time
import uexpect

CURDIR = os.path.dirname(os.path.realpath(__file__))

sys.path.insert(0, os.path.join(CURDIR))


prompt = r":\) "
end_of_block = r".*\r\n.*\r\n"


class NativeClient(object):
    def __init__(self, command=None, name="", log=None):
        self.client = uexpect.spawn(["/bin/bash", "--noediting"])
        if command is None:
            command = "mysql --user default -N -s"
            tcp_host = os.getenv("QUERY_MYSQL_HANDLER_HOST")
            if tcp_host is not None:
                command += f" --host={tcp_host}"
            else:
                command += " --host=127.0.0.1"
            command += f' --prompt "{prompt}"'

            tcp_port = os.getenv("QUERY_MYSQL_HANDLER_PORT")
            if tcp_port is not None:
                command += f" --port={tcp_port}"
            else:
                command += " --port=3307"

        self.client.command = command
        self.client.eol("\r")
        self.client.logger(log, prefix=name)
        self.client.timeout(120)
        self.client.expect(r"[#\$] ", timeout=60)
        self.client.send(command)

    def __enter__(self):
        return self.client.__enter__()

    def __exit__(self, type, value, traceback):
        self.client.reader["kill_event"].set()
        # send Ctrl-C
        self.client.send("\x03", eol="")
        time.sleep(0.3)
        self.client.send("quit", eol="\r")
        self.client.send("\x03", eol="")
        return self.client.__exit__(type, value, traceback)


if __name__ == "__main__":
    with NativeClient(name="client1>") as client1:
        client1.expect(prompt)

        client1.send("SET max_threads = 1;")
        client1.expect("")
        client1.send("SELECT 1 + 3;")
        client1.expect("4")
