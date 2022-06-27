#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import os
import fire

from logictest import is_empty_line
from http_connector import HttpConnector, format_result
from config import http_config

target_dir="./"

http_client = HttpConnector()
http_client.connect(**http_config)

def run(source_file, target_path="."):
    if not os.path.isfile(source_file):
        print("{} not a file".format(source_file))
        return
    print("Source file: {}".format(source_file))
    case_name = os.path.basename(source_file)
    print("Case name: {}".format(case_name))
    out = open("{}/{}".format(target_path,case_name),mode="w+", encoding='UTF-8')

    statement = list()
    f = open(source_file, encoding='UTF-8')
    for line in f.readlines():
        if line.startswith("--"):  # ignore comments
            continue
        if line.startswith("statement"):
            if len(statement) != 0: 
                sql = "".join(statement[1:])
                # print("Get a sql: {}".format(sql))
                try:
                    http_results = format_result(http_client.fetch_all(sql))
                    query_options = http_client.get_query_option()
                    if query_options:
                        statement[0] = statement[0].strip() + " " + query_options + "\n"
                except Exception as err:
                    print("Get query results error, with msg: {}".format(str(err)))

                # print(query_options)
                # print(http_results)

                if "query" not in statement[0]:
                    pass
                elif len(http_results) == 0:
                    out.write("-- auto generated, statement query get no results\n") # manual check
                    statement[0] = "statement query skipped\n"
                else:
                    statement.append("----\n")
                    statement.append(http_results)
                    statement.append("\n")
                out.writelines(statement)
            statement = list()

        if is_empty_line(line):
            if ";" not in statement[-1]:
                statement[-1] = statement[-1].strip() + ";\n"
        statement.append(line)

    out.flush()
    f.close()
    out.close()

if __name__ == '__main__':
    fire.Fire(run)