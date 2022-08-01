#!/usr/bin/env python3
# -*- coding: UTF-8 -*-


class LogicTestStatistics():

    def __init__(self) -> None:
        self._failed_map = dict()
        self._perf_map = dict()

    def add_failed(self, runner, suite_name, exception_obj):
        if runner not in self._failed_map:
            self._failed_map[runner] = dict()
        if suite_name not in self._failed_map[runner]:
            self._failed_map[runner][suite_name] = list()
        self._failed_map[runner][suite_name].append(exception_obj)

    def add_perf(self, runner, suite_name, statement, time_cost):
        if runner not in self._perf_map:
            self._perf_map[runner] = dict()
        if suite_name not in self._perf_map[runner]:
            self._perf_map[runner][suite_name] = list()
        self._perf_map[runner][suite_name].append({
            "statement": statement,
            "time_cost": time_cost,
        })

    def __str__(self):
        failure_output = f"Following failed statements:\n"
        self.total_failed = 0
        for runner in self._failed_map:
            for suite in self._failed_map[runner]:
                for err in self._failed_map[runner][suite]:
                    self.total_failed += 1
                    failure_output += f"---------------------------------------------\n{str(err)}\n"

        if self.total_failed == 0:
            failure_output = "All tests pass! Logic test success!\n"

        summary_output = f"Logic Test Summary\n"
        runner_list = list()
        for runner in self._perf_map:
            runner_list.append(runner)
            statement_cost = 0
            statement_count = 0
            suite_count = 0
            suite_cost = 0
            for suite in self._perf_map[runner]:
                suite_count += 1
                for result in self._perf_map[runner][suite]:
                    statement_count += 1
                    statement_cost = statement_cost + result['time_cost']
                    suite_cost = suite_cost + result['time_cost']
            summary_output += f"Runner {runner} test {suite_count} suites, avg time cost of suites is {round(suite_cost/suite_count*1000, 2)} ms\n"
            summary_output += f"Runner {runner} test {statement_count} statements, avg time cost of statements is {round(statement_cost/statement_count*1000,2)} ms\n"

        return f"{summary_output}\n{failure_output}"

    def check_and_exit(self):
        print(self.__str__())
        if self.total_failed != 0:
            exit(2)
        else:
            exit(0)


global_statistics = LogicTestStatistics()
