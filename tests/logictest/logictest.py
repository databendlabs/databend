import abc
import collections
import glob
import os
import re
import time

import six

from log import log
from statistics import global_statistics
from regex_type import compare_result_with_reg

supports_labels = ['http', 'mysql', 'clickhouse']

# statement is a statement in sql logic test
state_regex = r"^\s*statement\s+(?P<statement>((?P<ok>OK)|((?P<error>)ERROR\s*(?P<expectError>.*))|(?P<query>QUERY\s*((" \
              r"ERROR\s+(?P<queryError>.*))|(?P<queryOptions>.*)))))$"

result_regex = r"^----\s*(?P<label>.*)?$"
condition_regex = r"^(skipif\s+(?P<skipDatabase>.*))|(onlyif\s+(?P<onlyDatabase>.*))$"


# return the statement type
# `None` represent that the current format is not a statement type
def get_statement_type(line):
    return re.match(state_regex, line, re.MULTILINE | re.IGNORECASE)


def get_result_label(line):
    return re.match(result_regex, line, re.MULTILINE | re.IGNORECASE)


def get_statement_condition(line):
    return re.match(condition_regex, line, re.IGNORECASE)


# return false if the line is not empty
def is_empty_line(line):
    if line.split():
        return False
    return True


# iterate lines over a file and return a iterator
def get_lines(suite_path):
    with open(suite_path, encoding="UTF-8") as reader:
        for line_idx, line in enumerate(reader.readlines()):
            yield line_idx, line.rstrip('\n ')  # keep tab /t


# return a single statement
def get_single_statement(lines):
    statement_lines = []
    for line_idx, line in lines:
        if is_empty_line(line):
            statement = "\n".join(statement_lines)
            return statement
        statement_lines.append(line)
    return "\n".join(statement_lines)


def get_result(lines):
    result_lines = []
    result_label = None
    val = 0
    for line_idx, line in lines:
        val = line_idx
        if line.startswith('\t'):  # tab as empty row in results
            result_lines.append(line)
            continue
        if is_empty_line(line) and result_label is None:
            continue
        if is_empty_line(line) and result_label is not None:
            return result_label, line_idx, "\n".join(result_lines)
        if result_label is not None:
            result_lines.append(line)
        if result_label is None:
            result_label = get_result_label(line)

    if result_label is not None:
        return result_label, val, "\n".join(result_lines)


def parse_token_args(tokens, arg):
    i = 0
    while i < len(tokens):
        if tokens[i].startswith(
                "{}(".format(arg)) and tokens[i].endswith(")") is False:
            tokens[i] = tokens[i] + "," + tokens[i + 1]
            del tokens[i + 1]
            i -= 1
        i += 1


class LogicError(Exception):

    def __init__(self, message, errorType, runner):
        self.message = message
        self.errorType = errorType
        self.runner = runner

    def __str__(self):
        return f"Runner: {self.runner}\nErrorType: {self.errorType}\nMessage: {self.message}"


class Statement:

    def __init__(self, matched):
        assert matched is not None
        self.matched = matched
        self.label = None
        self.retry = False
        self.query_type = None
        self.expect_error = None
        if matched.group("ok") is not None:
            self.type = "ok"
        elif matched.group("error") is not None:
            self.type = "error"
            self.expect_error = matched.group("expectError")
        elif matched.group("query"):
            self.type = "query"
            if matched.group("queryError"):
                self.query_error = matched.group("queryError")
            else:
                qo = matched.group("queryOptions")
                s = qo.split(" ", 1)
                if len(s) < 1:
                    raise Exception(f"Invalid query options: {qo}")
                if len(s) == 1:
                    if is_empty_line(s[0]):
                        raise Exception(
                            f"Invalid query options, query type should not be empty: {qo}"
                        )
                    self.query_type = s[0]
                    return
                query_type, options = qo.split(" ", 1)

                tokens = options.split(",")
                tokens = [t.strip() for t in tokens]
                parse_token_args(tokens, "label")
                self.query_type = query_type
                for token in tokens:
                    if token.startswith("label(") and token.endswith(")"):
                        trimed = token[len("label("):-1]
                        self.label = trimed.split(",")
                    if token == "retry":
                        self.retry = True

        else:
            raise Exception(f"Unknown statement type {matched.group()}")

    def __str__(self):
        s = f"Statement: {self.type}, type: {self.query_type}"
        if self.type == "query":
            if self.query_type is not None:
                s += f", query_type: {self.query_type}"
                if self.label is not None:
                    s += f", label: {self.label}"
                s += f", retry: {self.retry}"

        return s


class ParsedStatement(
        collections.namedtuple(
            'ParsedStatement',
            ["at_line", "s_type", "suite_name", "text", "results", "runs_on"])):

    def get_fields(self):
        return self._fields

    def __str__(self):
        result = ["", "Parsed Statement"]
        for field in self.get_fields():
            value = str(getattr(self, field))
            if field != 'text':
                result.append(' ' * 4 + '%s: %s,' % (field, value))
            else:
                result.append(' ' * 4 + '%s:' % field)
                result.extend([' ' * 8 + row for row in value.split('\n')])
        return "\n".join(result)


# return all statements in a file
def get_statements(suite_path, suite_name):
    condition_matched = None
    lines = get_lines(suite_path)
    for line_idx, line in lines:
        if is_empty_line(line):
            # empty line or junk lines
            continue

        statement_type = get_statement_type(line)

        if statement_type is None:
            if condition_matched is None:
                condition_matched = get_statement_condition(line)
            continue

        s = Statement(statement_type)
        text = get_single_statement(lines)
        results = []
        runs_on = set(supports_labels)
        if condition_matched is not None:
            if condition_matched.group("skipDatabase") is not None:
                runs_on.remove(condition_matched.group("skipDatabase"))
            if condition_matched.group("onlyDatabase") is not None:
                runs_on = {
                    x for x in runs_on
                    if x == condition_matched.group("onlyDatabase")
                }
            condition_matched = None
        log.debug("runs_on: {}".format(runs_on))
        if s.type == "query" and s.query_type is not None:
            # Here we use labels to figure out number of results
            # Must have a default results (without any label)
            # One more label means an addion results
            result_count = 1
            if s.label is not None:
                result_count = len(s.label) + 1
            for i in range(result_count):
                results.append(get_result(lines))
        yield ParsedStatement(line_idx + 1, s, suite_name, text, results,
                              runs_on)


def format_value(vals, val_num):
    row = len(vals) // val_num
    table = ""
    for i in range(row):
        ans = []
        for j in range(val_num):
            ans.append(str(vals[i * val_num + j]))
        table += "  ".join(ans)
        table += "\n"
    return table


def safe_execute(method, *info):
    try:
        return method()
    except Exception as e:
        collected = "\n".join([str(e)] + [str(el) for el in info])
        raise RuntimeError("Failed to execute. Collected info: %s" % collected)


# factory class to abstract runtime interface
@six.add_metaclass(abc.ABCMeta)
class SuiteRunner(object):

    def __init__(self, kind, args):
        self.label = None
        self.retry_time = 3
        self.driver = None
        self.statement_files = []
        self.kind = kind
        self.show_query_on_execution = False
        self.on_error_return = False
        self.dir = dir
        self.args = args

    # return all files under the path
    # format: a list of file absolute path and name(relative path)
    def fetch_files(self):
        log.debug(f"Skip test file list {self.args.skip}")
        skips = self.args.skip

        if type(skips) is str:
            skips = skips.split(",")

        suite_path = self.args.suites

        for filename in glob.iglob(f'{suite_path}/**', recursive=True):
            if os.path.isfile(filename):
                base_name = os.path.basename(filename)
                dirs = os.path.dirname(filename).split(os.sep)

                if self.args.skip_dir and any(
                        s in dirs for s in self.args.skip_dir):
                    log.info(
                        f"Skip test file {filename}, in dirs {self.args.skip_dir}"
                    )
                    continue

                if self.args.skip and any(
                    [re.search(r, base_name) for r in skips]):
                    log.info(f"Skip test file {filename}")
                    continue

                if self.args.run_dir and not any(s in dirs
                                                 for s in self.args.run_dir):
                    log.info(
                        f"Skip test file {filename}, not in run dir {self.args.run_dir}"
                    )
                    continue

                if not self.args.pattern or any(
                    [re.search(r, base_name) for r in self.args.pattern]):
                    self.statement_files.append(
                        (filename, os.path.relpath(filename, suite_path)))

        self.statement_files.sort()

    def execute(self):
        for i in range(0, self.args.test_runs):
            # batch execute use single session
            if callable(getattr(self, "batch_execute")):
                # case batch
                for (file_path, suite_name) in self.statement_files:
                    self.suite_now = suite_name
                    statement_list = list()
                    for state in get_statements(file_path, suite_name):
                        statement_list.append(state)

                    try:
                        self.batch_execute(statement_list)
                    except Exception as e:
                        log.warning(
                            f"Get exception when running suite {suite_name}")
                        global_statistics.add_failed(self.kind, self.suite_now,
                                                     e)
                        continue

                    log.info(f"Suite file:{file_path} pass!")
            else:
                raise RuntimeError(
                    f"batch_execute is not implement in runner {self.kind}")

    def execute_statement(self, statement):
        if self.kind not in statement.runs_on:
            log.debug(
                f"Skip execute statement with {self.kind} SuiteRunner, only runs on {statement.runs_on}"
            )
            return
        if self.show_query_on_execution:
            log.Info(
                f"executing statement, type {statement.s_type.type}\n{statement.text}\n"
            )
        start = time.perf_counter()
        if statement.s_type.type == "query":
            self.assert_execute_query(statement)
        elif statement.s_type.type == "error":
            self.assert_execute_error(statement)
        elif statement.s_type.type == "ok":
            self.assert_execute_ok(statement)
        else:
            raise Exception("Unknown statement type")
        end = time.perf_counter()
        time_cost = end - start
        global_statistics.add_perf(self.kind, self.suite_now, statement.text,
                                   time_cost)

    # expect the query just return ok
    def assert_execute_ok(self, statement):
        try:
            actual = safe_execute(lambda: self.execute_ok(statement.text),
                                  statement)
        except Exception as err:
            raise LogicError(runner=self.kind,
                             message=str(err),
                             errorType="statement ok execute with exception")
        if actual is not None:
            raise LogicError(runner=self.kind,
                             message=str(statement),
                             errorType="statement ok get error in response")

    def assert_query_equal(self, f, resultset, statement, with_regex=False):
        # use join after split instead of strip
        compare_f = "".join(f.split())
        compare_result = "".join(resultset[2].split())
        if with_regex:
            try:
                return compare_result_with_reg(resultset[2].split(), f.split())
            except Exception as err:
                raise LogicError(message="\n{}\n Expected:\n{:<80}\n Actual:\n{:<80}\n Statement:{}\n Start " \
                                            "Line: {}, Result Label: {}".format(str(err),
                                                                                resultset[2].rstrip(),
                                                                                f.rstrip(),
                                                                                str(statement), resultset[1],
                                                                                resultset[0].group("label")),
                            errorType="statement query get result not equal to expected(with regex expression)",
                            runner=self.kind,
                            )

        if compare_f != compare_result:
            raise LogicError(message="\n Expected:\n{:<80}\n Actual:\n{:<80}\n Statement:{}\n Start " \
                                            "Line: {}, Result Label: {}".format(resultset[2].rstrip(),
                                                                                f.rstrip(),
                                                                                str(statement), resultset[1],
                                                                                resultset[0].group("label")),
                            errorType="statement query get result not equal to expected",
                            runner=self.kind,
                            )

    def assert_execute_query(self, statement):
        if statement.s_type.query_type == "skipped":
            log.debug(f"{statement.text} statement is skipped")
            return
        try:
            actual = safe_execute(lambda: self.execute_query(statement),
                                  statement)
        except Exception as err:
            raise LogicError(runner=self.kind,
                             message=str(err),
                             errorType="statement query execute with exception")
        try:
            f = format_value(actual, len(statement.s_type.query_type))
        except Exception:
            raise LogicError(
                message=f"{statement} statement type is query but get no result",
                errorType="statement query get no result",
                runner=self.kind)

        if statement.results is None or len(statement.results) == 0:
            raise LogicError(message=f"{statement} no result found by query",
                             errorType="statement query get empty result",
                             runner=self.kind)
        with_regex = False
        if 'R' in statement.s_type.query_type:
            with_regex = True
        hasResult = False
        for resultset in statement.results:
            if resultset[0].group("label") is not None and resultset[0].group(
                    "label") == self.kind:
                self.assert_query_equal(f, resultset, statement, with_regex)
                hasResult = True
        if not hasResult:
            for resultset in statement.results:
                if resultset[0].group("label") is None or len(
                        resultset[0].group("label")) == 0:
                    self.assert_query_equal(f, resultset, statement, with_regex)
                    hasResult = True
        if not hasResult:
            raise LogicError(
                message=f"{statement} no result found in test file",
                errorType="statement query has no result in test file",
                runner=self.kind)

    # expect the query just return error
    def assert_execute_error(self, statement):
        try:
            actual = safe_execute(lambda: self.execute_error(statement.text),
                                  statement)
        except Exception as err:
            raise LogicError(runner=self.kind,
                             message=str(err),
                             errorType="statement error execute with exception")
        if actual is None:
            raise LogicError(
                message=
                f"expected error {statement.s_type.expect_error}, but got ok on statement: {statement.text} ",
                errorType="Error code mismatch",
                runner=self.kind)
        match = re.search(statement.s_type.expect_error, actual.msg)
        if match is None:
            raise LogicError(
                message=
                f"\n expected error regex is {statement.s_type.expect_error}\n actual found {actual}{str(statement)}",
                errorType="Error code mismatch",
                runner=self.kind)

    def run_sql_suite(self):
        log.info(
            f"run_sql_suite for {self.kind} with suites dir {os.path.abspath(self.args.suites)}"
        )
        self.fetch_files()
        self.execute()

    def set_label(self, label):
        self.label = label

    def set_driver(self, driver):
        self.driver = driver
