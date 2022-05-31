import abc
import collections
import glob
import os
import re

import six
from hamcrest import assert_that, is_, none, is_not

from log import log

# statement is a statement in sql logic test
state_regex = r"^\s*statement\s+(?P<statement>((?P<ok>OK)|((?P<error>)ERROR\s*(?P<expectError>.*))|(?P<query>QUERY\s*((" \
              r"ERROR\s+(?P<queryError>.*))|(?P<queryOptions>.*)))))$"

result_regex = r"^----\s*(?P<label>.*)?$"


# return the statement type
# `None` represent that the current format is not a statement type
def get_statement_type(line):
    return re.match(state_regex, line, re.MULTILINE | re.IGNORECASE)


def get_result_label(line):
    return re.match(result_regex, line, re.MULTILINE | re.IGNORECASE)


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

    def __init__(self, message, expected):
        self.message = message
        self.expected = expected

    def __str__(self):
        return "Expected regex{}, Actual: {}".format(self.expected,
                                                     self.message)


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
                    raise Exception("Invalid query options: {}".format(qo))
                if len(s) == 1:
                    if is_empty_line(s[0]):
                        raise Exception(
                            "Invalid query options, query type should not be empty: {}"
                            .format(qo))
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
            raise Exception("Unknown statement type {}".format(matched.group()))

    def __str__(self):
        s = "Statement: {}, type: {}".format(self.type, self.query_type)
        if self.type == "query":
            if self.query_type is not None:
                s += ", query_type: {}".format(self.query_type)
                if self.label is not None:
                    s += ", label: {}".format(self.label)
                s += ", retry: {}".format(self.retry)

        return s


class ParsedStatement(
        collections.namedtuple(
            'ParsedStatement',
            ["at_line", "s_type", "suite_name", "text", "results"])):

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
    lines = get_lines(suite_path)
    for line_idx, line in lines:
        if is_empty_line(line):
            # empty line or junk lines
            continue
        statement_type = get_statement_type(line)

        if statement_type is None:
            continue

        s = Statement(statement_type)
        text = get_single_statement(lines)
        results = []
        if s.type == "query" and s.query_type is not None:
            # TODO need a better way to get all results
            if s.label is None:
                results.append(get_result(lines))
            else:
                for i in s.label:
                    results.append(get_result(lines))
        yield ParsedStatement(line_idx, s, suite_name, text, results)


def format_value(vals, val_num):
    row = len(vals) // val_num
    width = len(str(vals[0])) + 2
    for i in range(len(vals)):
        width = max(width, len(vals[i]) + 2)
    table = ""
    for i in range(row):
        ans = []
        for j in range(val_num):
            ans.append('{: >{w}}'.format(str(vals[i * val_num + j]), w=width))
        table += "".join(ans)
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

    def __init__(self, kind):
        self.label = None
        self.retry_time = 3
        self.driver = None
        self.path = "./suites/"
        self.statement_files = []
        self.kind = kind
        self.show_query_on_execution = True
        self.on_error_return = False

    # return all files under the path
    # format: a list of file absolute path and name(relative path)
    def fetch_files(self):
        for filename in glob.iglob('{}/**'.format(self.path), recursive=True):
            if os.path.isfile(filename):
                self.statement_files.append(
                    (filename, os.path.relpath(filename, self.path)))

    def execute(self):
        # batch execute use single session
        if callable(getattr(self, "batch_execute")):
            # case batch
            for (file_path, suite_name) in self.statement_files:
                statement_list = list()
                for state in get_statements(file_path, suite_name):
                    statement_list.append(state)
                self.batch_execute(statement_list)
        else:
            # case one by one
            for (file_path, suite_name) in self.statement_files:
                for state in get_statements(file_path, suite_name):
                    self.execute_statement(state)

    def execute_statement(self, statement):
        if self.show_query_on_execution:
            log.info("executing statement, type {}\n{}\n".format(
                statement.s_type.type, statement.text))
        if statement.s_type.type == "query":
            self.assert_execute_query(statement)
        elif statement.s_type.type == "error":
            self.assert_execute_error(statement)
        elif statement.s_type.type == "ok":
            self.assert_execute_ok(statement)
        else:
            raise Exception("Unknown statement type")

    # expect the query just return ok
    def assert_execute_ok(self, statement):
        actual = safe_execute(lambda: self.execute_ok(statement.text),
                              statement)
        assert_that(
            actual,
            is_(none()),
            str(statement),
        )

    def assert_query_equal(self, f, resultset, statement):
        # use join after split instead of strip
        compare_f = "".join(f.split())
        compare_result = "".join(resultset[2].split())
        assert compare_f == compare_result, "Expected:\n{}\n Actual:\n{}\n Statement:{}\n Start " \
                                                  "Line: {}, Result Label: {}".format(resultset[2].rstrip(),
                                                                                      f.rstrip(),
                                                                                      str(statement), resultset[1],
                                                                                      resultset[0].group("label"))

    def assert_execute_query(self, statement):
        actual = safe_execute(lambda: self.execute_query(statement), statement)
        try:
            f = format_value(actual, len(statement.s_type.query_type))
        except Exception:
            log.warning("{} statement type is query but return nothing".format(
                statement))
            raise
        assert statement.results is not None and len(
            statement.results) > 0, "No result found {}".format(statement)
        hasResult = False
        for resultset in statement.results:
            if resultset[0].group("label") is not None and resultset[0].group(
                    "label") == self.kind:
                self.assert_query_equal(f, resultset, statement)
                hasResult = True
        if not hasResult:
            for resultset in statement.results:
                if resultset[0].group("label") is None or len(
                        resultset[0].group("label")) == 0:
                    self.assert_query_equal(f, resultset, statement)
                    hasResult = True
        assert hasResult, "No result found {}".format(statement)

    # expect the query just return error
    def assert_execute_error(self, statement):
        actual = safe_execute(lambda: self.execute_error(statement.text),
                              statement)
        if actual is None:
            raise Exception("Expected error but got none")
        match = re.search(statement.s_type.expect_error, actual.msg)
        assert_that(
            match, is_not(none()),
            "statement {}, expect error regex {}, found {}".format(
                str(statement), statement.s_type.expect_error, actual))

    def run_sql_suite(self):
        log.info("run_sql_suite for {} on base {}".format(
            self.kind, os.path.abspath(self.path)))
        self.fetch_files()
        self.execute()

    def set_label(self, label):
        self.label = label

    def set_driver(self, driver):
        self.driver = driver
