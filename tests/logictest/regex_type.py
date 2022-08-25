import re
from log import log

regex_type_map = {
    'ANYTHING': '.*',  # empty space will make problem
    'DATE': '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d[.]\d\d\d [+-]\d\d\d\d',
    'DATE_IN_SHARE': '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d[.]\d+ UTC'
}

# save regex pattern compile from reg_type_map
regex_pattern_map = {}

# continual empty space list, expecially for multi empty space
# For example DATE: [' ', ' ']
regex_space_info = {}


# parse reg_type_map get reg_space_info and reg_pattern_map
def init_pattern():
    for key, val in regex_type_map.items():
        regex_pattern_map[key] = re.compile(val)

        empty_space_list = []
        item = ''
        for word in val:
            if word != ' ':
                if len(item) != 0:
                    empty_space_list.append(item)
                    item = ''
                continue
            item = item + ' '
        regex_space_info[key] = empty_space_list
    log.debug(
        f"regex pattern init, expression: {regex_type_map}, space list: {regex_space_info}"
    )


def check_reg(name, actual):
    if name not in regex_pattern_map:
        return False, "No such regex expression name support"
    m = regex_pattern_map[name].match(actual)
    if m is None:
        return False, "No match"
    return True, "OK"


# result is a list with list;
# expect is a string
# split expect into item and compare with result
def compare_result_with_reg(test_expect, test_result):
    index = 0  # for list of test_result which split from result string
    for col in test_expect:
        if col.startswith('$'):
            name = col[1:]
            if not name in regex_type_map:
                raise Exception("Not such regex expression name")
            item = ""  # save column record
            empty_space_list = regex_space_info[name]
            col_count = len(empty_space_list)
            for i in range(col_count + 1):
                if i < col_count:
                    item += test_result[index] + empty_space_list[i]
                else:
                    item += test_result[index]
                index += 1
            log.debug(f"Parse column regex {name} with result {item}")
            ok, msg = check_reg(name, item)
            if not ok:
                raise Exception(
                    f"get result '{item}' not match regex expression {name}: {regex_type_map[name]}"
                )
        else:
            if test_result[index] != col:
                raise Exception(
                    f"get result '{test_result[index]}' not equal expect '{col}'"
                )
            index += 1


init_pattern()

if __name__ == '__main__':
    # example
    test_expect = ['db1', 't1', 'FUSE', '$ANYTHING', '$DATE', 'NULL', '0', '0', '0', '0',\
        'db2', 't2', 'FUSE', '$ANYTHING', '$DATE', 'NULL', '0', '0', '0', '0']
    test_result =  ['db1', 't1', 'FUSE', '(a)', '2022-08-22', '03:34:42.338', '+0000', 'NULL', '0', '0', '0', '0',\
         'db2', 't2', 'FUSE', '(b)', '2021-08-21', '04:34:41.653', '-0000', 'NULL', '0', '0', '0', '0']
    compare_result_with_reg(test_expect, test_result)
    # print(check_reg("ANYTHING", "xxxx"))
    # print(check_reg("DATE", "2022-08-19 05:42:43.593 #0000"))
    # print(check_reg("DATE", "2022-08-19 05:42:43.593 -0000"))
    # print(check_reg("DATE", "2022-08-19 05:42:43.593"))
