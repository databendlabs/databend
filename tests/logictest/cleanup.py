# tuple of (type, name)
# example ('database', 'db1'), ('table', 't1')
need_cleanup_set = set()
enable_auto_cleanup = False


def set_auto_cleanup(flag):
    global enable_auto_cleanup
    enable_auto_cleanup = flag


def get_cleanup_statements():
    if not enable_auto_cleanup:
        return []
    global need_cleanup_set
    res = []
    for type, name in need_cleanup_set:
        res.append(f"drop {type} if exists {name};")
    need_cleanup_set = set()
    return res


def pick_create_statement(statement):
    if not enable_auto_cleanup:
        return
    global need_cleanup_set
    statement_lower = statement.lower()
    if 'create' not in statement_lower:
        return
    statement_words = statement_lower.strip(';').split()

    create_type_index = 1
    create_name_index = 2
    if 'if' in statement_lower and 'not' in statement_lower and 'exists' in statement_lower:
        create_type_index = 5
    elif 'if' in statement_lower and 'exists' in statement_lower:
        create_type_index = 4
    else:
        create_type_index = 2

    if 'transient' in statement_lower:
        create_type_index += 1
        create_name_index += 1

    create_type = statement_words[1]
    create_name = statement_words[create_name_index]
    need_cleanup_set.add((create_type, create_name.split('(')[0]))
