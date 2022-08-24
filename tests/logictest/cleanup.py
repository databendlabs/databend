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
    create_type = statement_words[1] 
    if 'if' in statement_lower and 'not' in statement_lower and 'exists' in statement_lower:
        create_name =  statement_words[5]
    elif 'if' in statement_lower and 'exists' in statement_lower:
        create_name =  statement_words[4]
    else:
        create_name = statement_words[2]
    need_cleanup_set.add((create_type, create_name.split('(')[0]))

# if __name__ == '__main__':
#     pick_create_statement("Create database db2;")
#     pick_create_statement("drop table db1")
#     pick_create_statement("CREATE database DB8;")
#     pick_create_statement("create TABLE t2;")
#     pick_create_statement("create user t2;")
#     print(get_cleanup_statements())
