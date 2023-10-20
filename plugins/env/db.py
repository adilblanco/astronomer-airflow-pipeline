def get_database_env_vars(conn_id, port=True, macro='build_conn_string'):
    return {"DB_CONN_STRING": _build_db_conn_string(conn_id, port, macro)}


def _build_db_conn_string(conn_id, port, macro):
    return f"{{{{ {macro}(" + \
            f"get_connection('{conn_id}').host, " + \
            f"get_connection('{conn_id}').login, " + \
            f"get_connection('{conn_id}').password, " + \
            f"get_connection('{conn_id}').schema" + \
            f"""{ ", get_connection('" + conn_id + "').port" if port else "" }""" + \
        f") }}}}"
