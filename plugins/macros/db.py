def build_postgres_conn_string(host, login, password, dbname, port=None):
    return f"""host='{host}' dbname='{dbname}' user='{login}' password='{password}'{f" port='{port}'" if port else ""}"""
