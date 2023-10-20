def get_s3_env_vars(conn_id):
    return {
        "S3_BUCKET": f"{{{{ get_connection('{conn_id}').schema }}}}",
        "S3_ENDPOINT": f"{{{{ get_connection('{conn_id}').host }}}}",  
        "S3_ACCESS_KEY": f"{{{{ get_connection('{conn_id}').login }}}}",
        "S3_SECRET_KEY": f"{{{{ get_connection('{conn_id}').password }}}}"
    }
