from sqlalchemy import create_engine


def setup_connection(args):
    """
    Set up a connection to the database using the keyword arguments provided.
    This will override system defaults.

    :param args: keyword arguments provided by the user
    :return: a connection to the database
    """
    db_string = create_connection_string(args)
    if connect_args := format_connect_args(args):
        return create_db_connection(db_string, connect_args=connect_args)
    else:
        return create_db_connection(db_string)


def create_db_connection(db_string, connect_args=None):
    """
    Create a connection to the database using the connection string provided.
    This will override system defaults.

    :param db_string: connection string to the database
    :param connect_args: additional arguments to pass to the connection
    :return: a connection to the database
    """
    if 'db.bit.io' in db_string:
        return create_engine(
            db_string,
            connect_args={'sslmode': 'require'},
            isolation_level='AUTOCOMMIT',
        )
    elif connect_args:
        return create_engine(
            db_string, connect_args=connect_args, isolation_level='AUTOCOMMIT'
        )
    else:
        return create_engine(db_string)


def format_connect_args(args):
    """
    Format the connection arguments provided by the user.

    :param args: keyword arguments provided by the user
    :return: a dictionary of connection arguments
    """
    connect_args = {}
    if args.sslmode:
        connect_args['sslmode'] = args.sslmode
    if args.client_cert_path:
        connect_args['sslcert'] = args.client_cert_path
    if args.client_key_path:
        connect_args['sslkey'] = args.client_key_path
    if args.server_ca_path:
        connect_args['sslrootcert'] = args.server_ca_path
    return connect_args


def create_connection_string(args):
    """
    Set the database connection string as an environment variable using the keyword arguments provided.
    This will override system defaults.

    :param args: keyword arguments provided by the user
    :return: a connection string to the database
    """
    if not args.username or not args.host:
        raise ValueError("Both username and host must be provided.")

    db_string = f'postgresql://{args.username}'
    if args.password:
        db_string += f':{args.password}'
    if args.host:
        db_string += f'@{args.host}'
    if args.port:
        db_string += f':{args.port}'
    if args.database:
        db_string += f'/{args.database}'
    if args.url_parameters:
        db_string += f'?{args.url_parameters}'

    return db_string
