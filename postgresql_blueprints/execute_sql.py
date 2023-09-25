from sqlalchemy import text
from sqlalchemy.pool import NullPool
from postgresql_blueprints.db_utils import setup_connection
import argparse
import os


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=False)
    parser.add_argument('--password', dest='password', required=False)
    parser.add_argument('--host', dest='host', required=False)
    parser.add_argument('--database', dest='database', required=False)
    parser.add_argument('--port', dest='port', default='5432', required=False)
    parser.add_argument(
        '--url-parameters',
        dest='url_parameters',
        required=False)
    parser.add_argument(
        '--db-connection-url',
        dest='db_connection_url',
        required=False)
    parser.add_argument('--query', dest='query', required=True)

    parser.add_argument("--sslmode", dest='sslmode', required=False)
    parser.add_argument('--client-cert-path', dest='client_cert_path', required=False)
    parser.add_argument('--client-key-path', dest='client_key_path', required=False)
    parser.add_argument('--server-ca-path', dest='server_ca_path', required=False)
    args = parser.parse_args()

    if not args.db_connection_url and not (
            args.host or args.database or args.username) and not os.environ.get('DB_CONNECTION_URL'):
        parser.error(
            """This Blueprint requires at least one of the following to be provided:\n
            1) --db-connection-url\n
            2) --host, --database, and --username\n
            3) DB_CONNECTION_URL set as environment variable""")
    if args.host and not args.database and not args.username:
        parser.error(
            '--host requires --database and --username')
    if args.database and not args.host and not args.username:
        parser.error(
            '--database requires --host and --username')
    return args



def main():
    args = get_args()
    query = text(args.query)

    try:
        db_connection = setup_connection(args)
    except Exception as e:
        print(f'Failed to connect to database {args.database}')
        raise e
    try:
        with db_connection.connect() as conn:
            result = conn.execute(query)
        print(result.context)
        print(f'Affected row count: {result.rowcount}')
    except Exception as e:
        print(f'Failed to execute query. Error message: {e}')


if __name__ == '__main__':
    main()
