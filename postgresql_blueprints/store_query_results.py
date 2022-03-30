from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import argparse
import os
import pandas as pd


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
    parser.add_argument('--query', dest='query', required=True)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default='output.csv',
        required=True)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--file-header',
        dest='file_header',
        default='True',
        required=False)
    parser.add_argument(
        '--db-connection-url',
        dest='db_connection_url',
        required=False)
    args = parser.parse_args()

    if not args.db_connection_url and not (
            args.host or args.database or args.username) and not os.environ.get('DB_CONNECTION_URL'):
        parser.error(
            """This Blueprint requires at least one of the following to be provided:\n
            1) --db-connection-url\n
            2) --host, --database, and --username\n
            3) DB_CONNECTION_URL set as environment variable""")
    if args.host and not (args.database or args.username):
        parser.error(
            '--host requires --database and --username')
    if args.database and not (args.host or args.username):
        parser.error(
            '--database requires --host and --username')
    if args.username and not (args.host or args.username):
        parser.error(
            '--username requires --host and --username')
    return args


def create_connection_string(args):
    """
    Set the database connection string as an environment variable using the keyword arguments provided.
    This will override system defaults.
    """
    if args.db_connection_url:
        os.environ['DB_CONNECTION_URL'] = args.db_connection_url
    elif (args.host and args.database):
        os.environ['DB_CONNECTION_URL'] = f'postgresql://{args.username}:{args.password}@{args.host}:{args.port}/{args.database}?{args.url_parameters}'

    db_string = os.environ.get('DB_CONNECTION_URL')
    return db_string


def convert_to_boolean(string):
    """
    Shipyard can't support passing Booleans to code, so we have to convert
    string values to their boolean values.
    """
    if string in ['True', 'true', 'TRUE']:
        value = True
    else:
        value = False
    return value


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')

    return combined_name


def create_csv(query, db_connection, destination_file_path, file_header=True):
    """
    Read in data from a SQL query. Store the data as a csv.
    """
    i = 1
    for chunk in pd.read_sql_query(query, db_connection, chunksize=10000):
        if i == 1:
            chunk.to_csv(destination_file_path, mode='a',
                         header=file_header, index=False)
        else:
            chunk.to_csv(destination_file_path, mode='a',
                         header=False, index=False)
        i += 1
    print(f'{destination_file_path} was successfully created.')
    return


def create_db_connection(db_string):
    if 'db.bit.io' in db_string:
        db_connection = create_engine(
            db_string,
            connect_args={'sslmode': 'require'},
            isolation_level='AUTOCOMMIT')
    else:
        db_connection = create_engine(
            db_string,
            execution_options=dict(stream_results=True))
    return db_connection


def main():
    args = get_args()
    destination_file_name = args.destination_file_name
    destination_folder_name = args.destination_folder_name
    destination_full_path = combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=destination_file_name)
    file_header = convert_to_boolean(args.file_header)
    query = text(args.query)

    if not os.path.exists(destination_folder_name) and (
            destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    db_string = create_connection_string(args)
    try:
        db_connection = create_db_connection(db_string)
    except Exception as e:
        print(f'Failed to connect to database {args.database}')
        raise(e)

    create_csv(
        query=query,
        db_connection=db_connection,
        destination_file_path=destination_full_path,
        file_header=file_header)
    db_connection.dispose()


if __name__ == '__main__':
    main()
