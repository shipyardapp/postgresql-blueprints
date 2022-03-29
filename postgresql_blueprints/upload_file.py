from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import argparse
import os
import glob
import re
import pandas as pd
import csv
from io import StringIO


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
    parser.add_argument('--source-file-name-match-type',
                        dest='source_file_name_match_type',
                        default='exact_match',
                        choices={
                            'exact_match',
                            'regex_match'},
                        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        default='output.csv',
        required=True)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--table-name',
        dest='table_name',
        default=None,
        required=True)
    parser.add_argument(
        '--schema',
        dest='schema',
        default=None,
        required=False)
    parser.add_argument(
        '--insert-method',
        dest='insert_method',
        choices={
            'fail',
            'replace',
            'append'},
        default='append',
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


def find_all_local_file_names(source_folder_name):
    """
    Returns a list of all files that exist in the current working directory,
    filtered by source_folder_name if provided.
    """
    cwd = os.getcwd()
    cwd_extension = os.path.normpath(f'{cwd}/{source_folder_name}/**')
    file_names = glob.glob(cwd_extension, recursive=True)
    return [file_name for file_name in file_names if os.path.isfile(file_name)]


def find_all_file_matches(file_names, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for file in file_names:
        if re.search(file_name_re, file):
            matching_file_names.append(file)

    return matching_file_names


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')

    return combined_name


def determine_upload_method(db_connection):
    if 'db.bit.io' in str(db_connection):
        upload_method = bitio_upload_method
    else:
        upload_method = 'multi'

    return upload_method


def upload_data(
        source_full_path,
        table_name,
        insert_method,
        db_connection,
        schema):

    upload_method = determine_upload_method(db_connection)
    if os.path.getsize(source_full_path) < 50000000:
        # Avoid chunksize if the file is small, since this is faster.
        df = pd.read_csv(source_full_path)
        df.to_sql(
            table_name,
            con=db_connection,
            index=False,
            if_exists=insert_method,
            method=upload_method,
            schema=schema)
    else:
        # Resort to chunks for larger files to avoid memory issues.
        for index, chunk in enumerate(
                pd.read_csv(source_full_path, chunksize=10000)):

            if insert_method == 'replace' and index > 0:
                # First chunk replaces the table, the following chunks
                # append to the end.
                insert_method = 'append'

            chunk.to_sql(
                table_name,
                con=db_connection,
                index=False,
                if_exists=insert_method,
                method=upload_method,
                chunksize=10000,
                schema=schema)
    print(f'{source_full_path} successfully uploaded to {table_name}.')


def create_db_connection(db_string):
    if 'db.bit.io' in db_string:
        db_connection = create_engine(
            db_string,
            connect_args={'sslmode': 'require'},
            isolation_level='AUTOCOMMIT')
    else:
        db_connection = create_engine(
            db_string)
    return db_connection


def bitio_upload_method(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(f'"{k}"' for k in keys)
        table_name = f'"{table.schema}"."{table.name}"'
        sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=sql, file=s_buf)


def main():
    args = get_args()
    source_file_name_match_type = args.source_file_name_match_type
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    table_name = args.table_name
    insert_method = args.insert_method

    if args.schema == '':
        schema = None
    else:
        schema = args.schema

    db_string = create_connection_string(args)
    try:
        db_connection = create_db_connection(db_string)
    except Exception as e:
        print(f'Failed to connect to database {args.database}')
        raise(e)

    if source_file_name_match_type == 'regex_match':
        file_names = find_all_local_file_names(source_folder_name)
        matching_file_names = find_all_file_matches(
            file_names, re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to upload...')

        for index, key_name in enumerate(matching_file_names):
            upload_data(
                source_full_path=key_name,
                table_name=table_name,
                insert_method=insert_method,
                db_connection=db_connection,
                schema=schema)

    else:
        upload_data(
            source_full_path=source_full_path,
            table_name=table_name,
            insert_method=insert_method,
            db_connection=db_connection,
            schema=schema)
    db_connection.dispose()


if __name__ == '__main__':
    main()
