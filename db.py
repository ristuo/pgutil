import os
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs


def get_db_connection(database):
    host = os.environ['DB_HOST']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    port = os.environ['DB_PORT']
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        port=port,
        password=password
    )
    return conn


def write_to_table(conn, table_name, logger, dict_list):
    if len(dict_list) == 0:
        logger.info('No need to write an empty list, returning')
        return 0
    if len(dict_list) > 500:
        logger.info('Inserting ' + str(len(dict_list)) + ' rows may take a while :(')
    cursor = conn.cursor()
    i = 1
    try:
        for row in dict_list:
            logger.debug('Preparing insert statement for row ' + str(i))
            i += 1
            columns = row.keys()
            values = [row[k] for k in columns]
            col_separated_by_comma = ','.join(columns)
            insert_statement = 'insert into {} (%s) values %s '.format(table_name)
            cursor.execute(
                insert_statement,
                (AsIs(col_separated_by_comma), tuple(values))
            )
        logger.info('Committing database insert')
        conn.commit()
        logger.info('Done, wrote ' + str(i) + ' rows')
    finally:
        cursor.close()
    return i


def get_as_df(dbname, qs, conn=None):
    should_close = False
    if conn is None:
        should_close = True
        conn = get_db_connection(dbname)
    try:
        cursor = conn.cursor()
        cursor.execute(qs)
        res = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    finally:
        if cursor is not None:
            cursor.close()
        if should_close:
            conn.close()
    return pd.DataFrame(res, columns=colnames)

