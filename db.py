import os
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
