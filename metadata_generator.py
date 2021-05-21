import argparse
import logging
import random
import sys
import uuid

from teradatasql import connect

_DATA_TYPES = [
    'INTEGER', 'CHAR(25)', 'DECIMAL(8,2)', 'DATE', 'VARCHAR(25)', 'SMALLINT',
    'CHAR', 'BYTEINT'
]

_COLUMN_NAMES = [
    'name', 'address', 'city', 'state', 'date_time', 'paragraph', 'randomdata',
    'person', 'credit_card', 'size', 'reason', 'school', 'food', 'location',
    'house', 'price', 'cpf', 'cnpj', 'passport', 'security_number',
    'phone_number', 'bank_account_number', 'ip_address', 'stocks'
]

_TABLE_NAMES = [
    'school_info', 'personal_info', 'persons', 'employees', 'companies',
    'store', 'home'
]

_DATABASE_NAMES = [
    'Harinder1_warehouse', 'Harinder2_warehouse', 'Harinder3_warehouse',
    'Harinder4_warehouse', 'Harinder4_warehouse'
]

_INTEGER_DATA = [12,13,14,15,16]

_CHAR25_DATA = ['PT', 'CN', 'TH', 'ID', 'JP', 'IE', 'SE', 'AR', 'NA', 'CN']
_CHAR_DATA = ['A', 'B','C','D','E','F','G','H','I']

_DECIMEL_DATA = ['749.68', '494.14', '5.17', '475.55', '769.86', '979.3', '123.25','795.51', '408.95', '617.38']

_DATE_DATA = ['2021-01-01', '2020-02-02', '2019-03-03', '1985-05-28', '2018-09-01', '2020-05-28', '2021-05-28']

_VARCHAR_DATA = ['Kin', 'Torrence', 'Virgie', 'Corrina', 'See', 'Donia', 'Lorette', 'Tamqrah', 'Gena', 'Rebecka']

_SMALLINT_DATA = [1,2,3,4,5,6,7,8,9,0]

_TABLE_DATATYPE = []



def get_conn(connection_args):
    return connect(None,
                   database=connection_args['database'],
                   host=connection_args['host'],
                   user=connection_args['user'],
                   password=connection_args['pass'])


def create_random_metadata(connection_args):
    conn = get_conn(connection_args)

    cursor = conn.cursor()

    for x in range(1000):
        database_name, database_stmt = build_create_database_statement()
        cursor.execute(database_stmt)
        for y in range(1000):
            tablename , query = build_create_table_statement(database_name)
            print('\n' + query)
            print('\n' +'DATABASE NO : '+ str(x+1) +' TABLE NO : '+str(y+1))
            cursor.execute(query)
            for z in range(1):
                insertquery = build_insert_data_statment(database_name, tablename)
                cursor.execute(insertquery)
        conn.commit()

    cursor.close()


def get_random_database_name():
    return random.choice(_DATABASE_NAMES)


def build_create_database_statement():
    database_name = '{}{}'.format(get_random_database_name(),
                                  str(random.randint(1, 100000)))
    database_stmt = 'CREATE DATABASE "{}" AS '.format(
        database_name)
    database_stmt += 'PERM = 2000000*(HASHAMP()+1), '
    database_stmt += 'SPOOL = 2000000*(HASHAMP()+1), '
    database_stmt += 'TEMPORARY = 2000000*(HASHAMP()+1) '

    # database_stmt += 'PERM = 200000000, '
    # database_stmt += 'SPOOL = 200000000, '
    # database_stmt += 'TEMPORARY = 200000000 '
    return database_name, database_stmt


def get_random_data_type():
    return random.choice(_DATA_TYPES)


def get_random_column_name():
    return random.choice(_COLUMN_NAMES)


def get_random_table_name():
    return random.choice(_TABLE_NAMES)

# all the below mentioned statments are for insert stmt

def get_random_int_value():
    return random.choice(_INTEGER_DATA)

def get_randon_char25_value():
    return random.choice(_CHAR25_DATA)

def get_randon_char_value():
    return random.choice(_CHAR_DATA)

def get_random_decimel_value():
    return random.choice(_DECIMEL_DATA)

def get_random_date_value():
    return random.choice(_DATE_DATA)

def get_random_varchar_value():
    return random.choice(_VARCHAR_DATA)

def get_random_smallint_value():
    return random.choice(_SMALLINT_DATA)

def get_random_byteint_value():
    return random.choice(_SMALLINT_DATA)

def build_insert_data_statment(database_name, table_name):

    insert_stmt = 'insert into {}.{} values ( {} '.format(database_name, table_name, get_insert_stmt_data(_TABLE_DATATYPE[0]))

    iterateDatatype = iter(_TABLE_DATATYPE)
    next(iterateDatatype)

    for x in iterateDatatype:
        #print(x)
        insert_stmt += ',{}'.format( get_insert_stmt_data(x))

    insert_stmt = '{} )'.format(insert_stmt)
    print(insert_stmt)
    return insert_stmt


def get_insert_stmt_data(x):

    if x == 'INTEGER':
        return get_random_int_value()
    elif x == 'CHAR(25)':
        return  "'" + get_randon_char25_value() + "'"
    elif x == 'DECIMAL(8,2)':
        return get_random_decimel_value()
    elif x == 'DATE':
        return "'" +get_random_date_value()+ "'"
    elif x == 'VARCHAR(25)':
        return "'" +get_random_varchar_value()+"'"
    elif x == 'SMALLINT':
        return get_random_smallint_value()
    elif x == 'CHAR':
        return "'"+get_randon_char_value()+"'"
    else:
        return get_random_byteint_value()


def build_create_table_statement(database_name):

    table_name = get_random_table_name()+uuid.uuid4().hex[:8]
    table_stmt = 'CREATE MULTISET TABLE {}.{} ( '.format(database_name,
                                                  table_name)
    _TABLE_DATATYPE.clear()
    datatype = get_random_data_type()
    _TABLE_DATATYPE.append(datatype)

    firstColName = get_random_column_name() + str(random.randint(1,100000000))
    table_stmt = '{}{} {} NOT NULL'.format(table_stmt, firstColName,
                                    datatype)

    for x in range(random.randint(1, 15)):
        datatype_2 = get_random_data_type()
        _TABLE_DATATYPE.append(datatype_2)
        table_stmt += ', {}{}'.format(get_random_column_name(),
                                      str(random.randint(1, 100000000))) + \
            ' {}'.format(datatype_2)

    table_stmt = '{} ,CONSTRAINT primary_1 PRIMARY KEY ( {} ) )'.format(table_stmt, firstColName)
    return table_name, table_stmt



def parse_args():
    parser = argparse.ArgumentParser(
        description='Command line generate random metadata into teradata')
    parser.add_argument('--teradata-host',
                        help='Your teradata server host',
                        required=True)
    parser.add_argument('--teradata-user',
                        help='Your teradata credentials user')
    parser.add_argument('--teradata-pass',
                        help='Your teradata credentials password')
    parser.add_argument('--teradata-database',
                        help='Your teradata database name')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    # Enable logging
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    create_random_metadata({
        'database': args.teradata_database,
        'host': args.teradata_host,
        'user': args.teradata_user,
        'pass': args.teradata_pass
    })