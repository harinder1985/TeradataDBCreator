import random


_INTEGER_DATA = [ '3537868957981190', '56022470409794790', '4911383411549928844', '374288015094948', '3539756380585704', '3530996831189685', '201479103345099',
                  '560221619577097390', '3588434505844832', '3580315067040703'
                  ]

_CHAR25_DATA = ['PT', 'CN', 'TH', 'ID', 'JP', 'IE', 'SE', 'AR', 'NA', 'CN']
_CHAR_DATA = ['A', 'B','C','D','E','F','G','H','I']

_DECIMEL_DATA = ['749.68', '494.14', '5.17', '475.55', '769.86', '979.3', '123.25','795.51', '408.95', '617.38']

_DATE_DATA = ['2021-01-01', '2020-02-02', '2019-03-03', '1985-05-28', '2018-09-01', '2020-05-28', '2021-0-28']

_VARCHAR_DATA = ['Kin', 'Torrence', 'Virgie', 'Corrina', 'See', 'Donia', 'Lorette', 'Tamqrah', 'Gena', 'Rebecka']

_SMALLINT_DATA = [1,2,3,4,5,6,7,8,9,0]

_TABLE_DATATYPE = [
    'INTEGER', 'CHAR(25)', 'DECIMAL(8,2)', 'DATE', 'VARCHAR(25)', 'SMALLINT',
    'CHAR', 'BYTEINT'
]

def get_random_int_value():
    return random.choice(_INTEGER_DATA)

def get_randon_char_value():
    return random.choice(_CHAR_DATA)

def get_randon_char25_value():
    return random.choice(_CHAR25_DATA)

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


build_insert_data_statment('harinder_db', 'test')
