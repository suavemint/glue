import psycopg2 as psql, sys, csv, logging, time
import datetime
from timer import timeit

def initial_headers(labels, vals):
    output_schema_list = []
    for l, v in zip(labels, vals):
        chk = type(check_and_set_type(v))
        if chk == int:
            if v > 2147483647:
                output_schema_list.append('%s bigint' % l)
            else:
                output_schema_list.append('%s integer' % l)
        elif chk == float:
            output_schema_list.append('%s double' % l)
        elif isinstance(chk, datetime.date):
            output_schema_list.append('%s date' % l)
        else:
            output_schema_list.append('%s varchar' % l)
    return output_schema_list


def return_type_in_psql_type_string(h_string):
    chk = type(check_and_set_type(h_string))
#    print chk
    if chk == int:
        return 'integer'
    elif chk == float:
        return 'double'
    elif isinstance(chk,datetime.date):
        return 'date'
    else:
        return 'varchar'

    
## NB: For large files (>131,074 b? B?), set csv.field_size_limit(N) for N > 131074
@timeit
def generate_schema(i):

    import csv, logging

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    header_max_values, header_types, table_data = [], [], []

    #print 'Input file is',i

    # Try to simply accept a row item, instead of an entire file, to speed up process    
    d = check_input_filetype(i)
        
    in_file = open(i,'rb')
    csv_file = csv.reader(open(i,'rb'), delimiter=d)
    header_labels = csv_file.next()#.split(',')

    print 'There are %i columns in this file.'%len(header_labels)
    trimmed_end = header_labels[-1].replace('\r\n','')
    header_labels[-1] = trimmed_end
    if header_labels[0] == '\xef\xbb\xbfSEQUENCE':
        header_labels[0] = 'SEQUENCE'

    # Clean the column titles.
    for entry in header_labels:
        index = header_labels.index(entry)
        header_max_values.append(len(entry))
        # PostgreSQL does not like "@" as an initial char in a column name, as that's the cursor.
        if '@' in entry:
            header_labels[index] = entry.replace('@','at')

    sample_values = csv_file.next()

#    type_dict = {'varchar' : str,
#                 'bigint'  : int,
#                 'date'    : datetime.date,
#                 'float'   : float}

    # Generate the schema-related string.
    schema_list = initial_headers(header_labels, sample_values)

    # Contrary to the next comment block, let's piece-by-piece varchar-out troublesome columns:
    # 1. Address fields can give trouble, such as with address1 or likely AA ethnic code (6E), so fix those:
    lexis_non_grata = ('address','lalethniccode')

    
    for l in schema_list:
        for s in lexis_non_grata:
            if s in l.lower():
                logger.debug('Found a PNG: %s' % l)
                index = schema_list.index(l)
                varname, ttype = l.split(' ')
                schema_list[index] = varname + ' varchar'            

    row_c = 0
    # Loop over rows in csv to generate the maximum lengths of the columns, if applicable.
    for row in csv_file:
        row_c += 1
        for index, entry in enumerate(row):
            length = len(entry)
            if length > header_max_values[index]:
                header_max_values[index] = length

    # Knowing now the maximum values, insert the max values for the pertinent header items: varchar(n).
    for i,o in enumerate(schema_list):
        if 'varchar' in o:
            schema_list[i] += '(%s)' % header_max_values[i]

    # Generate the outputted string, which will contain the variable type and char-limit for each column.
    print 'Number of rows considered: %i' % (row_c)

    # And with a patched-up list, make it into a string for the schema command.
    schema_string = ', '.join(schema_list)
            
    return header_labels, schema_list


def check_and_set_type(n):
    import datetime
    """ Based on inputted vartype, we want a string back out for PSQL."""

    heuristics = (lambda value: datetime.datetime.strptime(value, '%m-%d-%Y'), int, float)    

    for type in heuristics:
        try:
            return type(n)
        except ValueError:
            continue
    # If input fails all heuristics, input is defaulted to string
    return n


def check_input_filetype(in_f):
    ext = in_f.split('.')[1].lower()
    if ext == 'csv':
        return ','
    elif ext == 'tab':
        return '\t'
    else:
        raise 'Input file is not of TAB or CSV type.'

    
@timeit
def runit():
    
    import sys
    
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.debug('Running script...')
    logger.debug('Begin opening CSV/TAB file.')

    d = ''

#    f_name, schema_name, table_name = 'clipped.csv', 'clipped', 'clipped'
#    f_name, schema_name, table_name = 'FL GOP 050914.csv', 'test_schema', 'fl_test_table'
    f_name, schema_name, table_name = '/home/james/OH God File.csv', 'oh_god_schema2', 'oh_god_test2'


    connection = None

    header_labels, schema_list = generate_schema(f_name)
    num_args = len(header_labels)
            
    schema_string = ', '.join(schema_list)
    
    # Postpending the primary key determination to the string gives us flexiblity otherwise lost by editing the
    # + schema *list* directly.

    for k in schema_list:
        if 'LALVOTERID' in k:
            schema_string += ', PRIMARY KEY (LALVOTERID)'
            break
        else:
            logger.warning('Primary key \"LALVOTERID\" not found; going sans p-key.')

    print schema_string; sys.exit(1)
            
    logger.info('Starting DB ops.')

    try:        
        connection = psql.connect(database='testdb',user='postgres',password='password')
        cursor = connection.cursor()

        # Open the input file again, skipping header row, and write-in each row.
        row_c = 0
        with open(f_name,'rb') as f:      
#            input_file = csv.reader(f, delimiter=check_input_filetype(f_name), quoting=csv.QUOTE_NONE)
#            input_file = csv.reader(f, delimiter=check_input_filetype(f_name), quoting=csv.QUOTE_ALL)
            input_file = csv.reader(f, delimiter=check_input_filetype(f_name), dialect='excel')
            input_file.next()
            
            cursor.execute('DROP TABLE IF EXISTS %s' % table_name)
            
            sample_schema = 'CREATE TABLE %s (%s)' % (table_name, schema_string)  # NB: previously had sliced [:-2] to remove the terminal ', ' from the string.    
            cursor.execute(sample_schema)            
#            header_labels = generate_schema(input_file.next())
            column_labels = ', '.join(header_labels)
                             
            for row in input_file:
                row_c += 1
                for i, entry in enumerate(row):
                    if entry == ' ':
                        entry = None
                    row[i] = psql.extensions.adapt(entry).getquoted()
                    
                values = ','.join([x for x in row])
                insert_query = 'INSERT INTO %s(%s) VALUES (%s)' % (table_name, column_labels, values)
                
                try:
                    cursor.execute(insert_query)
                except Exception, e:
                        #print row
                        #print values
                    # SQL syntax errors don't have a specific etype in psycopg2...
                    logger.warn('Error returned:', e)
                    logger.warn('SQL error code', e.pgcode)
#                    sys.exit(1)

        connection.commit()
        
    except psql.DatabaseError, e:
        if connection:
            connection.rollback()  # Fail gracefully, and don't step on table with new data.
        logger.warn('WARNING: Database failed: %s' % e)
    finally:
        if connection:
            connection.close()  # Regardless of outcome, close connection safely.

            
if __name__ == '__main__':
    runit()
