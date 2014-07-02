import psycopg2 as psql, sys, csv, logging, time
import datetime
from timer import timeit

## NB: For large files (>131,074 b? B?), set csv.field_size_limit(N) for N > 131074
@timeit
def generate_schema(i):

    import csv, logging#, time

    #schema_start_time = time.time()

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    header_max_values, header_types, table_data = [], [], []
    d = ''

    print 'Input file is',i

    if i.lower().endswith('.tab'):
        d = '\t'
    if i.lower().endswith('.csv'):
        d = ','
        
#    in_file = open(i,'rb')
    csv_file = csv.reader(open(i,'rb'), delimiter=d, quoting=csv.QUOTE_NONE)    
    header_labels = csv_file.next()#.split(',')
    trimmed_end = header_labels[-1].replace('\r\n','')
    header_labels[-1] = trimmed_end

    # Clean the column titles.
    for entry in header_labels:
        index = header_labels.index(entry)
        header_max_values.append(len(entry))
        # PostgreSQL does not like "@" as an initial char in a column name, as that's the cursor.
        if '@' in entry:
            header_labels[index] = entry.replace('@','at')

    # Loop over rows in csv.
    for row in csv_file:
        for entry in row:
            length = len(entry)
            index = row.index(entry)
#            print length,
            if length > header_max_values[index]:
                header_max_values[index] = length
            else:
                continue

    # Generate the outputted string, which will contain the variable type and char-limit for each column.
    string_to_add = ''
    
    header_string = ','.join([x for x in header_labels])

    for x, num in zip(header_labels, header_max_values):
        #string_to_add = ''
        chk = type(x)
        if chk == int:
            string_to_add += '%s integer, '
        elif chk == float:
            string_to_add += '%s double, '
        else:
            string_to_add += '%s varchar(%s), ' % (x, num)
            
#        string_to_add += '%s varchar(%s), ' % (x, num)
#        string_to_add += 
#    schema_end_time = time.time()
#    print 'Schema generation took %.2f seconds.' % (schema_end_time - schema_start_time)

    return header_string, string_to_add


heuristics = (lambda value: datetime.strptime(value, '%m-%d-%Y'), int, float)

def check_and_set_type(n):
    """ Based on inputted vartype, we want a string back out for PSQL."""

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
    
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.debug('Running script...')
    logger.debug('Begin opening CSV/TAB file.')

    d = ''
#    f_name = 'clipped.csv'
    f_name = 'FL GOP 050914.csv'
#    schema_string, header_labels = generate_schema(f_name)
    header_labels, schema_string = generate_schema(f_name)

#    print schema_string
#    print; print
#    print header_labels

    schema_name = 'test_schema'
    table_name = 'test_table'
    sample_schema_cmd = 'CREATE %s' % schema_name

#    print schema_string
        
    # TODO: consider NOT NULL cases, etc. Is this constraint applicable to our case(s)?
    sample_schema = 'CREATE TABLE %s (%s)' % (table_name, schema_string[:-2])
#    print sample_schema

    # Now that we have the entire SQL query ready to go, let's dump it in a PSQL DB:
    connection = None

    logger.info('Starting DB ops.')

    try:
        connection = psql.connect(database='testdb',user='postgres',password='password')
        cursor = connection.cursor()

        num_args = len(header_labels)

        cursor.execute('DROP TABLE IF EXISTS %s' % table_name)
        cursor.execute(sample_schema)

        command_str = 'INSERT INTO %s VALUES' % table_name

        db_start_time = time.time()

        # Open the input file again, skipping header row, and write-in each row.
#        print 50*'='
        with open(f_name,'rb') as f:
            input_file = csv.reader(f, delimiter=check_input_filetype(f_name), quoting=csv.QUOTE_NONE)
            input_file.next()
            for row in input_file:
                for entry in row:
                    i = row.index(entry)                    
                    #print entry,
                    row[i] = psql.extensions.adapt(entry).getquoted()
                values = ','.join([z for z in row])
#                print values
                insert_query = 'INSERT INTO %s(%s) VALUES (%s)' % (table_name, header_labels, values)
                try:
                    cursor.execute(insert_query)
                except Exception, e:
                    # SQL syntax errors don't have a specific etype in psycopg2...
                    print 'Error returned:', e
                    print header_labels, '\n\n', values, '\n\n', insert_query
                    sys.exit(1)
        #for row in table_data:
    #        print row
    #        print 'Length of row items:', len(row)
    #        for entry in row:
    #           print entry
                #if entry == '' or entry == ' ':
                #    row[i] = None    
    ##            if ' ' in entry and entry is not ' ':
    ##                row[i] = psql.extensions.adapt(entry).getquoted()
            #row[i] = '\"'+entry+'\"'
            #print str(entry)
                #row[i] = psql.extensions.adapt(entry).getquoted()  # FIXME: May not need to adapt() every single GD entry...

    ##        row = tuple(row)
    ##  tuple_string = '"('+ num_args*'%s, '
    ##  tuple_string = tuple_string[:-1] + ')"'

        
    #       sys.exit(1)
        #print num_args
        #print tuple_string

    # These following two numbers should be and are 64, so they match up in length.    
    #    print len(tuple_string.split(', '))
    #    print len(row)#print len(row)

        #sys.exit(1)

    ##    query = ','.join( cursor.mogrify(tuple_string, x) for x in row )
        #print query
            #cursor.execute( 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, ','.join([x for x in header_labels]), ','.join([x for x in row])) )
        # FIXME: May not need to specify the column names for each execution... try it without the first list.  <-- NOT TRUE!
        #cursor.execute( command_str + query )
    #    query = 'INSERT INTO test_table 
    #    cursor.executemany(
        connection.commit()
        db_end_time = time.time()
        logger.info('DB write ops took %.2f seconds.' % (db_end_time - db_start_time))
        
    except psql.DatabaseError, e:
        if connection:
            connection.rollback()  # Fail gracefully, and don't step on table with new data.
        logger.warn('WARNING: Database failed: %s' % e)
    finally:
        if connection:
            connection.close()  # Regardless of outcome, close connection safely.

            
if __name__ == '__main__':
    runit()
