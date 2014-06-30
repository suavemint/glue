import psycopg2 as psql, sys, csv, logging, time
import datetime

## NB: For large files (>131,074 b? B?), set csv.field_size_limit(N) for N > 131074

#pass
heuristics = (lambda value: datetime.strptime(value, '%Y-%m-%d'), int, float)

def check_and_set_type(n):
#    pass
    for type in heuristics:
        try:
            return type(n)
        except ValueError:
            continue
        # If input fails all heuristics, input is defaulted to string
    return n


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.debug('Running script...')
logger.debug('Begin opening CSV/TAB file.')

start_time = time.time()

# First, let's take in a csv file (or a tsv file):
d = ''
f_name = 'FL GOP 050914.csv'
f_stub = f_name.split('.')[0]
#f_name = 'VoterMapping--AR--04-11-2014-HEADERS.tab'

if f_name.lower().endswith('.tab'):
    d = '\t'
if f_name.lower().endswith('.csv'):
    d = ','
    
input_file = csv.reader(open(f_name,'rb'), delimiter=d, quoting=csv.QUOTE_NONE)
header_labels = input_file.next()

print 'There are %i headers in this file, %s.' % (len(header_labels), f_name)
header_max_values = []
header_types = []
table_data = []

#header_header_string = ','.join([x for x in header_labels])

for entry in header_labels:
    index = header_labels.index(entry)
    header_max_values.append(len(entry))
    if '@' in entry:
	header_labels[index] = entry.replace('@','at')
    if entry[:4].isdigit():
        entry = '_' + entry
# We'll start by assuming all inputted vartypes are string, and build from there.
for row in input_file:
    out = ','.join([x for x in row])
#    table_data.append((row))
#    print row, type(row)
    for entry in row:
        index = row.index(entry)
#        if '@' in entry:
#            print 'PROBLEM FOUND WITH INPUT STRING:', entry, 'in row', row
#            row[entry] = entry.replace('@','at')

        length = len(entry)
        if length > header_max_values[index]:
            header_max_values[index] = length
        else:
            continue

finish_time = time.time()
#print header_max_values
#input_file.close()
logger.debug('CSV/TAB file closed.')
logger.debug('Scanning completed.')

print 'Scanning took %i seconds' % (finish_time - start_time)
# 'create schema %s' % schema_name  # Now the table + schema are acessed with 'schema_name.table_name' (or, in SQL, 'database_name.schema_name.table_name').
#
schema_name = 'test_schema'
table_name = 'test_table'
sample_schema_cmd = 'CREATE %s' % schema_name

string_to_add = ''
for x, num in zip(header_labels, header_max_values):
#    print x,num
    string_to_add += '%s varchar(%s), ' % (x, num)

# FIXME: Stop this string-copy nonsense (strs are immutable in py)
# + in removing the terminal comma.
#print 'Query test: %s' % string_to_add
#logger.debug('%s' % string_to_add)
string_short = string_to_add[:-2]
    
# TODO: consider NOT NULL cases, etc. Is this constraint applicable to our case(s)?
#sample_schema = 'CREATE TABLE %s.%s (%s);' % (schema_name, table_name, string_short)
sample_schema = 'CREATE TABLE %s (%s);' % (table_name, string_short)
#print sample_schema

# Now that we have the entire SQL query ready to go, let's dump it in a PSQL DB:
connection = None

try:
    connection = psql.connect(database='testdb',user='james')
    logger.info(connection.autocommit)

    cursor = connection.cursor()

    # For now, we're calling the test table "dump_test"...
    cursor.execute('DROP TABLE IF EXISTS dump_test')
    cursor.execute(sample_schema)
    for d in table_data:
        print d
        cursor.execute( 'INSERT INTO dump_test(%s) VALUES (%s)' % (','.join([x for x in header_labels]), ','.join([x for x in d])) )

    connection.commit()
    
except psql.DatabaseError, e:
    if connection:
        connection.rollback()  # Fail gracefully, and don't step on table with new data.
    logger.warn('WARNING: Database failed: %s' % e)
    print 15*'='
    print ','.join([x for x in header_labels])
    print 15*'='
    #print table_data
    sys.exit(1)
finally:
    if connection:
        connection.close()  # Regardless of outcome, close connection safely.
