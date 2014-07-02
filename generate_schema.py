import psycopg2 as psql, sys, csv, logging, time
import datetime

## NB: For large files (>131,074 b? B?), set csv.field_size_limit(N) for N > 131074
def generate_schema(i):

    import csv, logging, time

    schema_start_time = time.time()

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
#    header_labels = header_labels.split(',')
#    print header_labels[:-1]
#    print 15*'<'
#    print header_labels[:-1]
#    print 15*'<'
    trimmed_end = header_labels[-1].replace('\r\n','')
    header_labels[-1] = trimmed_end
    #print header_labels
	#file_in = csv.reader(in_file, delimiter=d, quoting=csv.QUOTE_NONE)
    #header_labels = input_file.next()
    #print header_labels
#    print header_labels    
#    print 'There are %i columns in file %s.' % (len(header_labels), f_name)
#    print d
    header_string = ','.join([x for x in header_labels])

    # Clean the column titles.
    for entry in header_labels:
        index = header_labels.index(entry)
        header_max_values.append(len(entry))
        # PostgreSQL does not like "@" as an initial char in a column name, as that's the cursor.
        if '@' in entry:
            header_labels[index] = entry.replace('@','at')

    # Loop over rows in csv.
    for row in csv_file:
#        print row
        for entry in row:
            length = len(entry)
            if length > header_max_values[index]:
                header_max_values[index] = length
            else:
                continue
#	in_file.close()

	# Generate the outputted string, which will contain the variable type and char-limit for each column.
    string_to_add = ''
    
    for x, num in zip(header_labels, header_max_values):
        string_to_add += '%s varchar(%s), ' % (x, num)
    schema_end_time = time.time()
    print 'Schema generation took %.2f seconds.' % (schema_start_time - schema_end_time)

    return header_string, string_to_add
## end generate_schema


heuristics = (lambda value: datetime.strptime(value, '%m-%d-%Y'), int, float)

def check_and_set_type(n):
#    pass
    for type in heuristics:
        try:
            return type(n)
        except ValueError:
            continue
        # If input fails all heuristics, input is defaulted to string
    return n

if __name__ == '__main__':

	logging.basicConfig(level=logging.DEBUG)
	logger = logging.getLogger(__name__)
	logger.debug('Running script...')
	logger.debug('Begin opening CSV/TAB file.')


	d = ''
	f_name = 'FL GOP 050914.csv'
	#f_name = 'clipped.csv'
	#f_stub = f_name.split('.')[0].replace(' ','_')
	#f_name = 'VoterMapping--AR--04-11-2014-HEADERS.tab'

	# Open the input file once, and output the schema.
	schema_string, header_labels = generate_schema(f_name)    

	schema_name = 'test_schema'
	table_name = 'test_table'
	sample_schema_cmd = 'CREATE %s' % schema_name

		
	# TODO: consider NOT NULL cases, etc. Is this constraint applicable to our case(s)?
	#sample_schema = 'CREATE TABLE %s.%s (%s);' % (schema_name, table_name, string_short)
	sample_schema = 'CREATE TABLE %s (%s);' % (table_name, schema_string[:-2])

	# Now that we have the entire SQL query ready to go, let's dump it in a PSQL DB:
	connection = None

	logger.info('Starting DB ops.')

	try:
		connection = psql.connect(database='testdb',user='postgres',password='password')
		cursor = connection.cursor()

		num_args = len(row)
		
		cursor.execute('DROP TABLE IF EXISTS %s' % table_name)
		cursor.execute(sample_schema)

		command_str = 'INSERT INTO %s VALUES' % table_name

		#header = ','.join([x for x in header_labels])

		db_start_time = time.time()

		# Open the input file again, skipping header row, and write-in each row.
		with open(f_name,'rb') as f:
			input_file = csv.reader(f, delimiter=d, quoting=csv.QUOTE_NONE)
			input_file.next()
			for row in input_file:
				i = row.index(entry)
				for entry in row:
					row[i] = psql.extensions.adapt(entry).getquoted()
				values = ','.join([z for z in row])
				try:
					cursor.execute( 'INSERT INTO %s(%s) VALUES (%s)' % (table_name, header, values) )
				except Exception, e:
					print 'Error!', e
					print header, '\n\n', row    
		#for row in table_data:
	#        print row
	#        print 'Length of row items:', len(row)
	#        for entry in row:
	#   	    print entry
				#if entry == '' or entry == ' ':
				#    row[i] = None    
	##            if ' ' in entry and entry is not ' ':
	##                row[i] = psql.extensions.adapt(entry).getquoted()
			#row[i] = '\"'+entry+'\"'
			#print str(entry)
				#row[i] = psql.extensions.adapt(entry).getquoted()  # FIXME: May not need to adapt() every single GD entry...

	##        row = tuple(row)
	##	tuple_string = '"('+ num_args*'%s, '
	##	tuple_string = tuple_string[:-1] + ')"'

		
	#	    sys.exit(1)
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


