from csv import reader
import subprocess, sys

# Need to find all CSVs in the target directory, then grab each in turn. The columns are
# + email,entry_id,entry_date, and source_url (from which we will splice out the utm_id).

source_dir = '/home/james/reclaim_entries_export_csvs/'
csvs_to_loop_over = subprocess.Popen(['ls', source_dir], stdout=subprocess.PIPE).communicate()[0].split('\n')[:-1]

with open('reclaim_america_entries.csv','w') as output_file:
    output_file.write('Email,"Entry ID","Entry Date","Source URL","UTM ID"\n')
    for c in csvs_to_loop_over:
        doc = reader(open(source_dir+c))
        try:
            doc.next()  # Skipping header
        except StopIteration:
            continue  # Some may have no rows; skip them.
        for row in doc:
            email, entry_id, entry_date, source_url = row
            utm_id = ''
            for piece in source_url.split('&'):
                if 'utm_id' in piece:
                    utm_id = piece.split('=')[1]
            output_file.write(','.join(row + [utm_id]) + '\n')