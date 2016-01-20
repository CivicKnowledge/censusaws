#!/usr/bin/env python 
"""Write CSV extracts to a remote"""
import ambry
from collections import defaultdict
import csv
import os
from fs.s3fs import S3FS


year = 2014 # TODO Make argument
release = 5 # TODO Make into an argument

s3 = S3FS(
    bucket='s3://extracts.census.civicknowledge.com', # TODO Make into an argument
    prefix='/', #todo make into an argument
    aws_access_key=os.environ.get('AWS_ACCESS_KEY'),
    aws_secret_key=os.environ.get('AWS_SECRET_KEY')

)

l = ambry.get_library()
b = l.bundle('census.gov-acs-p{}ye{}'.format(release, year))

for p in b.partitions:
    
    rows = defaultdict(list)
    
    table_name = p.table.name

    print 'Loading: ', year, release, table_name
    p.localize()
    
    for i, row in enumerate(p):
        rows[row.sumlevel].append(row.values())
    
    for i, sumlevel in enumerate(sorted(rows.keys())):
        sl_rows = rows[sumlevel]

        file_name = "{}/{}/{}/{}.csv".format(year, release, table_name, sumlevel)
        print 'Writing ', i, file_name, len(sl_rows)
        
        with s3.open(file_name, 'wb') as f:
            w = csv.writer(f)
            print [ unicode(c.name) for c in p.table.columns]
            w.writerow([ unicode(c.name) for c in p.table.columns])
            for row in sl_rows:
                w.writerow(row)