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
    bucket='extracts.census.civicknowledge.com', # TODO Make into an argument
    prefix='/', #todo make into an argument
    aws_access_key=os.environ.get('AWS_ACCESS_KEY'),
    aws_secret_key=os.environ.get('AWS_SECRET_KEY')

)

# Dealth with some stupid bug .... 
import ssl
_old_match_hostname = ssl.match_hostname

def _new_match_hostname(cert, hostname):
    if hostname.endswith('.s3.amazonaws.com'):
        pos = hostname.find('.s3.amazonaws.com')
        hostname = hostname[:pos].replace('.', '') + hostname[pos:]
    return _old_match_hostname(cert, hostname)

ssl.match_hostname = _new_match_hostname

l = ambry.get_library()
b = l.bundle('census.gov-acs-p{}ye{}'.format(release, year))


sumlevels = [40,50,60,160,400,140,150,950,960,970,610,620,500]

part_no = 1
for p in b.partitions:
    
    rows = defaultdict(list)
    
    table_name = p.table.name

    print 'Loading: ', year, release, table_name
    p.localize()

    
    for i, row in enumerate(p):
        if row.sumlevel in sumlevels:
            rows[row.sumlevel].append(row.values())
    
    for i, sumlevel in enumerate(sorted(rows.keys())):
        sl_rows = rows[sumlevel]

        file_name = "{}/{}/{}/{}.csv".format(year, release, table_name, sumlevel)
        print 'Writing ', part_no, i, file_name, len(sl_rows)
        part_no += 1
        
        with s3.open(file_name, 'wb') as f:
            w = csv.writer(f)
            w.writerow([ unicode(c.name) for c in p.table.columns])
            for row in sl_rows:
                w.writerow(row)