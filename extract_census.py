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


# Break up the rows we are storing to reduce memory usage
sumlevel_groups = [
    [40,50,60,160,400,950,960,970,610,620,500],
    [140], # Tracts
    [150]  # Blockgroups
]
    


def write_rows(sumlevel, table_name, p, rows):
    
        file_name = "{}/{}/{}/{}.csv".format(year, release, table_name, sumlevel)
        
        with s3.open(file_name, 'wb') as f:
            w = csv.writer(f)
            w.writerow([ unicode(c.name) for c in p.table.columns])
            for row in rows:
                w.writerow(row)

part_no = 1
for p in b.partitions:
    
    table_name = p.table.name

    print 'Loading: ', year, release, table_name
    p.localize()

    for sumlevels in sumlevel_groups:
        
        rows = defaultdict(list)
        
        for row in p:
            if row.sumlevel in sumlevels:
                rows[row.sumlevel].append(row.values())
                
        for sumlevel in sorted(rows.keys()):
            sl_rows = rows[sumlevel]
            part_no += 1
            print 'Writing ', part_no,  sumlevel, table_name, len(sl_rows)
            write_rows(sumlevel, table_name, p, sl_rows)
        
    
    