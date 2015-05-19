from __future__ import print_function
from optparse import OptionParser
from datetime import datetime
import couchdb, requests, json

def main():
    db=None
    idList = []
    data = []
    db_addr = None
    db_name = None
    db_view = None
    time1 = None
    time2 = None

    usage = "usage: Analyser -a db_address -n db_name"
    parser = OptionParser(usage)
    parser.add_option("-a","--address", type="string", dest="addr", help="DB Address")
    parser.add_option("-n","--name", type="string", dest="name", help="DB Name")
    parser.add_option("-v","--view", type="string", dest="view", help="DB View")

    opts,args = parser.parse_args()
    if opts.addr:
        db_addr=opts.addr
    if opts.name:
        db_name=opts.name
    if opts.view:
        db_view=opts.view

    couch = couchdb.Server(db_addr)
    db=couch[db_name]

    time1 = datetime.now()
    view = db.view(db_view,group='true')
    for row in view:
	content = {
	    'total': row.value,
	    'word': row.key
    	}
	data.append(content)
    data.sort(reverse=True)
    ctr=0
    for x in data:
	ctr+=1
        print(x)
	if(ctr==100):
	    break
    time2=datetime.now()
    #print ('Finish - Time Spent:',time2-time1)

'''
def couchdb_pager(db, view_name='_all_docs',
                  startkey=None, startkey_docid=None,
                  endkey=None, endkey_docid=None, bulk=5000):
    # Request one extra row to resume the listing there later.
    options = {'limit': bulk + 1}
    if startkey:
        options['startkey'] = startkey
        if startkey_docid:
            options['startkey_docid'] = startkey_docid
    if endkey:
        options['endkey'] = endkey
        if endkey_docid:
            options['endkey_docid'] = endkey_docid
    done = False
    while not done:
        view = db.view(view_name, **options)
        rows = []
        # If we got a short result (< limit + 1), we know we are done.
        if len(view) <= bulk:
            done = True
            rows = view.rows
        else:
            # Otherwise, continue at the new start position.
            rows = view.rows[:-1]
            last = view.rows[-1]
            options['startkey'] = last.key
            options['startkey_docid'] = last.id

        for row in rows:
            yield row.id
'''
if __name__ == "__main__":
    main()
