#!/usr/bin/env python

import sys
import json
import time
import calendar
import ConfigParser
import requests
from datetime import datetime

PD_API_URL = None
PD_API_KEY = None
VIEW = None
DB_URL = None
DB_ID = None
DB_PASSWD = None


def config_parse(settings_file):
    config = ConfigParser.ConfigParser()
    config.read(settings_file)

    global PD_API_URL, PD_API_KEY, VIEW, DB_URL, DB_ID, DB_PASSWD

    PD_API_URL = config.get('PAGER_DUTY_API','URL')
    PD_API_KEY= config.get('PAGER_DUTY_API','KEY')
    VIEW = config.get('VIEWS','UNRESOLVED')
    DB_URL = config.get('DB','URL')
    DB_ID = config.get('DB','ID')
    DB_PASSWD = config.get('DB','PASSWD')


def _do_pagerduty_request(resource, payload=None):
    url = "%s/%s" % (PD_API_URL, '/'.join(resource))
    auth = "Token token=%s" % (PD_API_KEY)
    headers = {"content-type": 'application/json', "Authorization": auth}
    data = dict()
    if payload: data=payload
    data['date_range'] = 'all'
    r = requests.get(url, headers=headers, params=data)
    r.raise_for_status()
    return r.json()


def get_incidents(offset=0, since=None):
    payload = dict()
    if since:
        payload['since'] = since.isoformat()

    payload['sort_by'] = "created_on:asc"
    payload['offset'] = offset
    return _do_pagerduty_request(['incidents'], payload)


def get_count(since=None):
    payload = dict()
    if since:
        payload['since'] = since.isoformat()
    r = _do_pagerduty_request(['incidents', 'count'], payload)
    return int(r['total'])


def get_all_incidents(since=None):
    offset = get_count(since=since)
    fetched = 0

    while True:
        # Get incidents at offset
        incidents = get_incidents(offset=offset,since=since)
        incidents_list = incidents['incidents']

        # yield incidents
        for i in incidents_list:
            yield i

        # Check for more incidents
        fetched += len(incidents['incidents'])
        total = int(incidents['total'])
        limit = int(incidents['limit'])

        print "fetched %d of %d" % (fetched, total)     

        if fetched < total:
            offset -= limit
        else:
            break


def pd_reader(last_run_time): 
    updates = []
    # get unresolved incidents from db view. 
    view = requests.get(VIEW, auth=(DB_ID, DB_PASSWD))
    view = view.json()

    # get incidents that need updating that are unresolved, if any.
    for incident in view['rows']:
        # get matching incident from db
        api_incident = _do_pagerduty_request(['incidents', incident['id'].strip("pd:")])
        api_time = unix_time(api_incident['last_status_change_on'])
        db_time = unix_time(incident['value'])
        if db_time < api_time:
            updates.append("pd:" + str(api_incident['incident_number']))

    num_view_updates = len(updates)

    # get the new incidents created since the last run from API
    last_run_time = datetime.utcfromtimestamp(last_run_time)
    for incident in get_all_incidents(last_run_time):
        incident_id = 'pd:' + str(incident['incident_number'])
        updates.append(incident_id)
    print
    print "unresolved incidents needing update: %s" % num_view_updates
    print "newly created incidents: %s" % (len(updates) - num_view_updates)
    print "total number of updates : %s \n" % len(updates)

    return {'updates':updates, 'num_view_updates':num_view_updates, 
            'num_new_updates':(len(updates) - num_view_updates)}


def get_rev(_id):
    r = requests.head(DB_URL + "/" + _id, auth=(DB_ID, DB_PASSWD))  
    if r.status_code == 200:
        return r.headers['etag'].strip('"')
    else:
        return None
        

def get_duration(incident):
    t_created = unix_time(incident['created_on'])
    t_resolved = unix_time(incident['last_status_change_on'])
    return t_resolved - t_created


def doc_builder(incident_id):
    incident = _do_pagerduty_request(['incidents', incident_id.strip("pd:")])
    doc = dict(incident)
    doc[('_id')] = ('pd:' + str(incident['incident_number']))
    if doc['status'] == "resolved":
        doc['duration'] = get_duration(incident)

    # are we updating an existing or new incident?
    _rev = get_rev(incident_id)
    if _rev:
        doc['_rev'] = _rev

    return doc


def bulk_builder():
    bulk = {'docs':[]}
    ids = set()
    for incident in get_all_incidents():
        num = incident['incident_number']
        if num in ids:
            break
        ids.add(num)
        doc = dict(incident)
        doc['_id'] = ('pd:' + str(num))
        if doc['status'] == "resolved":
            doc['duration'] = get_duration(incident)

        bulk['docs'].append(doc)
    return bulk


def update_last_run(current_run, num_new_updates, num_view_updates=0, updates=None):
    log = dict()
    log['finished_at'] =  calendar.timegm(time.gmtime())
    log['started_at'] = current_run
    log['new_incidents'] = num_new_updates
    log['total_updates'] = num_view_updates + num_new_updates
    log['updates_from_unresolved_view'] = num_view_updates
    if (updates):
        log['updated_incidents'] = updates

    try:
        f = open('pagerflow-log.json', 'r')
        data = json.load(f)
        log['index'] = len(data['history']) + 1
        f.close()
    except:  # if its the first log
        data = dict()
        data['history'] = []
        log['index'] = 1

    f = open('pagerflow-log.json', 'w')
    data['last_run'] = current_run
    data['number_of_executions'] = log['index']
    data['history'].append(log)
    json.dump(data, f)
    f.close()


def get_last_run():
    try:
        with open('pagerflow-log.json', 'r') as json_data:  
            data = json.load(json_data)
            # obtain current state
            last_run = data['last_run']
            json_data.close()
            return int(last_run)
    except:
        return 0


def upload(json_doc):
    headers = {"content-type": "application/json"}
    url = DB_URL+'/_bulk_docs' if 'docs' in json_doc else DB_URL
    resp = requests.post(url, auth=(DB_ID, DB_PASSWD), data=json.dumps(json_doc), headers=headers)
    return resp.status_code in [201, 202]


def unix_time(timestamp):
    return calendar.timegm(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ"))


def main():
    current_run = calendar.timegm(time.gmtime())
    last_run = get_last_run()
    print "last run : %s" % last_run

    if last_run:
        output = pd_reader(last_run)
        for incident_id in output['updates']:
            json_doc = doc_builder(incident_id)
            print "uploading " + str(incident_id),
            if not upload(json_doc):
                sys.exit(1)
        update_last_run(
            current_run, 
            output['num_new_updates'],
            output['num_view_updates'], 
            output['updates']
        )
    else: # initial upload
        if not upload(bulk_builder()):
            sys.exit(1)
        update_last_run(current_run, get_count())
        print "initial ",

    print 'upload complete.'


if __name__=='__main__':
    config_parse(sys.argv[-1])
    main()

