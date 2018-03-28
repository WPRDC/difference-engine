from django.shortcuts import render
from django.db import models as django_models
#from django.contrib.auth.decorators import user_passes_test
#from django.shortcuts import render_to_response

#from django.template import loader
from django.http import HttpResponse
import os, sys, csv, json, datetime, time, ckanapi
from pprint import pprint
from collections import OrderedDict

import difflib

from difference_engine.parameters.local_parameters import DIFFERENCE_ENGINE_SETTINGS_FILE as SETTINGS_FILE, SERVER

def get_number_of_rows(site,resource_id,API_key=None):
# This is pretty similar to get_fields and DRYer code might take
# advantage of that.

# On other/later versions of CKAN it would make sense to use
# the datastore_info API endpoint here, but that endpoint is
# broken on WPRDC.org.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=1) # The limit
        # must be greater than zero for this query to get the 'total' field to appear in
        # the API response.
        count = results_dict['total']
    except:
        return None

    return count

def get_resource_data_and_schema(site,resource_id,API_key=None,count=50,offset=0,selected_fields=None):
    # Use the datastore_search API endpoint to get <count> records from
    # a CKAN resource starting at the given offset and only returning the
    # specified fields in the given order (defaults to all fields in the
    # default datastore order).
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    if selected_fields is None:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset)
    else:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset, fields=selected_fields)
    # A typical response is a dictionary like this
    #{u'_links': {u'next': u'/api/action/datastore_search?offset=3',
    #             u'start': u'/api/action/datastore_search'},
    # u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'limit': 3,
    # u'records': [{u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}],
    # u'resource_id': u'd1e80180-5b2e-4dab-8ec3-be621628649e',
    # u'total': 88232}
    data = response['records']
    schema = response['fields']
    return data, schema


def get_all_records(site,resource_id,API_key=None,chunk_size=5000):
    all_records = []
    first_schema = None
    failures = 0
    k = 0
    offset = 0 # offset is almost k*chunk_size (but not quite)
    row_count = get_number_of_rows(site,resource_id,API_key)
    if row_count == 0: # or if the datastore is not active
       print("No data found in the datastore.")
       success = False
    while len(all_records) < row_count and failures < 5:
        time.sleep(0.1)
        try:
            records,schema = get_resource_data_and_schema(site,resource_id,API_key,chunk_size,offset)
            if first_schema is None:
                first_schema = schema
            if records is not None:
                all_records += records
            failures = 0
            offset += chunk_size
        except:
            failures += 1

        # If the number of rows is a moving target, incorporate
        # this step:
        #row_count = get_number_of_rows(site,resource_id,API_key)
        k += 1
        print("{} iterations, {} failures, {} records, {} total records".format(k,failures,len(records),len(all_records)))

        # Another option for iterating through the records of a resource would be to
        # just iterate through using the _links results in the API response:
        #    "_links": {
        #  "start": "/api/action/datastore_search?limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1",
        #  "next": "/api/action/datastore_search?offset=5&limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"
        # Like this:
            #if r.status_code != 200:
            #    failures += 1
            #else:
            #    URL = site + result["_links"]["next"]

        # Information about better ways to handle requests exceptions:
        #http://stackoverflow.com/questions/16511337/correct-way-to-try-except-using-python-requests-module/16511493#16511493

    return all_records, first_schema

def get_package_parameter(site,package_id,parameter,API_key=None):
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

    return desired_string

def find_resource_id(site,package_id,resource_name,API_key=None):
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def activate_wormhole(filepath='parameters/settings.json',server='stage'):
    # Get parameters to communicate with a CKAN instance
    # from the specified JSON file.
    with open(filepath) as f:
        settings = json.load(f)
        API_key = settings["loader"][server]["ckan_api_key"]
        site = settings["loader"][server]["ckan_root_url"]
        package_id = settings["loader"][server]["package_id"]

    return site, API_key, package_id, settings

def compare(request,resource_id_1=None,resource_id_2=None):
    # Get the resources.
    site, API_key, _, _ = activate_wormhole(SETTINGS_FILE,SERVER)
    data1, schema1 = get_all_records(site,resource_id_1,API_key=API_key,chunk_size=5000)
    data2, schema2 = get_all_records(site,resource_id_2,API_key=API_key,chunk_size=5000)
    # Compare the field names in the schema:
    #    [ ] What about situations where just field names have changed?
    field_names1 = [a['id'] for a in schema1]
    field_names2 = [a['id'] for a in schema2]

    # Compare things using difflib (or whatever).
    field_table = difflib.HtmlDiff().make_table(
        fromlines=field_names1,
        tolines=field_names2,
        fromdesc='Resource 1 fields',
        todesc='Resource 2 fields')
    one_but_not_two = list(set(field_names1) - set(field_names2))
    two_but_not_one = list(set(field_names2) - set(field_names1))

    # [ ] This set-difference approach does a poor job of taking 
    # advantage of the orderedness of the fields and the difflib capabilities.


    s1 = list(field_names1)
    s2 = list(field_names2)
    fn1, fn2 = list(field_names1), list(field_names2) # These should be just the kept and renamed fields.
    matcher = difflib.SequenceMatcher(None, field_names1, field_names2)
    for tag, i1, i2, j1, j2 in reversed(matcher.get_opcodes()):
        if tag == 'delete': # A column was deleted.
            print('Remove {} from positions [{}:{}]'.format(s1[i1:i2], i1, i2))
            del s1[i1:i2]
            del fn1[i1:i2]

        elif tag == 'equal':
            print('The sections [{}:{}] of s1 and [{}:{}] of s2 are the same'.format(i1, i2, j1, j2))

        elif tag == 'insert': # A column was added.
            print('Insert {} from [{}:{}] of s2 into s1 at {}'.format(s2[j1:j2], j1, j2, i1))
            s1[i1:i2] = s2[j1:j2]
            del fn2[j1:j2]

        elif tag == 'replace': # The field names were just changed
            print('Replace {} from [{}:{}] of s1 with {} from [{}:{}] of s2'.format(s1[i1:i2], i1, i2, s2[j1:j2], j1, j2))
            s1[i1:i2] = s2[j1:j2]

    pprint([[x,y] for x,y in zip(fn1,fn2)])

    diff_table = OrderedDict([])

    for f1,f2 in zip(fn1,fn2):
        column1 = [str(d[f1]) for d in data1] # We need to cast values to strings
        column2 = [str(d[f2]) for d in data2] # so that difflib can operate on them.
        diff_table[f1] = difflib.HtmlDiff().make_table(fromlines=column1,
                tolines=column2,
                fromdesc='Resource 1: {}'.format(f1),
                todesc='Resource 2: {}'.format(f2),
                context=True,
                numlines=2,
                )

    context = {'thing1': resource_id_1, 'thing2': resource_id_2, 'schema1': schema1, 'schema2': schema2,
            'field_table': field_table,
            'diff_table': diff_table,
            'one_but_not_two': one_but_not_two, 'two_but_not_one': two_but_not_one,
            }
    return render(request, 'difference_engine/results.html', context)


def index(request):
    context = { }
    return render(request, 'difference_engine/index.html', context)
    #template = loader.get_template('index.html')

    #return HttpResponse(template.render(context, request))

# Actually just use the simpleisbetterthancomplex.com 
# examples after pip-installing django-import-export.

# The following should serve an admin-only page
#@user_passes_test(lambda u: u.is_staff)
#def UploadFileView(request, *args, **kwargs):
#
## import CSV file
#    for line in csv_file:
#        # Parse line into fields
#        # Build row out of fields
#        fd = fire_department(name = ,
#            street_address = ,
#    
#    return render_to_response("admin/base_form.html", {'form_text': form_text},context_instance=RequestContext(request))
