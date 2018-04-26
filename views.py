from django.shortcuts import render
from django.db import models as django_models
#from django.contrib.auth.decorators import user_passes_test
#from django.shortcuts import render_to_response

#from django.template import loader
from django.http import HttpResponse
import os, sys, re, csv, json, datetime, time, ckanapi
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

def name_of_resource(resource):
    if 'name' not in resource:
        return "Unnamed resource" # This is how CKAN labels such resources.
    else:
        return resource['name']

def get_package_parameter(site,package_id,parameter=None,API_key=None):
    """Gets a CKAN package parameter. If no parameter is specified, all metadata
    for that package is returned."""
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        if parameter is None:
            return metadata
        else:
            return metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

def get_resource_parameter(site,resource_id,parameter=None,API_key=None):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.resource_show(id=resource_id)
        if parameter is None:
            return metadata
        else:
            return metadata[parameter]
    except:
        raise RuntimeError("Unable to obtain resource parameter '{}' for resource with ID {}".format(parameter,resource_id))

def fuzzy_find_resources(site,package_id,search_term,API_key=None):
    # Try to find the resources given the package ID and search term for the resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    r_candidates = []
    for r in resources:
        if re.search(search_term,r['name'], re.IGNORECASE) is not None:
            r_candidates.append(r)

    return r_candidates,resources
            

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

def remove_data_dictionaries(rs):
    filtered_rs = []
    for r in rs:
        if re.search('ictionary',r['name'], re.IGNORECASE) is None:
            filtered_rs.append(r)
    return filtered_rs

def find_resource_candidates_from_package(site,package,codes,resource_code):
    package_id = package['id']

    if len(codes) == 1:
        # Try to find a unique CSV file in the resources of that package.
        _, resources = fuzzy_find_resources(site,package_id,search_term='nothing',API_key=None)
        r_candidates = []
        for resource in resources:
            if resource['format'] in ['CSV','csv','.csv']: #'XLSX','XLS']
                r_candidates.append(resource)
        if len(r_candidates) == 0:
            print("No CSV resources found meeting the description '{}'.".format(resource_code))
        if len(r_candidates) in [0,1]:
            return r_candidates
        # There are multiple r_candidates. Filter out data dictionaries.
        ## (Suppressing data-dictionary results unless they are the unique results.)
        r_candidates = remove_data_dictionaries(r_candidates)

        if len(r_candidates) == 0:
            print("No CSV resources found meeting the description '{}'.".format(resource_code))
        if len(r_candidates) in [0,1]:
            return r_candidates

        # There are still multiple r_candidates. 
        #print("Here are the {} resources I found: {}".format(len(r_candidates),r_candidates))
        #pprint([r['name'] for r in r_candidates])
        return r_candidates

    elif len(codes) > 1:
        # Use codes[1] to narrow the search.
        found_rs, resources = fuzzy_find_resources(site,package_id,search_term=codes[1],API_key=None)
        print("len(resources) without filtering on codes[1] = {}: {}".format(codes[1],len(resources)))

        print("len(found_rs) after filtering on codes[1] = {}: {}".format(codes[1],len(found_rs)))

        # possibly common block #
        r_candidates = []
        for resource in found_rs:
            if resource['format'] in ['CSV','csv','.csv']: #'XLSX','XLS']
                r_candidates.append(resource)
        if len(r_candidates) == 0:
            print("No CSV resources found meeting the description '{}'.".format(resource_code))
        if len(r_candidates) in [0,1]:
            return r_candidates
        # There are multiple r_candidates. Filter out data dictionaries.
        ## (Suppressing data-dictionary results unless they are the unique results.)
        r_candidates = remove_data_dictionaries(r_candidates)

        if len(r_candidates) == 0:
            print("No CSV resources found meeting the description '{}'.".format(resource_code))
        if len(r_candidates) in [0,1]:
            return r_candidates
        # end possibly common block #


        # There are still multiple r_candidates. Use the third part of the code:
        #print("Here are the {} resources I found: {}".format(len(r_candidates),r_candidates))
        #pprint([r['name'] for r in r_candidates])
        return r_candidates



def decode(resource_code,site,API_key):
    # Try to find one unique CKAN table described by the given resource_code.
    try:
        resource_id = get_resource_parameter(site,resource_code,'id',API_key)
        assert resource_id == resource_code
        return resource_id
    except:
        # Decode the resource code, which is encoded according to this
        # human-readable schema:
        #   <package-name search term>.<resource-name search term>[.<relative month selector>]
        # Example:
        #   BigBurgh.services.0
        codes = resource_code.split('.')
        package_cache = {'bigburgh': '6e683016-0b93-4776-9d9f-caa03e94edc2'}
        p_candidates = []
        if codes[0].lower() in package_cache.keys():
            package_id = package_cache[codes[0]]
        else:
            ckan = ckanapi.RemoteCKAN(site) # Without specifying the apikey field value,
            # the next line will only return non-private packages.
            #packages = ckan.action.current_package_list_with_resources(limit=999999)
            #for p in found_packages:
            #    if re.search(codes[0],p['title']) is not None:
            #        p_candidates.append(p)
            found_packages = ckan.action.package_search(q='title:{}'.format(codes[0]))['results']
            # Search packages and find the ones that have codes[0] in their titles.
            pprint([fp['title'] for fp in found_packages])

            p_candidates = found_packages
            if len(p_candidates) == 0:
                print("No packages containing '{}' found.".format(codes[0]))
                return []
            all_potentials = []
            for package in p_candidates:
                r_candidates = find_resource_candidates_from_package(site,package,codes,resource_code)
                all_potentials += r_candidates
                print("len(r_candidates) = {}, len(all_potentials) = {}".format(len(r_candidates), len(all_potentials)))

            if len(all_potentials) == 0:
                return []
            if len(all_potentials) == 1:
                return all_potentials[0]['id']

            pprint([r['name'] for r in all_potentials])
            return all_potentials
            #r_candidates, resources = fuzzy_find_resources(site,package_id,search_term=codes[1],API_key=None)

def get_resource_stuff(site,resource_code,API_key):
    resource_id = decode(resource_code,site,API_key)
    if type(resource_id) == list:
        return None, None, None, None, [r['name'] for r in resource_id]
    data, schema = get_all_records(site,resource_id,API_key=API_key,chunk_size=5000)
    
    metadata = get_resource_parameter(site,resource_id,None,API_key)

    package_id = metadata['package_id']
    package_title = get_package_parameter(site,package_id,'title',API_key)
    resource_name = name_of_resource(metadata)
    package_url_path = "/dataset/" + get_package_parameter(site,package_id,'name',API_key)
    #package_url = site + package_url_patmetadatah
    resource_url_path = package_url_path + "/resource/" + resource_id
    resource_url = site + resource_url_path
    
    data_dict = {'resource_name': metadata['name'], 'resource_id': resource_id,
            'package_name': package_title, 'resource_url': resource_url,
            'type': 'resource'} # 'type' could also have the value "file"

    return data, schema, data_dict, [resource_id]

def remove_fields(data,schema,to_remove):
    new_schema = []
    for d in schema: # The pieces look like this: {'id': '_id', 'type': 'int4'}
        if d['id'] not in to_remove:
            new_schema.append(d)

    new_data = []
    for datum in data:
        new_datum = {}
        for k,v in datum.items():
            if k not in to_remove:
                new_datum[k] = v
        new_data.append(new_datum)

    new_field_names = [a['id'] for a in new_schema]
    return new_data, new_schema, new_field_names

def compare(request,resource_code_1=None,resource_code_2=None):
    # Get the resources.
    site, API_key, _, _ = activate_wormhole(SETTINGS_FILE,SERVER)

    # [ ] Start thinking about refactoring this to make one or both of these 
    #     uploaded CSV files.

    data1, schema1, data_dict_1, candidate_r_ids1 = get_resource_stuff(site,resource_code_1,API_key)
    data2, schema2, data_dict_2, candidate_r_ids2 = get_resource_stuff(site,resource_code_2,API_key)
 
    to_remove = ['_id', 'year_month'] # Eventually make this user-specified.
    data1, schema1, field_names1 = remove_fields(data1,schema1,to_remove)
    data2, schema2, field_names2 = remove_fields(data2,schema2,to_remove)


    context = { 'candidate_r_ids1': candidate_r_ids1,
                'candidate_r_ids2': candidate_r_ids2,
                }
    if data1 is None and data2 is None:
        context['error'] = 'Neither {} nor {} could be found.'.format(resource_code_1,resource_code2)
    elif data1 is None:
        context['error'] = 'No file could be found under code "{}".'.format(resource_code_1)
    elif data2 is None:
        context['error'] = 'No file could be found under code "{}".'.format(resource_code_2)
    else:
        pprint(data_dict_1)

        # Compare things using difflib (or whatever).
        field_table = difflib.HtmlDiff(wrapcolumn=60).make_table(
            fromlines=field_names1,
            tolines=field_names2,
            fromdesc='File 1 fields',
            todesc='File 2 fields')

        s1 = list(field_names1)
        s2 = list(field_names2)
        fn1, fn2 = list(field_names1), list(field_names2) # These should be just the kept and renamed fields.
        matcher = difflib.SequenceMatcher(None, field_names1, field_names2)
        identical_fn = True
        for tag, i1, i2, j1, j2 in reversed(matcher.get_opcodes()):
            if tag == 'delete': # A column was deleted.
                print('Remove {} from positions [{}:{}]'.format(s1[i1:i2], i1, i2))
                del s1[i1:i2]
                del fn1[i1:i2]
                identical_fn = False

            elif tag == 'equal':
                print('The sections [{}:{}] of s1 and [{}:{}] of s2 are the same'.format(i1, i2, j1, j2))

            elif tag == 'insert': # A column was added.
                print('Insert {} from [{}:{}] of s2 into s1 at {}'.format(s2[j1:j2], j1, j2, i1))
                s1[i1:i2] = s2[j1:j2]
                del fn2[j1:j2]
                identical_fn = False

            elif tag == 'replace': # The field names were just changed
                print('Replace {} from [{}:{}] of s1 with {} from [{}:{}] of s2'.format(s1[i1:i2], i1, i2, s2[j1:j2], j1, j2))
                s1[i1:i2] = s2[j1:j2]
                identical_fn = False

        diff_table = OrderedDict([])

        for f1,f2 in zip(fn1,fn2):
            column1 = [str(d[f1]) for d in data1] # We need to cast values to strings so that
            column2 = [str(d[f2]) for d in data2] # difflib.HtmlDiff can operate on them.
            diff_table[f1] = difflib.HtmlDiff(wrapcolumn=60).make_table(fromlines=column1,
                    tolines=column2,
                    fromdesc='Resource 1: {}'.format(f1),
                    todesc='Resource 2: {}'.format(f2),
                    context=True,
                    numlines=2,
                    )

        # Synthesize lists of data with only the selected (kept or renamed) fields.
        d1, d2 = [], []
        for row in data1:
            d1_row = {k:v for (k,v) in row.items() if k in fn1}
            delta_d1 = OrderedDict([(k,v) for (k,v) in row.items() if k in fn1])
            d1_flat = ','.join([str(row[f1]) for f1 in fn1])
            d1.append(d1_flat)
        for row in data2:
            d2_flat = ','.join([str(row[f2]) for f2 in fn2])
            d2.append(d2_flat)


        flat_table = difflib.HtmlDiff(wrapcolumn=60).make_table(fromlines=d1,
                tolines=d2,
                fromdesc='File 1',
                todesc='File 2',
                context=True,
                numlines=2,
                )


        context = {'data_dict_1': data_dict_1, 'data_dict_2': data_dict_2, 
                'identical_fn': identical_fn,
                'rows1': len(data1),
                'columns1': len(field_names1),
                'rows2': len(data2),
                'columns2': len(field_names2),
                'field_table': field_table,
                'diff_table': diff_table,
                'flat_table': flat_table,
                'candidate_r_ids1': candidate_r_ids1,
                'candidate_r_ids2': candidate_r_ids2,
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
