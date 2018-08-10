#!/usr/bin/python

# vi /var/log/httpd/alatis/
# apachectl -k graceful
""" Power the ALATIS server. """

from __future__ import print_function

# Standard imports
import os
import re
import sys
import datetime
from io import BytesIO
from random import randint
from shutil import copyfile
from zipfile import ZipFile
from time import gmtime, strftime

# Installed packages
import psycopg2
from psycopg2.extras import DictCursor
from werkzeug.utils import secure_filename
from flask import Flask, request, Response, jsonify, redirect, send_file, render_template, send_from_directory, url_for

# Set up paths for imports and such
local_dir = os.path.dirname(__file__)
os.chdir(local_dir)
sys.path.append(local_dir)
import bmrb

# Set up the flask application
application = Flask(__name__)

# Local folder locations
UPLOAD_FOLDER = '/websites/alatis/upload/'
BIN_PATH = '/websites/alatis/distrib/alatis_v4'
INCHI1_PATH = '/websites/alatis/distrib/inchi-1'
application.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def get_postgres_connection(user='web', database='webservers', host="pinzgau.nmrfam.wisc.edu",
                            dictionary_cursor=False):
    """ Returns a connection to postgres and a cursor."""

    if dictionary_cursor:
        conn = psycopg2.connect(user=user, database=database, host=host, cursor_factory=DictCursor)
    else:
        conn = psycopg2.connect(user=user, database=database, host=host)
    cur = conn.cursor()
    cur.execute("SET search_path TO alatis, public")

    return conn, cur


def process_a_folder(folder, submission_id, fname):
    fin = open(fname, 'r')
    tag_name = ''
    var_dict = {'input': '',
                'complete_inchi': ''}
    for a_line in fin:
        a_line = a_line.replace('\n', '')
        if '>  <ALATIS_' in a_line:
            if tag_name != '':
                if info != '':
                    var_dict[tag_name] = info
            a_line = a_line.replace('>  <ALATIS_', '')
            tag_name = a_line.replace('>', '')
            info = ''
        else:
            if a_line != '':
                if info == '':
                    info = a_line
                elif '$$$$' not in a_line:
                    info += '\n' + a_line
        if '$$$$' in a_line:
            if tag_name != '':
                var_dict[tag_name] = info
    fin.close()

    if var_dict['complete_inchi'] != '' and var_dict['complete_inchi'] != '.':
        inchi_fname = os.path.join(folder, submission_id, var_dict['complete_inchi'].replace('./', ''))
        if os.path.exists(inchi_fname):
            with open(inchi_fname, 'r') as fin:
                inchi_string = fin.readline()
            var_dict['InChI_string'] = inchi_string
            fin.close()
        else:
            var_dict['InChI_string'] = ''
    else:
        var_dict['InChI_string'] = ''
    return var_dict


@application.route('/pubchem/<pubchem_id>/dl/<ftype>')
def pubchem_getfile(pubchem_id, ftype):
    """ Get the molfile location, send it. """

    mime_types = {'input': "chemical/mdl-molfile",
                  'output': "chemical/mdl-molfile",
                  'pdb': "chemical/x-pdb",
                  'xyz': "chemical/x-xyz",
                  'inchi': "chemical/x-inchi",
                  'map': "text/plain"}
    extensions = {'map': 'txt', 'input': 'sdf', 'output': 'sdf'}
    if ftype not in mime_types:
        return "Invalid request!"

    # Get the data the standard way
    info, var_dict = get_var_dict(pubchem_id)
    if not info:
        return 'No such entry - or no data of specified type for entry.'

    # Prepare the response
    response = Response(info[ftype], mimetype=mime_types[ftype])
    # Choose the name based on the file type (those without specified
    #  extensions are themselves the extension
    if ftype in extensions:
        fname = "_%s.%s" % (ftype, extensions[ftype])
    else:
        fname = ".%s" % ftype
    # Set download file name
    response.headers["content-disposition"] = 'attachment; filename="PubChem_CID%s%s"' % (pubchem_id, fname)
    return response


@application.route('/pubchem/<pubchem_id>/dl/zip')
def pubchem_get_zip_file(pubchem_id):
    """ compress and return zip file. """

    # Get the pubchem info
    info, var_dict = get_var_dict(pubchem_id)
    if not info:
        return 'No such entry - or no data of specified type for entry.'

    # Make a new zip file in memory, writing to a BytesIO object
    memory_file = BytesIO()
    zfile = ZipFile(memory_file, 'w')

    # Write the various files
    zfile.writestr('input.sdf', info['input'])
    zfile.writestr('alatis_output.sdf', info['output'])
    zfile.writestr('inchi.inchi', info['inchi'])
    zfile.writestr('warnings.txt', info['warning'])
    zfile.writestr('error.txt', info['error'])
    zfile.writestr('alatis_output.pdb', info['pdb'])
    zfile.writestr('alatis_output.xyz', info['xyz'])
    zfile.writestr('mixture_map.txt', info['mixture_map'])
    zfile.writestr('map.txt', info['map'])
    meta_data = """PubChem CID,{id}
ALATIS formula,{formula}
InChI,"{inchi}"
PubChem molecular formula,{molecular_formula}
PubChem molecular weight,{molecular_weight}
PubChem mass,{mass}
""".format(**info)
    zfile.writestr('meta_data.csv', meta_data)

    comment = """Data downloaded from ALATIS server %s.
To view entry: %spubchem/%s""" % (strftime("%Y-%m-%d %H:%M:%S", gmtime()),
                                  request.url_root, pubchem_id.encode('ascii'))
    zfile.comment = comment.encode()

    # Close the file
    zfile.close()

    # Seek to the beggining of the memory file
    memory_file.seek(0)

    # Send the result
    return send_file(memory_file,
                     attachment_filename="ALATIS_PUBCHEM_ID%s_%s.zip" % (pubchem_id,
                                                                         strftime("%a%d%b%Y%H%M%S", gmtime())),
                     as_attachment=True)


def get_var_dict(pubchem_id, with_names=False):
    """ Get information about the pubchem entry. """

    # Get the postgres connection
    conn, cur = get_postgres_connection(dictionary_cursor=True)
    # Make the SQL query for the information for this ID
    cur.execute(
        '''SELECT * FROM compound_alatis AS ca  LEFT JOIN compound_metadata AS cm on ca.id = cm.id  WHERE cm.id=%s;''',
        [pubchem_id])

    # Fetch the result
    info = cur.fetchone()

    # Access the data you need from info['key']
    # Valid keys are:
    #  id,formula,input,map,warning,output,error,inchi,pdb,xyz,mixture_map,id,molecular_formula,molecular_weight,mass
    var_dict = {}
    if info is None:
        var_dict['ID'] = 'None'
    else:
        cid_str = str(info['id'])
        var_dict['ID'] = cid_str + '/'
        var_dict['PubChem_CID'] = info['id']
        var_dict['formula'] = info['formula']

        # Downloadable files
        var_dict['input'] = 'dl/input'  # info['input']
        var_dict['output_mol'] = 'dl/output'  # info['output']
        var_dict['output_map'] = 'dl/map'  # info['map']
        var_dict['output_pdb'] = 'dl/pdb'
        var_dict['complete_inchi'] = 'dl/inchi'
        var_dict['output_xyz'] = 'dl/xyz'
        var_dict['zfile'] = 'dl/zip'

        if info['warning'].strip() != '':
            var_dict['Warnings'] = info['warning']
        if info['error'].strip() != '':
            var_dict['Errors'] = info['error']
        var_dict['InChI_string'] = info['inchi']

        # var_dict['mixture_map'] = info['mixture_map']
        var_dict['molecular_formula'] = info['molecular_formula']
        var_dict['molecular_weight'] = info['molecular_weight']
        var_dict['mass'] = info['mass']

        # Cleanup the database info where needed
        info['input'] = info['input'][1:]
        info['inchi'] = info['inchi'].strip()

    # Only do the extra query if they want the names
    if with_names:
        cur.execute('''SELECT name FROM compound_name WHERE id=%s ORDER BY seq ASC;''', [pubchem_id])
        names = [x['name'] for x in cur.fetchall()]
        var_dict['names'] = names[1:]
        try:
            var_dict['best_name'] = names[0]
        except IndexError:
            var_dict['best_name'] = "Compound Name Unknown"

    return info, var_dict


@application.route('/pubchem/<pubchem_id>')
def pubchem(pubchem_id):
    """ Search the DB. """

    info, var_dict = get_var_dict(pubchem_id, with_names=True)
    if info is None:
        var_dict = {'pubchem_id': pubchem_id}
        return render_template("pubchem_notfound.html", **var_dict)
    else:
        cur = get_postgres_connection(dictionary_cursor=True)[1]
        cur.execute('''SELECT id,db,data_path FROM search_terms WHERE termname='InChI' AND term=%s;''', [info['inchi']])

        for item in cur.fetchall():
            if 'related' not in var_dict:
                var_dict['related'] = []
            var_dict['related'].append({'id': item['id'], 'db': item['db'],
                                        'data_path': item['data_path']})

        return render_template("submission_output.html", **var_dict)


@application.route('/upload/<submission_id>')
def submissions(submission_id):
    zpath = os.path.join("/websites/alatis/upload/", submission_id, "outputs.zip")

    if os.path.exists(zpath) and (os.stat(zpath)).st_size > 1:  # HTCondor returned outputs.zip
        zfile = ZipFile(zpath, 'r')
        zfile.extractall(os.path.join("/websites/alatis/upload/", submission_id))
        zfile.close()

        display_path = os.path.join("/websites/alatis/upload/", submission_id, 'display_info')

        if os.path.isfile(display_path):
            var_dict = process_a_folder("/websites/alatis/upload/", submission_id, display_path)
        else:
            # does not have display_info
            var_dict = {}

        # split mixture mol(s) and map(s)
        content = var_dict['output_mixture_info'].split('\n')
        mixtures = []
        for i in range(len(content)):
            a_content = content[i]
            a_mol_map_pair = a_content.split(',')
            if len(a_mol_map_pair) == 2:
                mixtures.append([a_mol_map_pair[0], a_mol_map_pair[1], str(i + 2)])
        var_dict['mixtures'] = mixtures

        var_dict['ID'] = submission_id + '/'

        if var_dict['input'] != '':
            name = var_dict['input'].replace('.', '_')
            copyfile(zpath, os.path.join("/websites/alatis/upload/", submission_id, name + ".zip"))
            var_dict['zfile'] = name + ".zip"
            var_dict['title'] = 'ALATIS output ' + name
        else:
            copyfile(zpath, os.path.join("/websites/alatis/upload/", submission_id, submission_id + ".zip"))
            var_dict['zfile'] = submission_id + ".zip"
            var_dict['title'] = 'ALATIS output (Job ID: "' + submission_id + '")'
        return render_template("submission_output.html", **var_dict)
    else:
        return render_template("submission_wait.html")


@application.route('/upload/<submission_id>/<file_>')
def submission_files(submission_id, file_):
    """Download one of the data files."""
    fdir = os.path.join("/websites/alatis/upload/", submission_id)
    return send_from_directory(fdir, file_)


@application.route('/upload', methods=['POST'])
def upload_file():
    # get posted parameters
    file_ = request.files.get('infile', None)
    input_text = request.form.get('inputtext', '')
    input_format = request.form.get("FORMAT", 'mol')
    input_project = request.form.get("proj2to3", '')
    input_addhyd = request.form.get('addHydr', '')
    if input_project == 'on':
        projection_3d = '1'
    else:
        projection_3d = '0'
    if input_addhyd == 'on':
        add_hyd = '1'
    else:
        add_hyd = '0'

    current_datetime = datetime.datetime.now()
    found_a_folder = 0
    for temp_iter in range(5):
        rand_number = randint(10 ** 5, 10 ** 6 - 1)
        folder_name = "%d%d%d%d%d%d%d%d" % (current_datetime.year, current_datetime.month, current_datetime.day,
                                            current_datetime.hour, current_datetime.minute, current_datetime.second,
                                            current_datetime.microsecond, rand_number)
        folder_path = os.path.join(UPLOAD_FOLDER, folder_name)
        if not os.path.isdir(folder_path):
            found_a_folder = 1
            break
    if found_a_folder == 0:
        var = {'err_msg': 'There was an error while uploading files! Please try again!'}
        return render_template("error.html", **var)
    # if there is no uploaded structure
    if not file_ and input_text == '':
        var = {'err_msg': 'No selected file'}
        return render_template("error.html", **var)

    os.system('mkdir ' + folder_path)
    os.system('chmod -R 777 ' + folder_path)
    if not file_:
        filename = 'input.' + input_format
        fout = open(os.path.join(folder_path, filename), 'w')
        fout.write(input_text)
        fout.close()
    else:
        filename = secure_filename(file_.filename)
        file_.save(os.path.join(folder_path, filename))

    fout = open(os.path.join(folder_path, 'alatis.sub'), 'w')
    variables = {'binary_path': BIN_PATH, 'filename': filename, 'input_format': input_format,
                 'projection_3d': projection_3d, 'add_hyd': add_hyd,
                 'folder_name': folder_path, 'inchi_path': INCHI1_PATH}
    fout.write("""universe = vanilla
executable = {binary_path}
arguments = {filename} {input_format} {projection_3d} {add_hyd} {folder_name}
environment = "LD_LIBRARY_PATH=""${{LD_LIBRARY_PATH}}:/raid/mcr/v901/runtime/glnxa64:/raid/mcr/v901/bin/glnxa64:/raid/mcr/v901/sys/os/glnxa64:/raid/mcr/v901/sys/opengl/lib/glnxa64"" "
error = temp.err
output = temp.out
log = temp.log
should_transfer_files = yes
transfer_input_files = {inchi_path},{filename}
when_to_transfer_output = on_exit
transfer_output_files = display_info, outputs.zip
periodic_remove = (time() - QDate) > 7200
queue
""".format(**variables))
    fout.close()

    os.chdir(folder_path)
    os.system('condor_submit ./alatis.sub')
    return redirect(request.url_root + 'upload/' + folder_name)


@application.route('/')
def home_page():
    """ Reroute to home page."""
    return render_template("index.html")


@application.route('/search')
def reroute():
    """ Reroute to query page."""
    term = request.args.get('term', "")
    return redirect("query?term=%s" % term, code=302)


@application.route('/js')
@application.route('/search/js')
def js():
    """ Send the JS"""
    return send_file("search.js")


@application.route('/search/css')
@application.route('/css')
def css():
    """ Send the JS"""
    return send_file("search.css")


@application.route('/search/results')
def results():
    """ Search results. """
    var_dict = {'title': request.args.get('term', ""),
                'results': search(local=True)}

    return render_template("search.html", **var_dict)


@application.route('/databases')
def databases():
    """ Send back the databases page. """
    return render_template("examples.html")


@application.route('/search/query')
def search(local=False):
    """ Search the DB. """

    term = request.args.get('term', None)
    if not term:
        return "Specify term."

    cur = get_postgres_connection(dictionary_cursor=True)[1]

    limit = " LIMIT 75;"
    if local:
        limit = ";"

    if request.args.get('debug', None):
        return '''
SELECT * FROM (
SELECT id,db,term,termname,data_path,similarity(term, '%s') AS sml FROM search_terms
  WHERE lower(term) LIKE lower('%s')
UNION
SELECT id,db,term,termname,data_path,similarity(term, '%s')  FROM search_terms
  WHERE identical_term @@ plainto_tsquery('%s')
UNION
(SELECT cm.id::text, 'PubChem', coalesce(cn.name, 'Unknown Name'), 'Compound',
 'pubchem/'||cm.id, 1 FROM compound_metadata AS cm
  LEFT JOIN compound_name AS cn
ON cn.id = cm.id WHERE cm.id=to_number('0'||'%s', '99999999999')::int ORDER BY cn.seq LIMIT 1)
UNION
(SELECT id::text, 'PubChem', name, 'Compound', 'pubchem/'||id, similarity(lower(name), lower(%s)) FROM compound_name
  WHERE lower(name) LIKE lower('%s') LIMIT 50)) AS f
ORDER by sml DESC, 2!='PubChem', id ASC LIMIT 75;''' % (term, term + "%", term, term, term, term, term + "%")

    cur.execute('''
SELECT * FROM (
SELECT id,db as database,term,termname,data_path,similarity(term, %s) AS sml FROM search_terms
  WHERE lower(term) LIKE lower(%s)
UNION
SELECT id,db,term,termname,data_path,similarity(term, %s)  FROM search_terms
  WHERE identical_term @@ plainto_tsquery(%s)
UNION
(SELECT cm.id::text, 'PubChem', coalesce(cn.name, 'Unknown Name'), 'Compound',
 'pubchem/'||cm.id, 1 FROM compound_metadata AS cm
  LEFT JOIN compound_name AS cn
  ON cn.id = cm.id WHERE cm.id=to_number('0'||%s, '99999999999')::int ORDER BY cn.seq LIMIT 1)
UNION
(SELECT id::text, 'PubChem', name, 'Compound', 'pubchem/'||id, similarity(lower(name), lower(%s)) FROM compound_name
  WHERE lower(name) LIKE lower(%s) LIMIT 50)) AS f
ORDER by sml DESC, database!='PubChem', id ASC''' + limit, [term, term + "%", term, term, term, term, term + "%"])

    # First query
    result = []
    for item in cur.fetchall():
        res = {"link": item['data_path'],
               "db": item["database"],
               "entry": item['id'],
               "termname": item['termname'],
               "term": unicode(item['term'], 'utf-8')}

        result.append(res)

    if not local:
        return jsonify(result)

    return result


@application.route('/reload')
def reload_db():
    """ Reload the DB."""

    # Open the DB and clear the existing index
    conn, cur = get_postgres_connection(user='postgres')
    sql_file1 = os.path.join(os.path.dirname(os.path.realpath(__file__)), "initialize1.sql")
    sql_file2 = os.path.join(os.path.dirname(os.path.realpath(__file__)), "initialize2.sql")
    cur.execute(open(sql_file1, "r").read())

    process_dirs = ['/websites/alatis/examples/BMRB/',
                    '/websites/alatis/examples/HMDB/',
                    '/websites/alatis/examples/PDB/']

    # Pattern for detecting BMRB ID in title field
    pattern = re.compile("^bmse[0-9]+$")

    for sdir in process_dirs:
        db = os.path.basename(os.path.normpath(sdir))

        try:
            for ssdir in os.listdir(sdir):
                print("Doing %s." % ssdir)
                data = bmrb.Entry.from_file(os.path.join(sdir, ssdir, "%s.str" % ssdir))
                title = data['chem_comp']['Name'][0]
                entry_id = data['entry_information']['ID'][0]
                inchi = data['chem_comp']['InChI_code'][0]
                formula = data['chem_comp']['Formula'][0].replace(" ", "").replace("-", "")
                path = data['chem_comp']['path'][0].replace("example/", "examples/")

                # ID
                cur.execute('''
INSERT INTO search_terms_tmp (id, db, termname, term, data_path)
VALUES (%s, %s, %s, %s, %s);''', [entry_id, db, 'Entry_ID', entry_id, path])

                # Title is BMRB ID
                if pattern.match(title):
                    cur.execute('''
INSERT INTO search_terms_tmp (id, db, termname, term, data_path)
VALUES (%s, %s, %s, %s, %s);''', [entry_id, db, 'Compound', title, path])
                # Title is compound name
                else:
                    cur.execute('''
INSERT INTO search_terms_tmp (id, db, termname, term, identical_term, data_path)
VALUES (%s, %s, %s, %s, to_tsvector(%s), %s);''', [entry_id, db, 'Compound', title, title, path])

                # Formula
                cur.execute('''
INSERT INTO search_terms_tmp (id, db, termname, term, data_path)
VALUES (%s, %s, %s, %s, %s);''', [entry_id, db, 'Formula', formula, path])

                # InChI
                cur.execute('''
INSERT INTO search_terms_tmp (id, db, termname, term, data_path)
VALUES (%s, %s, %s, %s, %s);''', [entry_id, db, 'InChI', inchi, path])
                cur.execute('''
INSERT INTO search_terms_tmp (id, db, termname, term, data_path)
VALUES (%s, %s, %s, %s, %s);''', [entry_id, db, 'InChI', inchi[6:], path])

        except Exception as e:
            print("Exception: %s" % str(e))
            continue

    cur.execute(open(sql_file2, "r").read())
    conn.commit()

    return redirect(url_for('home_page'), 302)


if __name__ == "__main__":
    reload_db()
