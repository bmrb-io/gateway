#!/usr/bin/python

""" Power the gateway server. """

from __future__ import print_function

import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from time import sleep
import subprocess

# Installed packages
import requests
import psycopg2
from flask import Flask, request, jsonify, redirect, render_template, url_for
from psycopg2.extras import DictCursor

# Set up the flask application
application = Flask(__name__)
dir_path = os.path.dirname(os.path.realpath(__file__))


def get_postgres_connection(user='web', database='webservers', host="pinzgau.nmrfam.wisc.edu",
                            dictionary_cursor=False):
    """ Returns a connection to postgres and a cursor."""

    if dictionary_cursor:
        conn = psycopg2.connect(user=user, database=database, host=host, cursor_factory=DictCursor)
    else:
        conn = psycopg2.connect(user=user, database=database, host=host)
    cur = conn.cursor()
    cur.execute("SET search_path TO dci, public")

    return conn, cur


@contextmanager
def TemporaryDirectory():
    name = tempfile.mkdtemp()
    try:
        yield name
    finally:
        shutil.rmtree(name)


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

    # if there is no uploaded structure
    if not file_ and not input_text:
        return render_template("error.html", error='No uploaded file or pasted file.')

    with TemporaryDirectory() as folder_path:
        if not file_:
            open(os.path.join(folder_path, 'submitted.data'), 'w').write(input_text)
        else:
            file_.save(os.path.join(folder_path, 'submitted.data'))

        variables = {'binary_path': os.path.join(dir_path, 'binary', 'get_inchi.py'),
                     'input_format': input_format,
                     'projection_3d': projection_3d,
                     'add_hyd': add_hyd}
        with open(os.path.join(folder_path, 'inchi.sub'), 'w') as fout:
            fout.write("""universe = vanilla
        executable = get_inchi.py
        arguments = submitted.data {input_format} {projection_3d} {add_hyd}
        error = temp.err
        output = temp.out
        log = temp.log
        should_transfer_files = yes
        transfer_input_files = inchi-1, submitted.data
        when_to_transfer_output = on_exit
        transfer_output_files = inchi.txt
        periodic_remove = (time() - QDate) > 7200
        queue
        """.format(**variables))
        shutil.copy(os.path.join(dir_path, 'binary', 'inchi-1'), os.path.join(folder_path, 'inchi-1'))
        shutil.copy(variables['binary_path'], folder_path)
        os.chdir(folder_path)
        subprocess.call(['condor_submit', 'inchi.sub'])

        ipath = os.path.join(folder_path, 'inchi.txt')
        timeout = 0
        while True:
            # Easier to ask for forgiveness than permission
            try:
                err = open(os.path.join(folder_path, 'temp.out'), 'r').read()
                if len(err) > 0:
                    return render_template("error.html", error=err)

                inchi = open(ipath, 'r').read()
                if len(inchi) == 0:
                    raise ValueError
                return redirect(url_for('inchi_search', inchi=inchi))
            # Results are not yet ready
            except (IOError, ValueError):
                sleep(.1)
                timeout += .1
                if timeout > 120:
                    return render_template("error.html", error='Timeout when calculating the InChI string.')


@application.route('/inchi')
@application.route('/inchi/<path:inchi>')
def inchi_search(inchi=None):
    """ Show the results for a given InChI. """

    # The aren't using the URL, they are sending as parameter
    if inchi is None:
        inchi = request.args.get('inchi', '')

    if not inchi:
        return []

    # Strip off a trailing path if needed
    if inchi.endswith('/'):
        inchi = inchi[:-1]

    if not inchi.startswith('InChI='):
        inchi = 'InChI=' + inchi

    #InChI=1S/C6H12O6/c7-1-3(9)5(11)6(12)4(10)2-8/h3,5-9,11-12H,1-2H2/t3?,5?,6u/m1/s1

    def enumerate_chirality(chiral_inchi_segment):

        for chiral_center in re.finditer('[u?]', chiral_inchi_segment):
            position = chiral_center.span()[0]
            left_hand = enumerate_chirality(
                chiral_inchi_segment[0:position] + '+' + chiral_inchi_segment[position + 1:])
            right_hand = enumerate_chirality(
                chiral_inchi_segment[0:position] + '-' + chiral_inchi_segment[position + 1:])
            return right_hand + left_hand

        return [chiral_inchi_segment]


    # This can fail if InChI doesn't have chirality section
    try:
        chiral_start = inchi.index(r'/t') + 1
        chiral_end = inchi.index('/', chiral_start)
        replace_from = inchi[chiral_start:chiral_end]
        chiral_options = enumerate_chirality(replace_from)
    except ValueError:
        chiral_options = [inchi]

    # Have to query multiple times or postgres doesn't realize it can use the index - so weird
    cur = get_postgres_connection(dictionary_cursor=True)[1]
    sql = 'SELECT * FROM dci.db_links where inchi=%s'
    results = []
    for chiral_chunk in enumerate_chirality(replace_from):
        cur.execute(sql, [inchi.replace(replace_from, chiral_chunk)])
        result = cur.fetchone()
        if result:
            results.append(result)

    if results:
        # No chirality - go straight to the results
        if len(chiral_options) == 1:
            return render_template("multi_search.html", inchi=inchi, matches=results[0], chiral_matches=results,
                                   active='result')
        else:
            # Chirality - display the results page
            return render_template("multi_search.html", inchi=inchi, chiral_matches=results, active='inchi')
    else:
        # No matches
        return render_template("multi_search.html", inchi=inchi, active='inchi',
                               error="No compound matching that InChI was found in the database.")


@application.route('/')
def home_page():
    """ Render the home page."""
    return render_template("multi_search.html", active='structure')


@application.route('/name')
def name_search():
    """ Render the name search."""

    term = request.args.get('term', "")
    if term:
        results = requests.get('http://alatis.nmrfam.wisc.edu/search/inchi', params={'term': term}).json()
    else:
        results = None
    var_dict = {'title': term, 'results': results, 'active': 'name'}

    return render_template("multi_search.html", **var_dict)


@application.route('/reload')
def reload_db():
    """ Reload the DB."""

    # Open the DB and clear the existing index
    conn, cur = get_postgres_connection(user='postgres')
    cur.execute('''
CREATE MATERIALIZED VIEW IF NOT EXISTS dci.inchi_index AS SELECT DISTINCT(inchi) FROM (
SELECT inchi FROM gissmo.entries
UNION
SELECT inchi FROM camp.camp
UNION
SELECT inchi FROM bmod.bmod_index
UNION
SELECT inchi FROM alatis.compound_alatis) s 
WHERE inchi IS NOT NULL and inchi != '' and inchi != 'FAILED';
CREATE UNIQUE INDEX IF NOT EXISTS inchi_index_index ON inchi_index (inchi);

DROP VIEW IF EXISTS dci.names CASCADE;
CREATE VIEW dci.names AS
select inchi, array_remove(array_agg(name ORDER BY sequence), NULL) as name FROM
(SELECT ii.inchi, cn.name, cn.seq as sequence
 FROM dci.inchi_index as ii
   LEFT JOIN alatis.compound_alatis as ca on ii.inchi=ca.inchi
   LEFT JOIN alatis.compound_name as cn on ca.id=cn.id
UNION ALL
SELECT ii.inchi, g.name, 0
 FROM dci.inchi_index as ii
   LEFT JOIN gissmo.entries as g on ii.inchi=g.inchi
UNION ALL
SELECT ii.inchi, unnest(c.name) as name, 100
 FROM dci.inchi_index as ii
   LEFT JOIN camp.camp as c ON ii.inchi=c.inchi) as why
GROUP BY inchi;

DROP VIEW IF EXISTS dci.db_links;
CREATE VIEW dci.db_links AS SELECT
  d.inchi,
  array_remove(array_agg(DISTINCT a.id), NULL) as alatis_ids,
  array_remove(array_agg(DISTINCT g.id), NULL) as gissmo_ids,
  array_remove(array_agg(DISTINCT c.id), NULL) as camp_ids,
  array_remove(array_agg(DISTINCT b.id), NULL) as bmod_ids,
  n.name as names
FROM dci.inchi_index AS d
 LEFT JOIN alatis.compound_alatis AS a ON a.inchi=d.inchi
 LEFT JOIN gissmo.entries AS g ON g.inchi=d.inchi
 LEFT JOIN camp.camp AS c ON c.inchi=d.inchi
 LEFT JOIN bmod.bmod_index AS b ON b.inchi=d.inchi
 LEFT JOIN names AS n ON n.inchi=d.inchi
GROUP BY d.inchi, n.name;

GRANT USAGE ON SCHEMA dci TO web;
GRANT SELECT ON ALL TABLES IN SCHEMA dci TO web;
''')
    conn.commit()
    return redirect(url_for('home_page'), 302)


@application.route('/hard-reload')
def hard_reload_db():
    """ Reload the DB."""

    # Open the DB and clear the existing index
    conn, cur = get_postgres_connection(user='postgres')
    cur.execute('REFRESH MATERIALIZED VIEW inchi_index')
    conn.commit()


if __name__ == "__main__":
    reload_db()
