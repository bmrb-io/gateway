#!/usr/bin/python

""" Power the gateway server. """

from __future__ import print_function

# Standard imports
import os

# Installed packages
import requests
import psycopg2
from psycopg2.extras import DictCursor

from flask import Flask, request, Response, jsonify, redirect, send_file, render_template, send_from_directory, url_for

# Set up the flask application
application = Flask(__name__)
application.config['ALATIS_API_URL'] = 'http://alatis.nmrfam.wisc.edu/upload'


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

    if not file_:
        structure_file = input_text
    else:
        structure_file = file_.read()

    data = {'format': input_format,
            'response_type': 'json',
            'project_2_to_3': projection_3d,
            'add_hydrogens': add_hyd,
            'input_text': structure_file
            }
    r = requests.post(application.config['ALATIS_API_URL'], data=data)
    json_result = r.json()

    return redirect(url_for('inchi_search', inchi=json_result['inchi']))


@application.route('/inchi/<path:inchi>')
def inchi_search(inchi):
    """ Show the results for a given InChI. """

    cur = get_postgres_connection(dictionary_cursor=True)[1]
    cur.execute('SELECT * FROM dci.db_links where inchi=%s', [inchi])
    result = cur.fetchone()

    return render_template('inchi.html', inchi=inchi, matches=result)


@application.route('/')
def home_page():
    """ Reroute to home page."""
    return render_template("index.html")


@application.route('/search')
def reroute():
    """ Reroute to query page."""
    term = request.args.get('term', "")
    return redirect("query?term=%s" % term, code=302)


@application.route('/search/results')
def results():
    """ Search results. """
    var_dict = {'title': request.args.get('term', ""),
                'results': search(local=True)}

    return render_template("search.html", **var_dict)


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
    cur.execute('''
DROP VIEW IF EXISTS dci.db_links;
CREATE VIEW dci.db_links AS SELECT
  d.inchi,
  array_remove(array_agg(DISTINCT a.id), NULL) as alatis_ids,
  array_remove(array_agg(DISTINCT g.id), NULL) as gissmo_ids,
  array_remove(array_agg(DISTINCT c.id), NULL) as camp_ids
FROM dci.inchi_index AS d
 LEFT JOIN alatis.compound_alatis AS a ON a.inchi=d.inchi
 LEFT JOIN gissmo.entries AS g ON g.inchi=d.inchi
 LEFT JOIN camp.camp AS c ON c.inchi=d.inchi
GROUP BY d.inchi;
GRANT USAGE ON SCHEMA dci TO web;
GRANT SELECT ON ALL TABLES IN SCHEMA dci TO web;
''')
    conn.commit()
    return redirect(url_for('home_page'), 302)


if __name__ == "__main__":
    reload_db()
