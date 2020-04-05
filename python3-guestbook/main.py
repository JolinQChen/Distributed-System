# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from flask import Flask, render_template, request, session, redirect
from google.cloud import datastore
from google.oauth2 import id_token

import datetime
import sys
import requests


datastore_client = datastore.Client()
DEFAULT_KEY = 'Action'

app = Flask(__name__)

app.secret_key = b'lkjadfsj009(*02347@!$&'

"""
User class I created, serves as a placeholder demonstrator
to store google user info
"""

# write new message to guestbook
def store_greeting(title, rating, platform, developer, year_release, username, email, dt, kn):
    entity = datastore.Entity(key=datastore_client.key(kn.lower()))
    entity.update({
        'title': title,
        'rating':rating,
        'platform':platform,
        'developer':developer,
        'year_release':year_release,
        'timestamp': dt,
        'username': username,
        'email': email
    })

    datastore_client.put(entity)

# fetch most recent 'limit' number of messages from guestbook
def fetch_greetings(limit, kn):
    query = datastore_client.query(kind=kn.lower())
    query.order = ['-timestamp']

    greetings = query.fetch(limit=limit)

    return greetings
def display(kn):
    query = datastore_client.query(kind=kn.lower()).fetch()
    results = list(query)
    return results
# search records
def search_entity(kn_genre, _title,_rating,_platform,_developer,_year_release):
    queries = datastore_client.query(kind = kn_genre.lower())
    queries.order = ['-timestamp']
    
    greetings = list(queries.fetch())
    results = list()
    for query in greetings:
        if(query['title'].lower().find(_title.lower())!=-1 and query['rating'].lower().find(_rating.lower())!=-1 and query['platform'].lower().find(_platform.lower())!=-1 and query['developer'].lower().find(_developer.lower())!=-1 and query['year_release'].lower().find(_year_release.lower())!=-1):
            results.append(query)
    return results



# main page HTTP request processing
# when choose "search, change to another page"


@app.route('/index.html', methods = ['GET','POST'])
def index():
    key_name = DEFAULT_KEY

    if 'username' in session:
        username = session['username'] 
    else:
        username = ''

    if 'email' in session:
        useremail = session['email'] 
    else:
        useremail = ''

    # If POST, store the new message into Datastore in the appropriate guestbook
    if request.method == 'POST':
        key_name = request.form['genre'].lower()
        store_greeting(request.form['title'], request.form['rating'], request.form['platform'], request.form['developer'], request.form['year_release'], username, useremail, datetime.datetime.now(), key_name)

    # Fetch the most recent 10 messages from the appropriate guestbook in Datastore
    

    return render_template(
        'index.html')


# link to display page
@app.route('/filterPage/<string:genre>')
def filterPage(genre):
    key_name = DEFAULT_KEY
    games = display(genre)
    return render_template('filterPage.html',greetings=games)






@app.route('/SearchPage.html', methods=['GET', 'POST'])
def search():
    key_name = DEFAULT_KEY
    k_title = ''
    k_rating = ''
    k_platform=''
    k_developer=''
    k_year_release=''
    if 'username' in session:
        username = session['username'] 
    else:
        username = ''

    if 'email' in session:
        useremail = session['email'] 
    else:
        useremail = ''

    # If POST, store the new message into Datastore in the appropriate guestbook
    if request.method == 'POST':
        key_name = request.form['genre']
        k_title = request.form['title']
        k_rating = request.form['rating']
        k_platform = request.form['platform']
        k_developer = request.form['developer']
        k_year_release = request.form['year_release']
       # store_greeting(request.form['title'], request.form['rating'], request.form['platform'], request.form['developer'], request.form['year_release'], request.form['genre'], username, useremail, datetime.datetime.now(), key_name)
    if k_title=='' and k_rating=='' and k_platform=='' and k_developer=='' and k_year_release=='':
        flag=0
    else:
        flag=1
    # Fetch the most recent 10 messages from the appropriate guestbook in Datastore
    greetings = search_entity(key_name, k_title,k_rating,k_platform,k_developer,k_year_release)

    
    #greetings = fetch_greetings(10, key_name)


    return render_template(
        'SearchPage.html', greetings=greetings, genre=key_name, flag=flag)



@app.route('/login', methods=['POST'])
def login():

    # Decode the incoming data
    token = request.data.decode('utf-8')

    # Send to google for verification and get JSON return values
    verify = requests.get("https://oauth2.googleapis.com/tokeninfo?id_token=" + token)
    
    # Use a session cookie to store the username and email

    session['username'] = verify.json()["name"]
    session['email'] = verify.json()["email"]

    return redirect("/")

# back to main page
@app.route('/')
def root():
    

    return render_template('mainPage.html')


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.

    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)



