from flask import Flask, request, jsonify
from MySQLdb import _mysql
import json
import datetime

app = Flask(__name__)

HOST = "****"
USER = "****"
PASSWD = "****"
DB = "****"


def test_db_connection():
    db=_mysql.connect(host=HOST,user=USER, passwd=PASSWD, db=DB)
    db.query("select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='botlog_dev';")
    r = db.store_result()
    return r.fetch_row()


def to_datetime(str_dt):
    return str_dt.replace('T', ' ').split('.')[0].split('+')[0]


def single_preprop(x):
    restricted_words = ['drop']
    v = "'" + str(x).replace('\\', '') + "'"
    for restricted_word in restricted_words:
        if restricted_word in v:
            return 'NULL'
    return v


def preprop(iterable):
    return ', '.join(list(map(single_preprop, list(iterable))))


def parse_dict_to_insert_query(d):
    query_dict = dict()
    for key in ['value', 'label', 'user_type', 'user_id', 'conv_id', 'message_id', 'timestamp']:
        query_dict[key] = d.get(key, 'NULL')
    query = "INSERT INTO botlog_dev (" + ', '.join(list(query_dict.keys())) + ') VALUES (' + preprop(query_dict.values()) + ');'
    return query


@app.route('/api/add_log_data', methods=['GET', 'POST'])
def add_log_data():
    log_dict = request.json
    db=_mysql.connect(host=HOST,user=USER, passwd=PASSWD, db=DB)
    db.query(parse_dict_to_insert_query(log_dict))
    return '200'


@app.route('/api/test_db_conn', methods=['GET', 'POST'])
def test_db_conn():
    return str(test_db_connection())


@app.route('/api/lastbotfbf', methods=['GET', 'POST'])
def get_last_bot_feedback_frame():
    chat_id = request.args.get('chatid')
    query = 'SELECT message_id, value, timestamp FROM botlog_dev WHERE conv_id = ' + single_preprop(chat_id) + "AND user_type = 'bot' ORDER BY timestamp DESC LIMIT 1;"
    db=_mysql.connect(host=HOST,user=USER, passwd=PASSWD, db=DB)
    db.query(query)
    r = db.store_result()
    try:
        return jsonify(r.fetch_row(how=1)[0])
    except:
        return 'EMPTY SET'

@app.route('/api/pollresults', methods=['GET', 'POST'])
def get_poll_results():
    chat_id = request.args.get('chatid', False)
    ts = request.args.get('ts', False)
    if not(chat_id or ts):
        return 'err'
    query = "SELECT message_id, value, timestamp FROM botlog_dev WHERE conv_id = " + single_preprop(chat_id) + " AND label = 'answer' AND timestamp >= " + single_preprop(ts) + ";"
    db=_mysql.connect(host=HOST,user=USER, passwd=PASSWD, db=DB)
    db.query(query)
    r = db.store_result()
    return jsonify(r.fetch_row(maxrows=0, how=1))


@app.route('/api/pollsincebot', methods=['GET', 'POST'])
def get_poll_since_bot():
    chat_id = request.args.get('chatid', False)
    if not chat_id:
        return 'err'
    query_last_bot = 'SELECT message_id, value, timestamp FROM botlog_dev WHERE conv_id = ' + single_preprop(chat_id) + "AND user_type = 'bot' ORDER BY timestamp DESC LIMIT 1;"

    db=_mysql.connect(host=HOST,user=USER, passwd=PASSWD, db=DB)
    db.query(query_last_bot)
    r = db.store_result()
    ts = r.fetch_row(maxrows=0, how=1)[0]['timestamp'].decode()
    query = "SELECT message_id, value, timestamp FROM botlog_dev WHERE conv_id = " + single_preprop(chat_id) + " AND label = 'answer' AND timestamp >= '" + ts + "';"
    db.query(query)
    r = db.store_result()
    try:
        return jsonify(r.fetch_row(maxrows=0, how=1))
    except:
        return 'EMPTY SET'
