import json
from flask import Flask, jsonify, Response, request
from neo4j import GraphDatabase

from functools import wraps
def check_auth(username, password):
    with open('creds.json', 'r') as f:
            creds_dict = json.load(f)
    return username == creds_dict["username"] and password == creds_dict["password"]


def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        "Could not verify your access level for that URL.\n"
        "You have to login with proper credentials",
        401,
        {"WWW-Authenticate": 'Basic realm="Login Required"'},
    )


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)

    return decorated


app = Flask(__name__)


class DBHandler(object):
    def __init__(self, uri):
        with open('creds.json', 'r') as f:
            creds_dict = json.load(f)
        self._driver = GraphDatabase.driver(uri, auth=(creds_dict["dbUser"], creds_dict["dbPwd"]))

    def close(self):
        self._driver.close()

    def print_greeting(self, message):
        with self._driver.session() as session:
            return session.write_transaction(self._create_and_return_greeting, message)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run(
            "MATCH (s:Sentence) "
            "WHERE s.sentiment_score = 1 "
            "RETURN s",
        )
        records = []
        for record in result:
            records.append(dict(record['s'].items()))
        return jsonify(records)


obj = DBHandler("bolt://localhost:7687")


@app.route("/")
def hello1():
    return jsonify({"messages": "response"})


@app.route("/add", methods=["POST"])
def hello2():
    data = json.loads(request.data)
    return jsonify({"messages": "response"})


@app.route("/echo/<msg>")
@requires_auth
def hello(msg=None):
    if not msg:
        msg = "helooo"

    response = obj.print_greeting(msg)
    return response

if __name__ == "__main__":
    app.run(host="0.0.0.0")
