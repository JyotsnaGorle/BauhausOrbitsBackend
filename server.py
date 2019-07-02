import json
from flask import Flask, jsonify, Response, request
from neo4j import GraphDatabase
from dicttoxml import dicttoxml

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

    def do_transaction(self):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_books)

    @staticmethod
    def _query_for_books(tx):
        result = tx.run(
            "MATCH (n:Book)"
            "RETURN n",
        )
        records = []
        for record in result:
            records.append(dict(record['n'].items()))
            xml = dicttoxml(records, custom_root='allBooks', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_all_pages(self, bookId):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_pages, bookId)

    @staticmethod
    def _query_for_pages(tx, bookId):
        result = tx.run(
            "MATCH (n:Book {book_id: 32})<-[:IS_IN]-(s:Sentence) WITH toInteger(s.page) AS page_number RETURN DISTINCT(page_number) ORDER by page_number ASC",
        bookId = bookId)
        records = []
        for record in result:
            records.append(record['page_number'])
            page_dict = {
            "page_numbers": records
            }
            xml = dicttoxml(page_dict, custom_root='pages', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp


    def get_Top_Ten_Topics(self, bookId1, bookId2):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_topics, bookId1, bookId2)

    @staticmethod
    def _query_for_topics(tx, bookId1, bookId2):
        result = tx.run(
            "MATCH (b:Book)<-[r1:IS_IN]-(s:Sentence)<-[r2:IS_IN]-(t:Topic) WHERE b.book_id = {first} or b.book_id = {second} RETURN t LIMIT 10",
        first = int(bookId1),
        second = int(bookId2))
        records = []
        for record in result:
            records.append(dict(record['t'].items()))
            xml = dicttoxml(records, custom_root='topicsInBooks', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_common_Topics(self, bookId1, bookId2):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_common_topics, bookId1, bookId2)

    @staticmethod
    def _query_for_common_topics(tx, bookId1, bookId2):
        result = tx.run(
            "MATCH (b:Book)<-[r1:IS_IN]-(s:Sentence)<-[r2:IS_IN]-(t:Topic) WHERE b.book_id = {first} or b.book_id = {second} RETURN t ORDER by t.score DESC LIMIT 10",
        first = int(bookId1),
        second = int(bookId2))
        records = []
        for record in result:
            records.append(dict(record['t'].items()))
            xml = dicttoxml(records, custom_root='commonTopics', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_sentences_for_topic(self, topic):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_sentences_with_topic, topic)

    @staticmethod
    def _query_for_sentences_with_topic(tx, topic):
        result = tx.run(
            "MATCH (s:Sentence)<-[r2:IS_IN]-(t:Topic) WHERE t.word = {word} RETURN s",
        word = topic)
        records = []
        for record in result:
            records.append(dict(record['s'].items()))
            xml = dicttoxml(records, custom_root='sentences', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_sentences_for_topic_sort_year(self, topic):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_sentences_with_topic_sort_year, topic)

    @staticmethod
    def _query_for_sentences_with_topic_sort_year(tx, topic):
        result = tx.run(
            "MATCH (n:Book)<-[r1:IS_IN]-(s:Sentence)<-[r2:IS_IN]-(t:Topic) WITH n.year AS book_year, s WHERE t.word = {word} RETURN s ORDER BY book_year DESC",
        word = topic)
        records = []
        for record in result:
            records.append(dict(record['s'].items()))
            xml = dicttoxml(records, custom_root='sentences', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_similar_topic_sentences(self, topic):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_sentences_with_topic_similarity, topic)

    @staticmethod
    def _query_for_sentences_with_topic_similarity(tx, topic):
        result = tx.run(
            "MATCH (s:Sentence)<-[r2:IS_IN]-(t:Topic) WHERE t.word = {word} RETURN s, t ORDER BY t.score ASC",
        word = topic)
        records = []
        for record in result:
            records.append(dict(record['s'].items()))
            xml = dicttoxml(records, custom_root='sentences', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_positive_topic_sentences(self, topic):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_sentences_with_positive_topic_sentiment, topic)

    @staticmethod
    def _query_for_sentences_with_positive_topic_sentiment(tx, topic):
        result = tx.run(
            "MATCH (s:Sentence)<-[r2:IS_IN]-(t:Topic) WHERE t.word = {word} RETURN s, t ORDER BY s.sentiment_score DSC",
        word = topic)
        records = []
        for record in result:
            records.append(dict(record['s'].items()))
            xml = dicttoxml(records, custom_root='sentences', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_negative_topic_sentences(self, topic):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_sentences_with_negative_topic_sentiment, topic)

    @staticmethod
    def _query_for_sentences_with_negative_topic_sentiment(tx, topic):
        result = tx.run(
            "MATCH (s:Sentence)<-[r2:IS_IN]-(t:Topic) WHERE t.word = {word} RETURN s, t ORDER BY s.sentiment_score ASC",
        word = topic)
        records = []
        for record in result:
            records.append(dict(record['s'].items()))
            xml = dicttoxml(records, custom_root='sentences', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_book_details(self, sentenceId):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_book_details, sentenceId)

    @staticmethod
    def _query_for_book_details(tx, sentenceId):
        result = tx.run(
            "MATCH (n:Book)<-[r1:IS_IN]-(s:Sentence {sentence_id: {sentenceId}}) RETURN n, s",
        sentenceId = int(sentenceId))
        records = []
        for record in result:
            records.append(dict(record['n'].items()))
            xml = dicttoxml(records, custom_root='bookDetails', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_image_with_caption(self, captionId):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_image_with_caption, captionId)

    @staticmethod
    def _query_for_image_with_caption(tx, captionId):
        result = tx.run(
            "MATCH (i:Image {image_id: {captionId}}) RETURN i",
        captionId = int(captionId))
        records = []
        for record in result:
            records.append(dict(record['i'].items()))
            xml = dicttoxml(records, custom_root='imageDetails', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_image_with_topic(self, topic):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_image_with_topic, topic)

    @staticmethod
    def _query_for_image_with_topic(tx, topic):
        result = tx.run(
            "MATCH (i:Image)<-[r2:IS_IN]-(t:Topic) WHERE t.word = {word} RETURN i, t ORDER BY t.score ASC",
        word = topic)
        records = []
        for record in result:
            records.append(dict(record['i'].items()))
            xml = dicttoxml(records, custom_root='images', attr_type=False)
            resp = Response(xml, status=200, mimetype='text/xml')
        return resp

    def get_sentences_and_topics(self, bookId1, bookId2):
        with self._driver.session() as session:
            return session.write_transaction(self._query_for_sentences_and_topics, bookId1, bookId2)

    @staticmethod
    def _query_for_sentences_and_topics(tx, bookId1, bookId2):
        result = tx.run(
            "MATCH (b:Book)<-[r1:IS_IN]-(s:Sentence)<-[r2:IS_IN]-(t:Topic) WHERE b.book_id = 1 OR b.book_id = 2 AND t.score > 0.5 RETURN collect(t) as t,s",
        bookId1 = int(bookId1),
        bookId2 = int(bookId2))
        records = []
        for record in result:
            sentence = dict(record['s'].items())

            topic = record['t']
            recordTopic = []
            for each in topic:
                a = dict(each.items())['word']
                recordTopic.append(a)
            sentence['topics'] = recordTopic
            records.append(sentence)
        xml = dicttoxml(records, custom_root='sentencesWithTopics', attr_type=False)
        resp = Response(xml, status=200, mimetype='text/xml')
        return resp


obj = DBHandler("bolt://localhost:7687")


@app.route("/")
def hello1():
    return jsonify({"messages": "response"})


@app.route("/add/", methods=["POST"])
def hello2():
    data = json.loads(request.data)
    return jsonify({"messages": "response"})

@app.route("/all/")
@requires_auth
def allBooks():
    response = obj.do_transaction()
    return response

@app.route("/allPagesForBook/<bookId>")
@requires_auth
def allPages(bookId=None):
    if not bookId:
        bookId=1
    response = obj.get_all_pages(bookId)
    return response

@app.route("/ldaTopicsForBooks/<bookId1>/<bookId2>")
@requires_auth
def topTenLDATopics(bookId1=None, bookId2=None):
    if not bookId1:
        bookId1=1
    if not bookId2:
        bookId2=2
    response = obj.get_Top_Ten_Topics(bookId1, bookId2)
    return response

@app.route("/commonLdaTopicsForBooks/<bookId1>/<bookId2>")
@requires_auth
def commonLdaTopicsForBooks(bookId1=None, bookId2=None):
    if not bookId1:
        bookId1=1
    if not bookId2:
        bookId2=2
    response = obj.get_common_Topics(bookId1, bookId2)
    return response

@app.route("/allSentencesForTopic/<topic>")
@requires_auth
def allSentencesForTopic(topic=None):
    response = obj.get_sentences_for_topic(topic)
    return response

@app.route("/allSentencesForTopicSortYear/<topic>")
@requires_auth
def allSentencesForTopicSortYear(topic=None):
    response = obj.get_sentences_for_topic_sort_year(topic)
    return response

@app.route("/sentencesSimilarToTopic/<topic>")
@requires_auth
def sentencesSimilarToTopic(topic=None):
    response = obj.get_similar_topic_sentences(topic)
    return response

@app.route("/sentencesPositiveToTopic/<topic>")
@requires_auth
def sentencesPositiveToTopic(topic=None):
    response = obj.get_positive_topic_sentences(topic)
    return response

@app.route("/sentencesNegativeToTopic/<topic>")
@requires_auth
def sentencesNegativeToTopic(topic=None):
    response = obj.get_negative_topic_sentences(topic)
    return response

@app.route("/bookDetailsForSentence/<sentenceId>")
@requires_auth
def bookDetailsForSentence(sentenceId=None):
    response = obj.get_book_details(sentenceId)
    return response

@app.route("/imageWithCaption/<captionId>")
@requires_auth
def imageWithCaption(captionId=None):
    response = obj.get_image_with_caption(captionId)
    return response

@app.route("/imageWithTopic/<topic>")
@requires_auth
def imageWithTopic(topic=None):
    response = obj.get_image_with_topic(topic)
    return response

@app.route("/sentencesAndTopics/<bookId1>/<bookId2>")
@requires_auth
def sentencesAndTopics(bookId1=None, bookId2=None):
    if not bookId1:
        bookId1=1
    if not bookId2:
        bookId2=2
    response = obj.get_sentences_and_topics(bookId1, bookId2)
    return response

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
