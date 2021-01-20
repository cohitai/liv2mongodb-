import logging
from importlib import reload
import sys
import pymongo
import requests
import json
import ast
import re
import time
reload(logging)
logging.basicConfig(stream=sys.stdout, filemode='a', level=logging.INFO)


class MongoLivingdocs:

    # Api Token for the production server
    api_key1 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InB1YmxpYy1hcGk6cmVhZCIsIm5hbWUiOiJUZXN0IiwicHJvamVjdElkIjoxLCJjaGFubmVsSWQiOjEsInR5cGUiOiJjbGllbnQiLCJqdGkiOiJlMjg1NWM1YS0xNGRiLTRkZTUtYjJlYS0wNTgwM2UwNzkzYTQiLCJjb2RlIjoiZTI4NTVjNWEtMTRkYi00ZGU1LWIyZWEtMDU4MDNlMDc5M2E0IiwiaWF0IjoxNTg5MzU3ODUxfQ.o6nTZdozih2vz9wpEXNJOyh60C9vjzu0ofLukcADiTg"
    client = pymongo.MongoClient(
        "mongodb+srv://cohitai:malzeit1984@cluster0.ufrty.mongodb.net/Livingdocs?retryWrites=true&w=majority")

    def __init__(self):
        self.db = self.client.Livingdocs
        self.articles = self.db.articles
        self.kafka_logs = self.db.kafka_logs
        self.articles_sqlike = self.db.articles_sqlike

    def extract_doc(self, docid):

        """function retrieves json object (dict) from document.
        :param docid - integer.
        :returns - json"""

        url = 'https://api.berliner-zeitung.de/blz/v1/print/document?documentId={1}&access_token={0}'.format(
            self.api_key1, docid)
        req = requests.get(url)

        cont = ast.literal_eval(str(json.loads(req.content)).replace("$", ""))

        return req, cont

    def update_articles(self, start_log_id=None):

        logging.info("Beginning update.... ")

        if start_log_id is None:
            start_log_id = self.update_kafka_logs()

        else:
            self.update_kafka_logs()

        for item in list(self.kafka_logs.find({"id": {"$gt": start_log_id}}).sort([("id", 1)])):

            logging.info("kafka_logs: loading eventid: {0}".format(item["id"]))

            if item['eventType'] == "unpublish":

                self.articles.delete_one(({"systemdata.documentId": item['documentId']}))
                self.articles_sqlike.delete_one(({"id": item['documentId']}))

            else:
                req, cont = self.extract_doc(item["documentId"])
                if req.ok:
                    test_dict = self.is_exists(item["documentId"])
                    if test_dict["articles"]:
                        logging.info("replaceing article, docId: {0}".format(item["documentId"]))
                        self.articles.replace_one({"systemdata.documentId": item["documentId"]}, cont)
                    else:
                        logging.info("inserting article, docId: {0}".format(item["documentId"]))
                        self.articles.insert_one(cont)

                    if test_dict["articles_sqlike"] and cont["systemdata"]["contentType"] == "regular":
                        logging.info("replacing in article-sqlike collection, id: {0}".format(item["documentId"]))
                        self.articles_sqlike.replace_one({"id": item["documentId"]}, self.transform_obj(cont))

                    elif cont["systemdata"]["contentType"] == "regular":
                        logging.info("inserting in article-sqlike collection , id: {0}".format(item["documentId"]))
                        self.articles_sqlike.insert_one(self.transform_obj(cont))

        # sanity checks:
        self._remove_duplicates_articles()

    def create_kafka_logs(self, start_id=0):

        """creates kafka_logs collection
        :param start_id - integer """

        log_list = self._retrieve_logs(start_id)[1]
        while log_list:
            self.kafka_logs.insert_many(log_list)
            start_id = log_list[-1]["id"]
            log_list = self._retrieve_logs(start_id)[1]
        logging.info("kafka_logs: updated last known event id: {0}".format(
            next(self.kafka_logs.find().sort([("id", -1)]))["id"]))

    def update_kafka_logs(self):

        """script for updating the logs"""

        last = next(self.kafka_logs.find().sort([("id", -1)]))["id"]
        logging.info("kafka_logs: current last known event id: {0}".format(last))
        self.create_kafka_logs(last)
        return last

    def _retrieve_logs(self, pub_event_id, lim=1000):

        """method to retrieve logs from the server
        :PARAMs pub_event_id - an integer ; lim - int, number of an articles for pulling.
        "RETURNS a tuples: (status ,list) """

        url = 'https://api.berliner-zeitung.de/api/v1/publicationEvents?after={1}&limit={2}&access_token={0}'.format(
            self.api_key1, pub_event_id, lim)
        req = requests.get(url)
        try:
            cont = ast.literal_eval(req.content.decode("UTF-8"))
        except IndexError:
            cont = None
        return req, cont

    def is_exists(self, doc_id):

        """tests whether a document already exists in the database. """

        bool_dict = {}
        if list(self.articles.find({"systemdata.documentId": {"$exists": True, "$in": [doc_id]}},
                                   {'_id': 0, "systemdata.documentId": 1})):
            bool_dict["articles"] = True
        else:
            bool_dict["articles"] = False

        if list(self.articles_sqlike.find({"id": {"$exists": True, "$in": [doc_id]}}, {'_id': 0, "id": 1})):
            bool_dict["articles_sqlike"] = True
        else:
            bool_dict["articles_sqlike"] = False

        return bool_dict

    def transform_obj(self, item):

        """converts a NoSQL item into SQLike item. """
        try:
            return {"id": item["systemdata"]["documentId"], "url": item["metadata"]["routing"]["path"],
                    "title": item["metadata"]["title"],
                    "lead": self.find_lead_p(str(item["livingdoc"]["content"])),
                    "author": self.find_author(str(item["livingdoc"]["content"])),
                    "publishdate": item["metadata"]["publishDate"],
                    "text": self.extract_text_from_livingdoc_obj(str(item["livingdoc"]["content"])),
                    "language": item["metadata"]["language"]["label"],
                    "image_url": item["metadata"]["teaserImage"]["url"]}
        except KeyError:
            try:
                return {"id": item["systemdata"]["documentId"], "url": item["metadata"]["routing"]["path"],
                    "title": item["metadata"]["title"],
                    "lead": self.find_lead_p(str(item["livingdoc"]["content"])),
                    "author": self.find_author(str(item["livingdoc"]["content"])),
                    "publishdate": item["metadata"]["publishDate"],
                    "text": self.extract_text_from_livingdoc_obj(str(item["livingdoc"]["content"])),
                    "language": item["metadata"]["language"]["label"]}
            except KeyError:
                return {"id": item["systemdata"]["documentId"], "url": item["metadata"]["routing"]["path"],
                        "title": item["metadata"]["title"],
                        "lead": self.find_lead_p(str(item["livingdoc"]["content"])),
                        "author": self.find_author(str(item["livingdoc"]["content"])),
                        "publishdate": item["metadata"]["publishDate"],
                        "text": self.extract_text_from_livingdoc_obj(str(item["livingdoc"]["content"])),
                        "language": "German"}

    @staticmethod
    def extract_text_from_livingdoc_obj(obj):
        start = '\'text\': '
        end = '}}'
        ite = re.finditer(start, obj)
        text_list = [tup.span()[1] for tup in ite]
        text_list_1 = [obj[x: obj[x:].find(end) + x] for x in text_list]
        text_list_2 = "\n".join(
            [term.replace("<strong>", "").replace("</strong>", "").replace("<em>", "").replace("</em>", "") for term
             in text_list_1])
        try:
            return text_list_2[re.search('[a-zA-Z]', text_list_2).span()[0]:]
        except AttributeError:
            return text_list_2

    @staticmethod
    def extract_lead_from_livingdoc_obj(obj):
        start = '\'lead\': '
        end = '\','
        ite = re.finditer(start, obj)
        text_list = [tup.span()[1] for tup in ite]
        text_list_1 = [obj[x: obj[x:].find(end) + x] for x in text_list]
        text_list_2 = "\n".join(
            [term.replace("<strong>", "").replace("</strong>", "").replace("<em>", "").replace("</em>", "") for term
             in text_list_1])
        try:
            return text_list_2[re.search('[a-zA-Z]', text_list_2).span()[0]:]
        except AttributeError:
            return text_list_2

    @staticmethod
    def find_lead_p(string):

        """find lead title within a json string."""

        try:
            rex1 = re.search('lead\': \'', string)
            e1 = rex1.end()
            e2 = string[e1:].find('\',')
            return string[e1:e1 + e2]
        except AttributeError:
            return ''

    @staticmethod
    def find_author(string):

        """find author name within a json string."""

        try:
            rex1 = re.search('author\': \'', string)
            e1 = rex1.end()
            e2 = string[e1:].find('\'}')
            return string[e1:e1 + e2]
        except AttributeError:
            return ''

    def _remove_duplicates_article_sqlike(self):

        """removes duplicates from articles_sqlike (sanity check) """

        duplicates = self.articles_sqlike.aggregate([
            {"$group": {"_id": "$id", "count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}},
            {"$project": {"id": "$_id", "_id": 0}}])

        # extract their documentId's
        dup_l = []
        for item in duplicates:
            dup_l.append(item["id"])

        # remove from database
        logging.info("removing {0} duplicates".format(len(dup_l)))

        while dup_l:
            i = dup_l.pop()
            my_cursor = self.articles_sqlike.find({"id": i}).sort([("_id", -1)])

            # omits the most recent update.
            next(my_cursor)
            for item in my_cursor:
                self.articles_sqlike.delete_one({"_id": item["_id"]})

    def _remove_duplicates_log(self):

        """removes duplicates from kafka_logs (sanity check)"""

        duplicates = self.kafka_logs.aggregate([
            {"$group": {"_id": "$id", "count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}},
            {"$project": {"id": "$_id", "_id": 0}}])

        # extract their documentId's
        dup_l = []
        for item in duplicates:
            dup_l.append(item["id"])

        # remove from database
        logging.info("removing {0} duplicates".format(len(dup_l)))

        while dup_l:
            i = dup_l.pop()
            my_cursor = self.kafka_logs.find({"id": i}).sort([("_id", -1)])

            # omits the most recent update.
            next(my_cursor)
            for item in my_cursor:
                self.kafka_logs.delete_one({"_id": item["_id"]})

    def _remove_duplicates_articles(self):

        """removes duplicates from articles but keeps the last"""

        # search for the duplicates:
        duplicates = self.articles.aggregate([
            {"$group": {"_id": "$systemdata.documentId", "count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}},
            {"$project": {"systemdata.documentId": "$_id", "_id": 0}}])

        # extract their documentId's
        dup_l = []
        for item in duplicates:
            dup_l.append(item["systemdata"]["documentId"])

        # remove from database
        logging.info("removing {0} duplicates".format(len(dup_l)))

        while dup_l:
            i = dup_l.pop()
            my_cursor = self.articles.find({"systemdata.documentId": i}).sort([("_id", -1)])

            # omits the most recent update.
            next(my_cursor)
            for item in my_cursor:
                self.articles.delete_one({"_id": item["_id"]})

    def automation(self, hiatus=1200):

        """this is how we automate the procedure """

        while True:
            self.update_articles()
            logging.info("going to sleep...\n hiatus: {0}".format(hiatus))
            time.sleep(hiatus)
