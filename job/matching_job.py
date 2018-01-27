#-*- coding: utf-8 -*-
import os
from math import ceil
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import scan


class Matching(object):
    data = {}
    filter = []
    must = []
    should = []
    minimum_should_match = 1
    results = []
    query = {}
    es = None

    size = 20
    page = 1
    scroll = None
    scroll_id = None
    scroll_size = 2000

    def __init__(self, job, size, page, search, scroll, scroll_id):
        if size:
            self.size = size
        if page:
            self.page = page

        self.search = search
        self.scroll = scroll
        self.scroll_id = scroll_id

        self.es = Elasticsearch(
            [os.environ["NAME_ES_DOMAIN"]],
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

        self.reset()
        self.get_data(job)
        self.create_request()
        self.create_query()
        self.execute_query()


    def reset(self):
        self.data = {}
        self.filter = []
        self.must = []
        self.should = []
        self.results = []
        self.query = {}


    def return_response(self):
        if self.scroll:
            hits = self.results['hits']['hits'] if "hits" in self.results['hits'] else []
            response = {
                'results': hits,
                'scroll': self.results['_scroll_id'] if len(hits) >= self.scroll_size else None,
                'max_score': self.results['hits']['max_score']
            }
        else:
            response = {
                'total': self.results['hits']['total'],
                'pages': int(ceil(self.results['hits']['total'] / float(self.size))),
                'results': [],
                'next' : self.page + 1 if self.page < int(ceil(self.results['hits']['total'] / float(self.size))) else None,
                'prev' : self.page - 1 if self.page > 1 else None
            }
            for res in self.results['hits']['hits']:
                obj = res['_source']
                obj['id'] = res['_id']
                obj['matching'] = int(res['_score'] / self.results['hits']['max_score'] * 100)
                response['results'].append(obj)

        return response


    def get_from(self):
        if self.page <= 1:
            return 0
        return (self.page - 1) * self.size


    def execute_query(self):
        if self.scroll :
            if self.scroll_id:
                self.results = self.es.scroll(scroll="2m", scroll_id=self.scroll_id, filter_path=['hits.hits._id', 'hits.hits._score', 'hits.max_score', '_scroll_id'])
            else:
                self.results = self.es.search(
                    index='matching',
                    doc_type='applicant',
                    scroll="2m",
                    size=self.scroll_size,
                    body=self.query,
                    filter_path=['hits.hits._id', 'hits.hits._score', 'hits.max_score', '_scroll_id'])
        else:
            self.results = self.es.search(
                index='matching',
                doc_type='applicant',
                size=self.size,
                from_ =self.get_from(),
                body=self.query,
                filter_path=[
                    'hits.total',
                    'hits.hits._score',
                    'hits.max_score',
                    'hits.hits._id',
                    'hits.hits._source.first_name',
                    'hits.hits._source.last_name',
                    'hits.hits._source.title',
                    'hits.hits._source.location',
                ])


    def create_query(self):
        self.query = {
            "query": {
                "bool": {
                    "filter": self.filter,
                    "should": self.should,
                    "minimum_should_match" : self.minimum_should_match,
                    "must": self.must
                }
            }
        }


    def create_request(self):
        self.filter_location()
        self.filter_contract()
        self.filter_search()
        self.should_title()
        self.should_tags()
        self.calculate_minimum_should_match()


    def calculate_minimum_should_match(self):
        minimum = int(len(self.data['tags']) / 3)
        if minimum > 1:
            self.minimum_should_match = minimum


    def get_data(self, job):
        res = self.es.get(index="matching", doc_type='job', id=job)
        self.data = res['_source']


    def filter_location(self):
        if "location"  in self.data:
            if "locality" in self.data['location']:
                request = {"match" : { "location.locality" : self.data['location']['locality'] }}
                self.filter.append(request)


    def filter_contract(self):
        if "contracts_wanted" in self.data:
            ct = " ".join(self.data['contracts_wanted'])
            request = {"match": {"wanted_contracts": ct}}
            self.filter.append(request)


    def should_title(self):
        self.should.append({
              "simple_query_string": {
                  "query": self.data['title'],
                  "fields" : ["wanted_jobs"],
                  "boost" : 2
            }
        })


    def filter_search(self):
        if self.search:
            self.filter.append({
                "match" : {
                    "_all" : self.search
                }
            })



    def should_tags(self):
        for tag in self.data['tags']:
            self.should.append( {"match_phrase": {"wanted_skills": tag}} )




def lambda_handler(event, context):
    if "job" in event:
        job = event['job']
        search = event['search'] if "search" in event else None
        size = event['size'] if "size" in event else None
        page = event['page'] if "page" in event else None
        scroll = event['scroll'] if "scroll" in event else None
        scroll_id = event['scroll_id'] if "scroll_id" in event else None
        matching = Matching(job, size, page, search, scroll, scroll_id)
        return matching.return_response()
    else:
        return "error"
