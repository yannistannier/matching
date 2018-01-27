import psycopg2
import boto3
import json
import time
import os


class Candidacy(object):
    conn = None
    host = None
    database = None
    user = None
    password = None
    port = None
    id = None
    event = {
        "ApplicantMatchingWasRequested" : {"lambda" : os.environ["LAMBDA_MATCHING_JOB"], "field" : "job", "insert": "job_id, applicant_id" },
        "JobMatchingWasRequested" : {"lambda" : os.environ["LAMBDA_MATCHING_APPLICANT"], "field" : "applicant", "insert": "applicant_id, job_id" },
    }
    matching = None
    max_score = 0
    results = []

    def __init__(self, id, event):
        self.reset()
        self.id = id
        self.matching = self.event[event]
        self.set_variables()
        self.get_matching()
        self.insert()


    def reset(self):
        self.id = None
        self.max_score = 0
        self.results = []
        self.matching = None


    def set_variables(self):
        self.host = os.environ["HOST"]
        self.database = os.environ["DATABASE"]
        self.user = os.environ["USER"]
        self.password = os.environ["PASSWORD"]
        self.port = os.environ["PORT"]



    def insert(self):
        conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        cur = conn.cursor()

        if self.results:

            datas = ','.join(
                cur.mogrify(
                    "(%s, %s, %s,'M', now())",
                    (int(row['_score'] / self.max_score * 100), self.id, row['_id'])) for row in self.results if int(row['_score'] / self.max_score * 100) > 10)

            cur.execute('insert into candidacy_candidacy (matching_score, ' + self.matching['insert'] + ', status, date_matching) values ' + datas + ' ON CONFLICT (job_id, applicant_id) DO NOTHING ')
            conn.commit()

        conn.close()


    def get_matching(self, scroll_id=None):
        lm = boto3.client("lambda")

        payload = {
            "job" : int(self.id),
            "scroll" : True,
            "scroll_id" : scroll_id
        }

        response = lm.invoke(FunctionName=self.matching['lambda'], InvocationType='RequestResponse', Payload=json.dumps(payload))
        res = json.load(response['Payload'])

        self.max_score = res['max_score']
        self.results =  self.results + res['results']

        if "scroll" in res:
            if res['scroll']:
                self.get_matching(res['scroll'])




def lambda_handler(event, context):
    dynamodb = boto3.session.Session().resource('dynamodb')
    table = dynamodb.Table(os.environ["NAME_DYNAMODB_TABLE"])

    reponse = table.get_item(Key={'type': 'MatchingEvent', 'uuid': event['uuid']})
    res = reponse['Item']

    cd = Candidacy(res['id'], res['event'])
    
    table.update_item(
        Key={
            'type': 'MatchingEvent',
            'uuid': event['uuid']
        },
        UpdateExpression="set is_read = :val, payload.count_matching = :tt  ",
        ExpressionAttributeValues={
            ':val': True,
            ':tt' : len(cd.results)
        }
    )




