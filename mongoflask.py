from flask import Flask, jsonify
from pymongo import MongoClient
#from bson.json_util import dumps, loads
#import json
app = Flask(_name_)

client = MongoClient("mongodb://nagarjuna.kappurapu:c2LAyJLtPONLZ8ja@qa-mcsmongo01.aws.phenom.local:27017")
db = client["mongo_deloitteg_qa"]
collection = db["applyeventslogs"]

@app.route('/') #by using this route we activate GET method automatically
def get_data():
    
    filter={    'uid': "e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435"}
    project={    '_id':0,'kafkaSuccessDate': 1,     'eventSubmittedPayLoad': 1,     'eventStatus': 1,     'candidateId': 1,     'eventName': 1,     'refNum': 1}
    result = client['mongo_deloitteg_qa']['applyeventslogs'].find(  filter=filter,  projection=project)
    res = [r for r in result]
    count_servicedata = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_service_data"})
    count_successdata = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_thankYou"})
    count_thankyou    = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_success_data"})
    return jsonify(res,count_servicedata,count_thankyou,count_successdata)

@app.route('/status1')
def get_status_servicedata():
    success     = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_service_data","eventStatus":"SUCCESS"})   
    reprocessed = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_service_data","eventStatus":"REPROCESSED"})
    reprocess_fail = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_service_data","eventStatus":"REPROCESS_FAIL"})
    return jsonify (success,reprocessed,reprocess_fail)
@app.route('/status2')
def get_status_thankyou():
    
     success     = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_thankYou","eventStatus":"SUCCESS"})   
     reprocessed = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_thankYou","eventStatus":"REPROCESSED"})
     reprocess_fail = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_thankYou","eventStatus":"REPROCESS_FAIL"})
     return jsonify(success,reprocessed,reprocess_fail)
@app.route('/status3')
def get_status_successdata():
     success_2     = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_success_data","eventStatus":"SUCCESS"})   
     reprocessed_2 = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_success_data","eventStatus":"REPROCESSED"})
     reprocess_fail_2 = collection.count_documents({"uid":"e3ddb723-7fad-4981-83ae-24ac844d346b183d0e0c435","eventName" :"apply_success_data","eventStatus":"REPROCESS_FAIL"})
     return jsonify(success_2,reprocessed_2,reprocess_fail_2)
if _name_ == '_main_':
    app.run(host="0.0.0.0", port=5004, debug=True)