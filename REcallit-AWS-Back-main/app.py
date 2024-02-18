import os
import json
import base64
from controllers.scraping import get_cpsc_mapping, get_results_category_approx, get_results_category_details, \
                                    get_results_title_details, get_results_manufacturer_approx, get_results_manufacturer_details, \
                                    get_incident_title_details, record_feedback, get_boyandgirl_values, \
                                    get_incident_manufacturer_details, get_forecast_er, get_diagnosisdisposition_values, \
                                    get_smoke_words, get_incident_category_details, get_all

class RecallitRequest:
    path = "/"
    body = {}

    def __init__(self, path, body):
        self.path = path
        self.body = body
    
    def get_json(self):
        return self.body

    def get_path(self):
        return self.path

def jsonify(status, val):
    return {
        'statusCode': status,
        'headers': {
            "Content-Type": "application/json"
        },
        'body': json.dumps(val, default=str)
    }

def lambda_handler(event, context):
    if event['httpMethod'] != 'POST':
        return jsonify(500, {'success': False, 'error': 500})

    path = event['path']
    body = dict()
    if 'body' in event and event['body'] != None:
        if event['isBase64Encoded']:
            body = json.loads(base64.b64decode(event['body']).decode('utf-8'))
        else:
            body = json.loads(event['body'])
    
    print("API request: {}".format(path))
    print("Request body: {}".format(body))
    request = RecallitRequest(path, body)

    if path == '/api/scraping/get/all':
        return jsonify(200, get_all_route(request))
    elif path == '/api/scraping/get/record_feedback':
        return jsonify(200, get_record_feedback_route(request))
    elif path == '/api/scraping/get/cpsc_mapping':
        return jsonify(200, get_cpsc_mapping_route(request))
    elif path == '/api/scraping/get/category_approx':
        return jsonify(200, get_category_approx_route(request))
    elif path == '/api/scraping/get/category_details':
        return jsonify(200, get_category_details_route(request))
    elif path == '/api/scraping/get/manufacturer_approx':
        return jsonify(200, get_manufacturer_approx_route(request))
    elif path == '/api/scraping/get/manufacturer_details':
        return jsonify(200, get_manufacturer_details_route(request))
    elif path == '/api/scraping/get/boyandgirl_values':
        return jsonify(200, get_boyandgirl(request))
    elif path == '/api/scraping/get/diagnosisdisposition_values':
        return jsonify(200, get_diagnosisdisposition(request))
    elif path == '/api/scraping/get/forecast_er':
        return jsonify(200, getforecaster(request))
    elif path == '/api/scraping/get/incident_manufacturer_details':
        return jsonify(200, get_incident_manufacturer_details_route(request))
    elif path == '/api/scraping/get/incident_title_details':
        return jsonify(200, get_incident_title_details_route(request))
    elif path == '/api/scraping/get/incident_category_details':
        return jsonify(200, get_incident_category_details_route(request))
    elif path == '/api/scraping/get/smoke_words':
        return jsonify(200, get_smoke_words_route(request))
    elif path == '/api/scraping/get/version':
        return jsonify(200, {'success': True, 'value': 20210220})
    else:
        return jsonify(404, {'success': False, 'status': 404})

def get_cpsc_mapping_route(request):
    obj = request.get_json()
    ecommerce_platform = obj['ecommerce_platform']
    ecommerce_category_path = obj['ecommerce_category_path']
    return get_cpsc_mapping(ecommerce_platform, ecommerce_category_path)

def get_category_approx_route(request):
    obj = request.get_json(request)
    category = obj['category']
    return get_results_category_approx(category)

def get_category_details_route(request):
    obj = request.get_json()
    category = obj['category']
    return get_results_category_details(category)

def get_manufacturer_approx_route(request):
    obj = request.get_json()
    manufacturer = obj['manufacturer']
    return get_results_manufacturer_approx(manufacturer)

def get_manufacturer_details_route(request):
    obj = request.get_json()
    manufacturer = obj['manufacturer']
    return get_results_manufacturer_details(manufacturer)

def get_boyandgirl(request):
    obj = request.get_json()
    category = obj['category']
    return get_boyandgirl_values(category)

def get_diagnosisdisposition(request):
    obj = request.get_json()
    category = obj['category']
    return get_diagnosisdisposition_values(category)

def getforecaster(request):
    obj = request.get_json()
    productid = obj['productid']
    return get_forecast_er(productid)

def get_incident_manufacturer_details_route(request):
    obj = request.get_json()
    manufacturer = obj['manufacturer']
    return get_incident_manufacturer_details(manufacturer)

def get_incident_title_details_route(request):
    obj = request.get_json()
    title = obj['title']
    return get_incident_title_details(title)

def get_incident_category_details_route(request):
    obj = request.get_json()
    category = obj['category']
    return get_incident_category_details(category)

def get_record_feedback_route(request):
    obj = request.get_json()
    rating = obj['rating']
    name = obj['name']
    email = obj['email']
    comment = obj['comment']
    return record_feedback(rating, name, email, comment)

def get_smoke_words_route(request):
    return get_smoke_words()

def get_all_route(request):
    obj = request.get_json()
    by = obj['by']
    title = obj['title']
    category = obj['category']
    domain = obj['domain']
    return get_all(by, title, category, domain)

