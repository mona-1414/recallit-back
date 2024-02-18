from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from urllib.parse import unquote
import requests
import json
import time
import re
from pymongo import MongoClient, InsertOne
from rapidfuzz import process
from datetime import datetime
import re
import cleanco
import inflect
import boto3
import base64
import elasticache_auto_discovery

from hashlib import sha256
from botocore.exceptions import ClientError
from pymemcache.client.hash import HashClient
from pymemcache import serde

class recallit_cache(object):
    cache_prefix = ""
    mc = None

    def __init__(self, nodes, cache_prefix):
        self.cache_prefix = cache_prefix
        self.mc = HashClient(nodes, serializer=serde.python_memcache_serializer, deserializer=serde.python_memcache_deserializer)

    def __call__(self, f):
      def cache_check(*args):
        key = sha256((self.cache_prefix + str(args)).encode('utf-8')).hexdigest()
        cached_val = None
        
        try:
          cached_val = self.mc.get(key)
        except:
          pass

        if cached_val is None:
          res = f(*args)
        
          try:
            self.mc.set(key, res, expire = 24*60*60*3)
          except:
            pass
        
          return res
        else:
            return cached_val
      return cache_check


def get_secret():
    secret_name = "kid-backend-secrets"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    secret = dict()

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return secret
            

secrets = json.loads(get_secret())
Client = MongoClient(secrets['mongodb_secret'])
nodes = elasticache_auto_discovery.discover(secrets['es_endpoint'])
nodes = list(map(lambda x: (x[1].decode('utf-8'), int(x[2])), nodes))

db = Client["recallit"]
recall = db["recall_extracts"]
neiss = db["neiss_extracts"]
neiss_mappings = db["neiss_mappings"]
neiss_forecast = db["neiss_forecast"]
category_mapping = db["category_mapping"]
feedback = db["feedback"]
smoke_words = db["smoke_words"]
db_test = db['manufacturer_approx']
application_key = secrets['appkey_secret']

BY_ERROR = [
	'Small gray toddler recliner folding recliner chair ', 
	'combination expresso and coffee maker',
	'Kids II Recalls Baby Einstein Activity Jumpers Due to Impact Hazard; Sun Toy Can Snap Backward ',
	'Elise Youth Bunk Bed in Soft White purchased from Walmart: http://www.walmart.com/ip/Elise-Bunk-Bed-Soft-White/10575812',
	'Eastern King,  9 foot high dark gray wingback heavy tufted headboard and siderails',
	'7" Glow Stick'
]

@recallit_cache(nodes, "get_results_title")
def get_results_title(title):
  print('get_results_title: {}'.format(title))
  try:
    payload_title = {'format': 'json', 'RecallTitle': title}
    r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_title)
    data = r.json()
    results_title = []
    ProductsNames = recall.find({}).distinct("Products.Name")
    if list(process.extractOne(title, ProductsNames))[1] > 88:
      title = process.extractOne(title, ProductsNames)[0]
      payload_title = {'format': 'json', 'RecallTitle': title}
      r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_title)
      data = data + r.json()
    for i in data:
      if i not in results_title:
        if 'Images' in i.keys() and len(i["Images"]) > 0:
          img = i["Images"][0]["URL"]
          try:
            results_title.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name'], "image": img})
          except IndexError:
            results_title.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})

        else:
          try:
            results_title.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name']})
          except IndexError:
            results_title.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})
    return {
      'success': True,
      'count_title': len(results_title),
      'results_title': results_title
    }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_results_by")
def get_results_by(by):
  print('get_results_by: {}'.format(by))
  try:
    payload_by = {'format': 'json', 'RecallTitle': by}
    r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_by)
    data = r.json()
    results_by = []
    ManufacturesName = recall.find({}).distinct("Manufacturers.Name")
    if list(process.extractOne(by, ManufacturesName))[1] > 88:
      by = process.extractOne(by, ManufacturesName)[0]
      payload_by = {'format': 'json', 'RecallTitle': by}
      r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_by)
      data = data + r.json()
    for i in data:
      if i not in results_by:
        if 'Images' in i.keys() and len(i["Images"]) > 0:
          img = i["Images"][0]["URL"]
          try:
            results_by.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name'], "image": img})
          except IndexError:
            results_by.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})

        else:
          try:
            results_by.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name']})
          except IndexError:
            results_by.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})
    return {
      'success': True,
      'count_by': len(results_by),
      'results_by': results_by
    }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_results_categories")
def get_results_categories(categories):
  print('get_results_categories: {}'.format(categories))
  try:
    payload_category = {'format': 'json', 'RecallTitle': categories[len(categories) - 1]}
    r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_category)
    data = r.json()
    k = 1
    results_category = []
    ProductsTypes = recall.find({}).distinct("Products.Type")
    if list(process.extractOne(categories[len(categories) - 1], ProductsTypes))[1] > 88:
      category = process.extractOne(categories[len(categories) - 1], ProductsTypes)[0]
      payload_category = {'format': 'json', 'RecallTitle': category}
      r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_category)
      data = data + r.json()
    for i in data:
      if i not in results_category:
        if 'Images' in i.keys() and len(i["Images"]) > 0:
          img = i["Images"][0]["URL"]
          try:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name'], "image": img})
          except IndexError:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})

        else:
          try:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name']})
          except IndexError:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})
    while len(results_category) == 0 and k < len(categories):
      p = {'format': 'json', 'RecallTitle': categories[len(categories) - k]}
      r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=p)
      data = r.json()
      if list(process.extractOne(categories[len(categories) - k], ProductsTypes))[1] > 88:
        category = process.extractOne(categories[len(categories) - k], ProductsTypes)[0]
        payload_category = {'format': 'json', 'RecallTitle': category}
        r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_category)
        data = data + r.json()
      for i in data:
        if 'Images' in list(i.keys()) and len(i["Images"]) > 0:
          img = i["Images"][0]["URL"]
          try:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name'], "image": img})
          except IndexError:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})

        else:
          try:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name']})
          except IndexError:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})
      k = k + 1
    return {
      'success': True,
      'count_category': len(results_category),
      'results_category': results_category
    }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_cpsc_mapping")
def get_cpsc_mapping(ecommerce_platform, ecommerce_category_path):
  print('get_cpsc_mapping: {} and {}'.format(ecommerce_platform, ecommerce_category_path))
  try:
    if ecommerce_platform.lower() == 'amazon':
      cpsc_category = category_mapping.find_one({'ecommerce_platform':ecommerce_platform.lower(), 'ecommerce_category_path':ecommerce_category_path.replace("%20%20%20", " & ").replace("%20", " ").replace("_", "/")})['cpsc_category']
      return {
        'success': True,
        'cpsc_category': cpsc_category
      }
    else:
      return {
        'success': False
      }
  except:
    return {
      'success': False
    }


@recallit_cache(nodes, "get_results_category_approx")
def get_results_category_approx(category):
  print('get_results_category_approx: {}'.format(category))
  try:
    if category.lower() == 'unbranded':
      return {'success': False}
    
    ProductsTypes = recall.find({}).distinct("Products.Type")
    if list(process.extractOne(category, ProductsTypes))[1] > 78:
      category_approx = process.extractOne(category, ProductsTypes)[0]
      return {
        'success': True,
        'category_approx': category_approx
      }
    else:
      return {
        'success': False
      }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_results_category_details")
def get_results_category_details(category):
  print('get_results_category_details: {}'.format(category))
  try:
    payload_category = {'format': 'json', 'RecallTitle': category}
    r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_category)
    data = r.json()

    payload_category = {'format': 'json', 'ProductType': category}
    r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_category)
    data = data + r.json()
    
    results_category = []
    for i in data:
      if i not in results_category:
        img = []
        if 'Images' in list(i.keys()) and len(i["Images"]) > 0:
          img = i["Images"][0]["URL"]
          try:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name'], "image": img})
          except IndexError:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})

        else:
          try:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name']})
          except IndexError:
            results_category.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})
    return {
      'success': True,
      'count_category': len(results_category),
      'results_category': results_category
    }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "two_sentences_match")
def two_sentences_match(a, b):
  text = inflect.engine()
  extraneous = ["recall", "recalls", "due", "to", "a", "the", "hazard", "", "and", "of", "cpsc", "announce", "announces"]
  colors = [
    'beige',
    'black',
    'blue',
    'brown',
    'coral',
    'cyan',
    'gold',
    'gray',
    'grey',
    'green',
    'orange',
    'pink',
    'purple',
    'red',
    'silver',
    'violet',
    'yellow'
  ]
  a = re.sub('[^A-Za-z0-9]+', ' ', a.lower().replace(" ", "thisisaspace")).replace("thisisaspace", " ").replace("  ",  " ").split(" ")
  b = re.sub('[^A-Za-z0-9]+', ' ', b.lower().replace(" ", "thisisaspace")).replace("thisisaspace", " ").replace("  ", " ").split(" ")
  # converts each word in both lists to their singular form
  for i in [a, b]:
      for j in i:
          if j[-2:] == 'ss':
              pass
          elif text.singular_noun(j) != False:
              i[i.index(j)] = text.singular_noun(j)
  # removes extraneous words in both lists
  for word in extraneous + colors:
      a = list(filter(word.__ne__, a))
      b = list(filter(word.__ne__, b))
  ratio = len(set(a).intersection(b)) / max(min(len(a), len(b)), 3)
  return ratio

@recallit_cache(nodes, "get_results_title_details")
def get_results_title_details(title, data):
  print('get_results_title_details: {}'.format(title))
  try:
    results_title = []
    for i in data:
      score = two_sentences_match(i['title'], title)
      if score >= 0.6 and i['title'] not in [d['title'] for d in results_title if 'title' in d]:
        results_title.append(i)
    return {
      'success': True,
      'count_title': len(results_title),
      'results_title': results_title
    }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_results_manufacturer_approx")
def get_results_manufacturer_approx(manufacturer):
  print('get_results_manufacturer_approx: {}'.format(manufacturer))
  res0 = old_get_results_manufacturer_approx(manufacturer)
  res1 = new_get_results_manufacturer_approx(manufacturer)
  res2 = new2_get_results_manufacturer_approx(manufacturer)

  res = res1
  if res1['success'] == False:
    res = res2

  db_test.insert_one({
    "val": manufacturer,
    "res0": res0,
    "res1": res1,
    "res2": res2,
    "res": res
  })
  print("Results of manufacturer approx: {}".format(res))
  return res

@recallit_cache(nodes, "old_get_results_manufacturer_approx")
def old_get_results_manufacturer_approx(manufacturer):
  try:
    manufacturer = manufacturer.lower()
    manufacturer_cleaned = manufacturer
    tags = ['children', 'child', 'kids', 'kid', 'babies', 'baby', 'brand: ', 'visit the ', ' store', 'unbranded']
    for tag in tags:
      if tag in manufacturer_cleaned:
        manufacturer_cleaned = manufacturer_cleaned.replace(tag, '')

    ManufacturesName = recall.find({}).distinct("Manufacturers.Name")
    temp_cleaned = process.extractOne(manufacturer_cleaned, ManufacturesName)
    temp = process.extractOne(manufacturer, ManufacturesName)
    if list(temp_cleaned)[1] > 78:
      manufacturer_approx = temp_cleaned[0]
      return {
        'success': True,
        'manufacturer_approx': manufacturer_approx
      }
    elif list(temp)[1] > 78:
      manufacturer_approx = temp[0]
      return {
        'success': True,
        'manufacturer_approx': manufacturer_approx
      }
    else:
      return {
        'success': False
      }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_results_manufacturer_details")
def get_results_manufacturer_details(manufacturer):
  print('get_results_manufacturer_details: {}'.format(manufacturer))
  try:
    payload_manufacturer = {'format': 'json', 'RecallTitle': manufacturer}
    r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_manufacturer)
    data = r.json()

    payload_manufacturer = {'format': 'json', 'Manufacturer': manufacturer}
    r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_manufacturer)
    data = data + r.json()

    if " " in manufacturer:
      payload_manufacturer = {'format': 'json', 'RecallTitle': manufacturer.replace(' ', '')}
      r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_manufacturer)
      data = data + r.json()

      payload_manufacturer = {'format': 'json', 'Manufacturer': manufacturer.replace(' ', '')}
      r = requests.get('https://www.saferproducts.gov/RestWebServices/Recall', params=payload_manufacturer)
      data = data + r.json()
    
    results_manufacturer = []
    for i in data:
      if i not in results_manufacturer:
        if 'Images' in list(i.keys()) and len(i["Images"]) > 0:
          img = i["Images"][0]["URL"]
          try:
            results_manufacturer.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name'], "image": img})
          except IndexError:
            results_manufacturer.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})

        else:
          try:
            results_manufacturer.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "hazard": i['Hazards'][0]['Name']})
          except IndexError:
            results_manufacturer.append({"title": i["Title"], "url": i["URL"], "description": i["Description"],
                        "recalldate": i["RecallDate"], "image": img})

    sort_results_manufacturer = sorted(results_manufacturer,
         key=lambda k: datetime.strptime(k['recalldate'].replace('T', ' '), '%Y-%m-%d %H:%M:%S'),
         reverse=True)
    return {
      'success': True,
      'count_manufacturer': len(results_manufacturer),
      'results_manufacturer': sort_results_manufacturer
    }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_boyandgirl_values")
def get_boyandgirl_values(category):
  print('get_boyandgirl_values: {}'.format(category))
  if category[-1:] == 's':
    category = category[:-1]
  pipeline_boy = [{ "$match": { "$and": [{'Narrative_1':{'$regex': category, '$options': 'i'}}, {'Sex':1} ] } }
      ,{"$unwind": "$Age_cleaned"}
      ,{"$group": {"_id": "$Age_cleaned", "count": {"$sum": 1}}}
  ]

  pipeline_girl = [{ "$match": { "$and": [{'Narrative_1':{'$regex': category, '$options': 'i'}}, {'Sex':2} ] } }
        ,{"$unwind": "$Age_cleaned"}
        ,{"$group": {"_id": "$Age_cleaned", "count": {"$sum": 1}}}
  ]

  boy_dic = {}
  girl_dic = {}
  boy_values = []
  girl_values = []
  age_band = [0,1,2,3,4,5,6,7,8,9,10,11,12]

  for i in neiss.aggregate(pipeline_boy):
    if i['_id'] <= 12:
      boy_dic[i['_id']] = i['count']

  for i in age_band:
    if i not in boy_dic.keys():
      boy_dic[i] = 0

  for i in neiss.aggregate(pipeline_girl):
    if i['_id'] <= 12:
      girl_dic[i['_id']] = i['count']

  for i in age_band:
    if i not in girl_dic.keys():
      girl_dic[i] = 0

  for i in sorted (boy_dic):
    boy_values.append(boy_dic[i])

  for i in sorted (girl_dic):
    girl_values.append(girl_dic[i])

  return {
    'success': True,
    'boy_values': boy_values,
    'girl_values': girl_values
  }

@recallit_cache(nodes, "get_diagnosisdisposition_values")
def get_diagnosisdisposition_values(category):
  print('get_diagnosisdisposition_values: {}'.format(category))
  if category[-1:] == 's':
    category = category[:-1]
  pipeline_diagnosis = [{ "$match": {'Narrative_1':{'$regex': category, '$options': 'i'}}}
        ,{"$unwind": "$Diagnosis"}
        ,{"$group": {"_id": "$Diagnosis", "count": {"$sum": 1}}}
  ]

  pipeline_disposition = [{ "$match": {'Narrative_1':{'$regex': category, '$options': 'i'}}}
        ,{"$unwind": "$Disposition"}
        ,{"$group": {"_id": "$Disposition", "count": {"$sum": 1}}}
  ]

  pipeline_category = [{ "$match": {'Narrative_1':{'$regex': category, '$options': 'i'}}}
        ,{"$unwind": "$Product_1"}
        ,{"$group": {"_id": "$Product_1", "count": {"$sum": 1}}}
  ]

  diagnosis_mapping = neiss_mappings.find_one({})['allTypesOfDiagnosis']
  disposition_mapping = neiss_mappings.find_one({})['allTypesOfDisposition']
  denominator = neiss.find({'Narrative_1':{'$regex': category, '$options': 'i'}}).count()

  colors = ['#0085C3', '#FFD202', '#EC7063', '#A569BD', '#5DADE2', '#45B39D', '#58D68D', '#F4D03F'
      , '#EB984E', '#5D6D7E', '#E9967A', '#27E6F3']

  diagnosis_colors = []
  diagnosis_dic = {}
  n = 0
  for i in neiss.aggregate(pipeline_diagnosis):
    if i['count']/denominator >= 0.1:
      diagnosis_dic[diagnosis_mapping[str(i['_id'])]] = i['count']
      diagnosis_colors.append(colors[n])
      n += 1
  diagnosis_dic['Other'] = denominator - sum(diagnosis_dic.values())
  diagnosis_colors.append('#AAB7B8')

  disposition_colors = []
  disposition_dic = {}
  n = 0
  for i in neiss.aggregate(pipeline_disposition):
    if i['count']/denominator >= 0.1:
      disposition_dic[disposition_mapping[str(i['_id'])]] = i['count']
      disposition_colors.append(colors[n])
      n += 1
  disposition_dic['Other'] = denominator - sum(disposition_dic.values())
  disposition_colors.append('#AAB7B8')
  
  productid_freq = 0
  productid = "0"

  for i in neiss.aggregate(pipeline_category):
    if i['count'] > productid_freq:
      productid = str(i['_id'])
      productid_freq = i['count']

  return {
    'success': True,
    'actual': denominator,
    'diagnosis_values': [list(diagnosis_dic.keys()), list(diagnosis_dic.values()), diagnosis_colors],
    'disposition_values': [list(disposition_dic.keys()), list(disposition_dic.values()), disposition_colors],
    'forecastproductid': productid
  }

@recallit_cache(nodes, "get_forecast_er")
def get_forecast_er(productid):
  print('get_forecast_er: {}'.format(productid))
  estimate = 0
  name = ""
  for i in neiss_forecast.find({'Product_code':int(productid)}):
    estimate += i["National Estimate"]
    name = i["Product_name"]
  return {
    'success': True,
    'forecastestimate': estimate,
    'forecastproductname': name
  }

@recallit_cache(nodes, "get_incident_manufacturer_details")
def get_incident_manufacturer_details(manufacturer):
  print('get_incident_manufacturer_details: {}'.format(manufacturer))
  try:
    r = requests.get('https://www.saferproducts.gov/webapi/Cpsc.Cpsrms.Web.Api.svc/IncidentDetails?$filter=ProductManufacturerName eq '+ "'"+manufacturer+"'&$orderby=IncidentDate desc",
             auth=(application_key, ''),
             headers={'Accept': 'application/json'}
             )
    data = r.json()

    r2 = requests.get(
      'https://www.saferproducts.gov/webapi/Cpsc.Cpsrms.Web.Api.svc/IncidentDetails?$filter=ProductBrandName eq ' + "'" + manufacturer + "'&$orderby=IncidentDate desc",
      auth=(application_key, ''),
      headers={'Accept': 'application/json'}
      )
    data2 = r2.json()

    results_manufacturer = []
    for i in data["d"]["results"]:
      i_time = datetime.fromtimestamp(int(i["IncidentDate"][6:-2])//1000.0)
      results_manufacturer.append(
        {"IncidentProductDescription": i["IncidentProductDescription"], "IncidentDate": i_time, "IncidentDescription": i["IncidentDescription"]})

    for i in data2["d"]["results"]:
      if i not in data["d"]["results"]:
        i_time = datetime.fromtimestamp(int(i["IncidentDate"][6:-2]) // 1000.0)
        results_manufacturer.append(
          {"IncidentProductDescription": i["IncidentProductDescription"], "IncidentDate": i_time,
           "IncidentDescription": i["IncidentDescription"]})

    return {
      'success': True,
      'count_manufacturer': len(results_manufacturer),
      'results_manufacturer': results_manufacturer
    }
  except Exception as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_incident_title_details")
def get_incident_title_details(title):
  print('get_incident_title_details: {}'.format(title))
  title = title.replace("'", "").replace("\"", "")
  try:
    r = requests.get(
      'https://www.saferproducts.gov/webapi/Cpsc.Cpsrms.Web.Api.svc/IncidentDetails?$filter=indexof(IncidentProductDescription,' + "'" + title + "') gt -1&$orderby=IncidentDate desc",
      auth=(application_key, ''),
      headers={'Accept': 'application/json'}
      )
    data = r.json()

    results_manufacturer = []
    for i in data["d"]["results"]:
      i_time = datetime.fromtimestamp(int(i["IncidentDate"][6:-2]) // 1000.0)
      results_manufacturer.append(
        {"IncidentProductDescription": i["IncidentProductDescription"], "IncidentDate": i_time,
         "IncidentDescription": i["IncidentDescription"]})
    return {
      'success': True,
      'count_title': len(results_manufacturer),
      'results_title': results_manufacturer
    }
  except ValueError as e:
    return {'error': str(e)}

@recallit_cache(nodes, "get_incident_category_details")
def get_incident_category_details(category):
  print('get_incident_title_details: {}'.format(category))
  try:
    r = requests.get(
      'https://www.saferproducts.gov/webapi/Cpsc.Cpsrms.Web.Api.svc/ProductCategories?$expand=IncidentDetails&$filter=indexof(ProductCategoryPublicName,' + "'" + category + "') gt -1",
      auth=(application_key, ''),
      headers={'Accept': 'application/json'}
      )
    data = r.json()

    results_categories = []
    for i in data["d"]["results"]:
      for j in i["IncidentDetails"]["results"]:
        j_time = datetime.fromtimestamp(int(j["IncidentDate"][6:-2]) // 1000.0)
        results_categories.append({"IncidentProductDescription": j["IncidentProductDescription"], "IncidentDateOrigins": j["IncidentDate"],"IncidentDate":j_time,
           "IncidentDescription": j["IncidentDescription"]})

    results_categories.sort(key = lambda x:x["IncidentDateOrigins"],reverse=True)

    return {
      'success': True,
      'count_category': len(results_categories),
      'results_category': results_categories
    }
  except Exception as e:
    return {'error': str(e)}

def record_feedback(rating, name, email, comment):
  feedback.insert_one({'date': datetime.now(), 'rating': rating, 'name': name, 'email': email, 'comment': comment})
  return {'success': True}

@recallit_cache(nodes, "get_smoke_words")
def get_smoke_words():
  return {
      'success': True,
      'smoke_words': smoke_words.find_one({})['smoke_words']
  }

@recallit_cache(nodes, "get_all")
def get_all(by, title, category, domain):
  print('get_all')
  categories = category.split("_")
  categories.reverse()

  manufacturerApprox = get_results_manufacturer_approx(unquote(by))

  if manufacturerApprox['success'] != False:
    manufacturerDetails = get_results_manufacturer_details(manufacturerApprox['manufacturer_approx'])
  else:
    manufacturerDetails = {
      'success': True,
      'count_manufacturer': 0,
      'results_manufacturer': []
    }

  categoryDetails = any
  categoryApprox = any
  reportCategory = list()
  categoryApprox =  get_cpsc_mapping(domain, category)
  if categoryApprox['success']:
    cpsc_category = categoryApprox.get('cpsc_category', "")
    categoryDetails = get_results_category_details(cpsc_category)
    barChartValues = get_boyandgirl_values(cpsc_category)
    pieChartValue = get_diagnosisdisposition_values(cpsc_category)

    reportCategoryRes = get_incident_category_details(cpsc_category)
    if reportCategoryRes.get('success', False):
      reportCategory = reportCategoryRes.get('results_category', list())
    disclaimerTrigger = cpsc_category
  else:
    for c in categories:
      approx_category = categoryApprox.get("category_approx", unquote(c))
      categoryApprox = get_results_category_approx(unquote(c))
      category_val = categoryApprox.get('category_approx', "")
      categoryDetails = get_results_category_details(category_val)
      if 'results_category' in categoryDetails:
        if len(categoryDetails['results_category']) > 0:
          break
    barChartValues = get_boyandgirl_values(approx_category)
    pieChartValue = get_diagnosisdisposition_values(approx_category)

    reportCategoryRes = get_incident_category_details(categoryApprox.get('category_approx', ''))
    if reportCategoryRes.get('success', False):
      reportCategory = reportCategoryRes.get('results_category', list())
    disclaimerTrigger = categoryApprox.get('category_approx', '')
  
  productsByType = uniqData(categoryDetails.get('results_category', []), 'url')
  productsByManufacturer = uniqData(manufacturerDetails.get('results_manufacturer', []), 'url')

  data_combined = manufacturerDetails['results_manufacturer'] + categoryDetails['results_category']
  dataByTitle = get_results_title_details(unquote(title), data_combined)

  productsByName = dataByTitle.get('results_title', [])
  forecast = get_forecast_er(pieChartValue.get('forecastproductid', ''))

  if len([i for i in barChartValues['boy_values'] if i != 0]) == 0 and \
     len([i for i in barChartValues['girl_values'] if i != 0]) == 0 and \
     pieChartValue['actual'] == 0:
     dataChatsContent = False
  else:
     dataChatsContent = True

  scrapData = {
    "actual": pieChartValue.get('actual', "0"),
    "forecastEstimate": forecast.get('forecastestimate', "0"),
    "forecastProdName": forecast.get('forecastproductname', "")
  }

  reportManufacturer = list()
  reportManufacturerRes = get_incident_manufacturer_details(manufacturerApprox.get('manufacturer_approx', ''))
  if reportManufacturerRes.get('success', False):
    reportManufacturer = reportManufacturerRes.get('results_manufacturer', list())
  IncidentProductDescription = [rm['IncidentProductDescription'] for rm in reportManufacturer]
  if IncidentProductDescription == BY_ERROR:
    reportManufacturer = list()
  
  reportName = list()
  reportNameRes = get_incident_title_details(unquote(title))
  if reportNameRes.get('success', False):
    reportName = reportNameRes.get('results_title', list())
  
  smokeWords = get_smoke_words()

  return {
    'success': True,
    'all': {
      'productsByManufacturer': productsByManufacturer,
      'productsByName': productsByName,
      'productsByType': productsByType,
      'dataChatsContent': dataChatsContent,
      'dataChats': {
        'boysCount': barChartValues.get("boy_values", ""),
        'girlsCount': barChartValues.get("girl_values", ""),
        'diagnosisLabel': pieChartValue.get("diagnosis_values", [[],[],[]])[0],
        'diagnosisData': pieChartValue.get("diagnosis_values", [[],[],[]])[1],
        'diagnosisColors': pieChartValue.get("diagnosis_values", [[],[],[]])[2],
        'dispositionLabel': pieChartValue.get("disposition_values", [[],[],[]])[0],
        'dispositionData': pieChartValue.get("disposition_values", [[],[],[]])[1],
        'dispositionColors': pieChartValue.get("disposition_values", [[],[],[]])[2]
      },
      'scrapData': scrapData,
      'reportName': reportName,
      'reportCategory': reportCategory,
      'reportManufacturer': reportManufacturer,
      'disclaimerTrigger': disclaimerTrigger,
      'smokeWords': smokeWords
    }
  }

@recallit_cache(nodes, "uniqData")
def uniqData(arr, col):
  col_uniq = list()
  ret = list()
  for x in arr:
    if x[col] not in col_uniq:
      ret.append(x)
      col_uniq.append(x[col])
  return ret

def string_found(s1, s2):
  if re.search(r"\b" + re.escape(s1.lower()) + r"\b", s2.lower()):
      return True
  return False

@recallit_cache(nodes, "new_get_results_manufacturer_approx")
def new_get_results_manufacturer_approx(raw_val):
  global manufacturer_name_list_new
  val = cleanco.cleanco(raw_val).clean_name().lower()
  #print("val: " + val)
  try:
    manufacturer_name_list_new = recall.find({}).distinct("Manufacturers.Name")
    approx_val = ""
    match_len = 0
    for manufacturer in manufacturer_name_list_new:
      #print(manufacturer, val)
      if string_found(manufacturer, val) and len(manufacturer) > match_len:
        approx_val = manufacturer
        match_len = len(approx_val)
    if match_len == 0:
      return {
      'success': False
    }
    else:
      return {
        'success': True,
        'manufacturer_approx': approx_val
      }
  except ValueError as e:
    return {'error': str(e)}

def new2_get_results_manufacturer_approx(raw_val):
  global stopwords

  key = re.sub("[^0-9a-zA-Z]+", "", raw_val.lower())
  if len(key) == 0:
    return {'success': False}

  dynamodb = boto3.resource(service_name='dynamodb', region_name='us-east-1')
  table = dynamodb.Table('manufacturer_approx')
  item = table.get_item(Key={"r": key})
  if 'Item' in item:
    return {'success': True, 'manufacturer_approx':item['Item']['v']}

  comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
  response = comprehend.detect_entities(
    Text = raw_val,
    LanguageCode = 'en'
  )

  manufacturer_approx = ""
  manufacturer_approx_len = 0
  if len(response['Entities']) == 0:
    return {'success': False}
  
  for ent in response['Entities']:
    if ent['Type'] == "LOCATION" or ent['Type'] == "PRODUCT" or ent['Type'] == "ORGANIZATION" or ent['Type'] == "PERSON":
      if len(ent['Text']) > manufacturer_approx_len:
        text_tokenize = re.split(r'\W+', ent['Text'])
        stopwords = ['the', 'store', 'furniture', 'shop']
        tokens_without_sw = [word for word in text_tokenize if not word.lower() in stopwords]
        manufacturer_approx = " ".join(tokens_without_sw)
        manufacturer_approx_len = len(manufacturer_approx)

  if manufacturer_approx_len > 0:

    table.put_item(Item={
      'r': key,
      'v': manufacturer_approx
    })
    return {
          'success': True,
          'manufacturer_approx': manufacturer_approx
        }
  
  else:
    return {
      'success': False
    }