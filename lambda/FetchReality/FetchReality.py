import json
import urllib
import sys
import math
import os
from datetime import datetime, timedelta
##config = json.load(file(os.path.join(BASE_DIR, "config.json"),)) ##future config file from foler or event 
## DynamoDb Functions
import boto3
from boto3.dynamodb.conditions import Key
from pprint import pprint
from decimal import Decimal


def get_record(item, dynamodb=None):
    if not dynamodb:
        print("PLS PASS A DB RESOURCE")

    table = dynamodb.Table(dbname)
    response = table.get_item(Key={"hash_id": item})
    return response


def query_record(item, dynamodb=None):
    if not dynamodb:
        print("PLS PASS A DB RESOURCE")

    table = dynamodb.Table(dbname)
    response = table.query(KeyConditionExpression=Key("hash_id").eq(item))
    return response


def put_record(item, dynamodb=None):
    if not dynamodb:
        print("PLS PASS A DB RESOURCE")

    table = dynamodb.Table(dbname)
    try:
        response = table.put_item(Item=item,ConditionExpression='attribute_not_exists(hash_id)')#? its just attribute check not value
        return response


    except Exception as e:
      return e


def update_record(item, item_from_request, dynamodb=None):
    if not dynamodb:
        print("PLS PASS A DB RESOURCE")
    table = dynamodb.Table(dbname)
    stringa = (
        "SET price_czk.value_raw = :p, parsed.price.h"
        + str(datetime.today().strftime("%d_%m_%Y"))
        + " = :pp"
    )
    try:
        response1 = table.update_item(
            Key={"hash_id": item},  # key
            UpdateExpression="SET parsed.space_m2 = :s, parsed.hyperlink = :h, actualized = :a",
            ConditionExpression='actualized <> :a', # Do not update if the same
            ExpressionAttributeValues={
                ":s": item_from_request['parsed']['space_m2'],
                ":h": item_from_request["parsed"]["hyperlink"],
                ":a": item_from_request["actualized"],
            },
            ReturnValues="UPDATED_NEW",
        )

        response2 = table.update_item(
            Key={"hash_id": item},  # key
            UpdateExpression=stringa,
            ConditionExpression="price_czk.value_raw <> :p",  # Do not update if the same
            ExpressionAttributeValues={
                ":p": item_from_request["price_czk"]["value_raw"],
                ":pp": item_from_request["price_czk"]["value_raw"],
            },
            ReturnValues="UPDATED_NEW",
        )
        return response1, response2
    except Exception as e:
        return e


def put_record2(item, dynamodb=None):

    if not dynamodb:
        print("PLS PASS A DB RESOURCE")
    item_from_request = ItemData(item["hash_id"])
    

    if item_from_request != "Bad Data":
        item_from_request["hash_id"] = item["hash_id"]
        item_from_db = query_record(item_from_request["hash_id"], dynamodb)
        if item_from_db["Count"]==0:
            put_record(item_from_request, dynamodb)
        else:
            update_record(
                item_from_request["parsed"]["id"], item_from_request, dynamodb
            )
    return


# API functions
def FilterData(request, per_page, page_number):
    url = (
        (
            "https://www.sreality.cz/api/cs/v2/estates?"
            + "&".join(request)
            + "&per_page="
            + str(per_page)
            + "&page="
            + str(page_number)
        )
        .replace("-", "%7C")
        .replace(",", "%7C")
    )
    response = urllib.request.urlopen(url)
    data = json.loads(response.read(), parse_float=Decimal)
    return data

def get_google_directions (trans,dest,key,org):
    url = ("https://maps.googleapis.com/maps/api/directions/json?" 
    + "origin=" + str(org["lat"]) + "," + str(org["lon"]) 
    + "&destination=" + str(dest["lat"]) + "," + str(dest["lon"]) 
    + "&mode=" + trans 
    + "&key=" + key)
    response = urllib.request.urlopen(url)
    data = json.loads(response.read(),parse_float=Decimal)
    if data["status"]== "OK":
        distance = data["routes"][0]["legs"][0]["distance"]#["text"]["value"]
        duration = data["routes"][0]["legs"][0]["duration"]
        response={"distance":distance,"duration":duration}
    else:
        response = data["status"]

    return (response)


def ItemData(id):
    hradcanska = {"lat":50.098411,"lon": 14.406452} ##desired locations for google API
    msd = {"lat":50.067479,"lon":14.409609}
    liberec = {"lat":50.792541,"lon":15.070705}

    url = "https://www.sreality.cz/api/cs/v2/estates/" + str(id)
    response = urllib.request.urlopen(url)
    data = json.loads(response.read(), parse_float=Decimal)
    category_type_cb = {1: "prodej", 2: "pronajem", 3: "drazby"}
    category_main_cb = {1: "byt", 2: "dum", 3: "pozemek", 4: "komercni", 5: "ostatni"}
    category_sub_cb = {  # flats
        47: "pokoj",
        2: "1+kk",
        3: "1+1",
        4: "2+kk",
        5: "2+1",
        6: "3+kk",
        7: "3+1",
        8: "4+kk",
        9: "4+1",
        10: "5+kk",
        11: "5+1",
        12: "6-a-vice",
        16: "atypicky",
        # houses
        37: "rodinny",
        39: "vila",
        43: "chalupa",
        33: "chata",
        40: "na-klic",
        44: "zemedelska-usedlost",
        35: "pamatka",
        # land
        19: "bydleni",
        18: "komercni",
        20: "pole",
        22: "louky",
        21: "lesy",
        46: "rybniky",
        48: "sady-vinice",
        23: "zahrady",
        24: "ostatni",
    }
    space_m2 = ""
    if "items" in data:
        for item in data["items"]:
            if item["name"] == "Užitná plocha":
                space_m2 = item["value"]
            if item["name"] == "Aktualizace" and item["value"] == "Dnes":
                item["value"] = datetime.today().strftime("%d.%m.%Y")
            elif item["name"] == "Aktualizace" and item["value"] == "Včera":
                item["value"] = (datetime.today() - timedelta(days=1)).strftime("%d.%m.%Y")
            if item["name"] == "Aktualizace":
                data["actualized"] = item["value"]
        hyper_link = (
            "https://www.sreality.cz/detail/"
            + category_type_cb[data["seo"]["category_type_cb"]]
            + "/"
            + category_main_cb[data["seo"]["category_main_cb"]]
            + "/"
            + category_sub_cb[data["seo"]["category_sub_cb"]]
            + "/"
            + data["seo"]["locality"]
            + "/"
            + str(id)
        )

        data["parsed"] = {"date":  datetime.today().strftime("%d.%m.%Y"),#data["actualized"],
                            "id": id,"hyperlink": hyper_link,
                            "price": {datetime.today().strftime("%d-%m-%Y"): data["price_czk"]["value_raw"]}, "space_m2": space_m2}

        data["_embedded"].pop("images", None)
        data["_embedded"].pop("favourite", None)
        data.pop("_links", None)
        data.pop("codeItems", None)
        data.pop("poi", None)
        if "seller" in data["_embedded"] and "user_id" in data["_embedded"]["seller"]:
            if data["_embedded"]["seller"]["user_id"] != "":
                tmp = data["_embedded"]["seller"]["user_id"]
                data["_embedded"]["seller"].clear()
                data["_embedded"]["seller"]["user_id"] = tmp
        else:
            data["_embedded"]["seller"] = {"user_id": "NA"}
        ##Calling google API
        data["Transport"] = {"hradcanska": {"driving": get_google_directions("driving",hradcanska,google_api_key,data["map"]),
                                            "transit": get_google_directions("transit",hradcanska,google_api_key,data["map"])},
                                    "msd": {"driving": get_google_directions("driving",msd,google_api_key,data["map"]),
                                            "transit": get_google_directions("transit",msd,google_api_key,data["map"])},
                                "liberec": {"driving": get_google_directions("driving",liberec,google_api_key,data["map"]),
                                            "transit": get_google_directions("transit",liberec,google_api_key,data["map"])}}

        #data["Transport"]= {"hradcanska": {"transit": get_google_directions("transit",hradcanska,google_api_key,data["map"])}}
        #data["Transport"]= {"msd" : {"transit": get_google_directions("transit",msd,google_api_key,data["map"])}}
        #data["Transport"]= {"liberec":}
    else:
        data = "Bad Data"
    return data


# Execution code


def handler(event, context):
    global dbname
    dbname = "my-reality-devXXXXXXXX" ##Add your Dynamo DB name- could be dynamicaly added
    # "Reality-" + os.environ['env']
    global google_api_key 
    google_api_key = "XXXXXaSyCd2yFqFqMQc-XXXXXXXXXXXXXXXXXXXX"##Add your google API key
    
    hradcanska = {"lat":50.098411,"lon": 14.406452}
    msd = {"lat":50.067479,"lon":14.409609}
    liberec = {"lat":50.792541,"lon":15.070705}
    global destination
    destination = [hradcanska,msd,liberec]
    global transport 
    transport= ["transit","driving"]

    #Domy-prg
    request1 = [
        "category_type_cb=1,3",  # category_type_cb = {1:"Prodej",3:"Drazby" }
        "category_main_cb=2",  # category_main_cb => 2:"Domy"
        "czk_price_summary_order2=3000000-9000000",
        "estate_age=2",  # estate_age => 1 :"Bez omezeni", 2:"Den", 8:"Poslednich 7 dni"
        "locality_district_id=56,57,5001,5002,5003,5004,5005,5006,5007,5008,5009,5010", 
        "locality_region_id=10,11", 
        "pois_in_place=7", #zakladka
        "pois_in_place_distance=2", #to 2km possible to 1.5
        "usable_area=80,200"
    ]
        
    #Domy lbc
    request2 = [
        "category_type_cb=1,3",  # category_type_cb = {1:"Prodej", 2:"Pronajem", 3:"Drazby" }
        "category_main_cb=2",  # category_main_cb => 1:"Byty", 2:"Domy", 3:"Pozemky", 4:"Komercni", 5:"Ostatni"
        "czk_price_summary_order2=2000000-8000000",
        "estate_age=2",  # estate_age => 1 :"Bez omezeni", 2:"Den", 8:"Poslednich 7 dni", 31:"Poslednich 30 dni"
        "region_entity_id=14390,14381,14391,14407,102",
        "locality_region_id=5",
        "usable_area=90-200"      
         ]
    #Byty prg
    request3 = [
        "category_type_cb=1,3",  # category_type_cb = {1:"Prodej", 2:"Pronajem", 3:"Drazby" }
        "category_main_cb=1",  # category_main_cb => 1:"Byty", 2:"Domy", 3:"Pozemky", 4:"Komercni", 5:"Ostatni"
        "category_sub_cb=7,8,9,10,11,12,16",
        "ownership=1",
        "czk_price_summary_order2=4000000-8900000",
        "estate_age=2",  # estate_age => 1 :"Bez omezeni", 2:"Den", 8:"Poslednich 7 dni", 31:"Poslednich 30 dni"
        "locality_district_id=56,57,5001,5002,5003,5004,5005,5006,5007,5008,5009,5010",
        "locality_region_id=10,11",
        "building_type_search=2,3", #=> 1:"Panel", 2:"Cihla", 3:"Ostatni"
        "pois_in_place=7",
        "pois_in_place_distance=2",#,
        "usable_area=80-200",      
         ]
    
    #Pozemky prg
    request4 = [
        "category_type_cb=1,3",  # category_type_cb = {1:"Prodej", 2:"Pronajem", 3:"Drazby" }
        "category_main_cb=3",  # category_main_cb => 1:"Byty", 2:"Domy", 3:"Pozemky", 4:"Komercni", 5:"Ostatni"
        "category_sub_cb=19",
        "czk_price_summary_order2=1000000-4500000",
        "estate_age=2",  # estate_age => 1 :"Bez omezeni", 2:"Den", 8:"Poslednich 7 dni", 31:"Poslednich 30 dni"
        "locality_district_id=56,57,22,5001,5002,5003,5004,5005,5006,5007,5008,5009,5010",
        "locality_region_id=10,11",
        "pois_in_place=7",
        "pois_in_place_distance=2"
        ]
    
    per_page = 200
    request_tuple = [request1,request2,request3,request4]
    # Create DynamoDB connection
    dynamodb = boto3.resource("dynamodb")  # , aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name="eu-west-3")
    
    for request in request_tuple:
            #request=x
            total_number = FilterData(request, 1, 1)["result_size"]

            total_number_of_pages = math.ceil(total_number / per_page)
            # pool = ThreadPool(10)

            if total_number_of_pages == 1:
                houses = FilterData(request, per_page, 1)["_embedded"]["estates"]
                for house in houses:
                   put_record2(house, dynamodb)

             # results = pool.map(record_item_to_db, houses)
                # pool.close()
             # pool.join()

            else:
             while total_number_of_pages >= 1:
                  houses = FilterData(request, per_page, total_number_of_pages)["_embedded"]["estates"]
                  for house in houses:
                        put_record2(house, dynamodb )
                    # results = pool.map(record_item_to_db, houses)
                    # pool.close()
                    # pool.join()
                        total_number_of_pages -= 1
            total_sum=+total_number

    end_time = datetime.now()
    message = (
             "Totally "
             + str(total_sum)
             + " items found.Script finished successfully ...\n Ended at "
             + end_time.strftime("%Y-%m-%d %H:%M:%S")
            )
    return {"message": message}
