#!/usr/bin/python3
from pymongo import MongoClient
import pymongo
import os
import time
# from multiprocessing import Process
import threading
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from pprint import pprint
import random

MongoClient = MongoClient('192.168.12.200:24578')


# db = MongoClient.get_database('test',write_concern= pymongo.WriteConcern("majority") )
db = MongoClient.source


# def run(value):
# 	count = 1
# 	while count < 2000000:
# 		count += 1
# 		db.kkk.insert_one({'c': count,'tedcadast': value,'aacdaaa': count,'vvcdcvvv' : 'asdcdacaasda'})


# def bulk(vlaue):
#     count = 1
#     while count < 10000:
#         request = db.bulk.bulk_write([
#             InsertOne({'daada': count, 'dadada': vlaue, 'ddasdasdas': count, 'dasdasda': 'dasdasdasd'}),
#             InsertOne({'daadsadda': count, 'dadasdada': vlaue, 'ddasdfafasdas': count, 'dasdadasdsda': 'dasdaewqesdasd'}),
#             InsertOne({'daadsadda': count, 'dadadfda': vlaue, 'ddasdadassdas': count, 'dasddasdasda': 'dasdeqwasdasd'}),
#             InsertOne({'dadsadada': count, 'dadfffgada': vlaue, 'ddasdgggasdas': count, 'dasdaqweqwsda': 'dasdavggresdasd'}),
#             UpdateOne({'dasdasda': 'dasdasdasd'}, {'$set': {'dasdasda': 'asdkashdjkasdklas'}}),
#             UpdateOne({'dasdadasdsda': 'dasdaewqesdasd'}, {'$set': {'dasdadasdsda': '123121'}}),
#             UpdateOne({'dasddasdasda': 'dasdeqwasdasd'}, {'$set': {'dasddasdasda': '12131'}}),
#             UpdateOne({'dasdaqweqwsda': 'dasdavggresdasd'}, {'$set': {'dasdaqweqwsda': '12013129312'}}),
#             UpdateOne({'dasdadasdsda': 'dasdaewqesdasd'}, {'$inc': {'j': 1}}, upsert=True)
#         ])
#
#         # pprint(request.bulk_api_result)
#         count += 1

def InsertMany(value):
    count = 1
    while count < 100000000000:
        db.lhp8.insert_many([{'x': i, 'name':value, 'ddasdfafasdas': count, 'dsadasdasd' : "dasdasdasd", 'asda' : 'dasdsadasdsdqqwc','age' : count} for i in range(10)])
        db.lhp6.insert_many([{'x': i, 'name':value, 'ddasdfafasdas': count, 'dsadasdasd' : "dasdasdasd", 'asda' : 'dasdsadasdsdqqwc','age' : count} for i in range(10)])
        db.lhp7.insert_many([{'a': i, 'number':value, 'ddasdfadasdfasdas': count, 'dsadasdasdasd' : "dasdasdasd", 'asdasdda' : 'dasdsadasdseedqqwc','age' : count} for i in range(10)])
        db.lhp9.insert_many([{'x': i, 'name':value, 'ddasdfafasdas': count, 'dsadasdasd' : "dasdasdasd", 'asda' : 'dasdsadasdsdqqwc','age' : count} for i in range(10)])
        count += 1
def UpdateMany(value):
    count = 1
    while count < 100000000000:
        i = random.randint(0, 10)
        db.lhp8.update_one({'name':value, 'age': count, 'x': i}, {'$inc': {'ddasdfafasdas': 3}})
        db.lhp6.update_one({'name':value, 'age': count, 'x': i}, {'$inc': {'ddasdfafasdas': 3}})
        db.lhp7.update_one({"_id": count, 'a': i}, {'$inc': {'ddasdfafasdas': 3}})
        db.lhp9.update_one({'name':value, 'age': count, 'x': i}, {'$inc': {'ddasdfafasdas': 3}})
        db.lhp10.update_one({'name':value, 'age': count, 'x': i}, {'$inc': {'ddasdfafasdas': 3}})
        count += 1


def DeleteOne(value):
    count = 1
    while count < 100000000000:
        db.lhp8.delete_one({'name':value, 'age': count})
        db.lhp6.delete_one({'name':value, 'age': count})
        db.lhp7.delete_one({"_id": count})
        db.lhp9.delete_one({'name':value, 'age': count})
        db.lhp10.delete_one({'name':value, 'age': count})

        count += 1
        time.sleep(0.1)


if __name__ == '__main__':

    try:
        # t1 = threading.Thread(target=run, args=(u'twdasdash',))
        for x in range(10):
            t = threading.Thread(target=InsertMany, args=(x,))
            t.start()
            threading.Thread(target=UpdateMany, args=(x, )).start()
            threading.Thread(target=DeleteOne, args=(x, )).start()
            print("start")
    except:
        print("Error: s")

