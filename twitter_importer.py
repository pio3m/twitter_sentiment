import urllib.parse
from pytwitter import Api
import pandas as pd
from pymongo import MongoClient


def getTwit(id_twid):
    api = Api(
        bearer_token="XXX")
    try:
        txt_return = api.get_tweets(id_twid)
        return txt_return.data.text
    except:
        return ''


def getTwits(id_twid):
    api = Api(
        bearer_token="XXX")
    try:
        txt_return = api.get_tweets(id_twid)
        return list(txt_return.data)

    except Exception as e:
        return list()

def appendTwits(count, chunk_size):
    file1 = open('twit_list.txt', 'r')
    Lines = file1.readlines()

    tab_twitow = []
    twit_txt = []
    last_index = chunk_size + count
    for line in Lines[count:]:
        count += 1
        tab_twitow.append(line.split()[0])
        print(count)
        if count + 1 > last_index:
            break

    return tab_twitow

client = MongoClient("XXX:" + urllib.parse.quote(XXX)
db = client.get_database('zum_twits')

def load_twits_id():
    id_twits_to_save = appendTwits(100000, 500000)
    all_ids = []
    count_id = 0

    for id_one in id_twits_to_save:
        id_o = {
            'count': count_id,
            'id_twit': id_one
        }
        all_ids.append(id_o)
        count_id += 1
    #
    records = db.source
    records.insert_many(all_ids)

def get_twits_from_db(start, stop):
    chunk_list = range(start, stop)
    records = db.source
    ids_list_range = list(records.find({'count': {'$in': list(chunk_list)}}, {'_id': 0, 'count': 0}))
    ids_list_only = []
    for dic_o in ids_list_range:
        ids_list_only.append(dic_o['id_twit'])
    return ids_list_only


def save_text(list_twits):
    if len(list_twits) > 0:
        to_save_text = []
        for twit in list_twits:
            t_o = {
                'twit_id': twit.id,
                'text': twit.text
            }
            to_save_text.append(t_o)

        twits_db = db.twits_text
        print("save")
        twits_db.insert_many(to_save_text)

def run_main(start, chunk_size):
    end = start + chunk_size
    main = range(start, end, 100)

    for start in list(main):
        print(start)
        stop = start + 100
        list_ids_to_send = get_twits_from_db(start, stop)
        retried_twits_text = getTwits(list_ids_to_send)
        save_text(retried_twits_text)

run_main(483810, 30000)

# twits_db = db.twits_text
# all = list(twits_db.find())
# df = pd.DataFrame(all)
# df.to_csv('pobrane_twity.csv', encoding="utf-8")
print("end")


