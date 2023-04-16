import json
import bson
import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]
col = db["employees"]

with open("dump/sampleDB/sample_collection.bson", "rb") as f:
    data = bson.decode_all(f.read())


col.drop()
col.insert_many(data)


def get_update_pipeline(unit):
    update_pipleine = [
        {
            "$set": {
                unit: {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S",
                        "date": {"$dateTrunc": {"date": "$dt", "unit": unit}},
                    }
                }
            }
        }
    ]
    return update_pipleine


col.update_many({}, get_update_pipeline("month"))
col.update_many({}, get_update_pipeline("day"))
col.update_many({}, get_update_pipeline("hour"))


async def is_query_valid(query):
    query1 = {
        "dt_from": "2022-09-01T00:00:00",
        "dt_upto": "2022-12-31T23:59:00",
        "group_type": "month",
    }
    query2 = {
        "dt_from": "2022-10-01T00:00:00",
        "dt_upto": "2022-11-30T23:59:00",
        "group_type": "day",
    }

    query3 = {
        "dt_from": "2022-02-01T00:00:00",
        "dt_upto": "2022-02-02T00:00:00",
        "group_type": "hour",
    }

    if query in [query1, query2, query3]:
        return True
    else:
        return False


async def get_dates_range(dt_from, dt_upto, group_type):
    dates = [dt_from.isoformat()]
    while dt_from <= dt_upto:
        if group_type == "month":
            dt_from += relativedelta(months=1)
        elif group_type == "day":
            dt_from += relativedelta(days=1)
        elif group_type == "hour":
            dt_from += relativedelta(hours=1)
        if dt_from <= dt_upto:
            dates.append(dt_from.isoformat())
    return dates


async def get_pipeline(dt_from, dt_upto, group_type, dates_range):
    match = {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}}
    group_by_date = {
        "$group": {"_id": "${}".format(group_type), "dataset": {"$sum": "$value"}}
    }
    sort = {"$sort": {"_id": 1}}
    columns = {"$project": {"_id": 0, "datetime": "$_id", "dataset": "$dataset"}}
    group_by_id = {"$group": {"_id": None, "points": {"$push": "$$ROOT"}}}
    project_map = {
        "$project": {
            "points": {
                "$map": {
                    "input": dates_range,
                    "as": "datetime",
                    "in": {
                        "$let": {
                            "vars": {
                                "datetimeIndex": {
                                    "$indexOfArray": ["$points.datetime", "$$datetime"]
                                }
                            },
                            "in": {
                                "$cond": {
                                    "if": {"$ne": ["$$datetimeIndex", -1]},
                                    "then": {
                                        "$arrayElemAt": ["$points", "$$datetimeIndex"]
                                    },
                                    "else": {"datetime": "$$datetime", "dataset": 0},
                                }
                            },
                        }
                    },
                }
            }
        }
    }
    unwind = {"$unwind": "$points"}
    replace = {"$replaceRoot": {"newRoot": "$points"}}
    return [
        match,
        group_by_date,
        sort,
        columns,
        group_by_id,
        project_map,
        unwind,
        replace,
    ]


async def get_result_cursor(pipeline):
    query_result = col.aggregate(pipeline)
    return query_result


async def get_results(result_cursor):
    statistics = {"dataset": [], "labels": []}
    for doc in result_cursor:
        statistics["dataset"].append(doc["dataset"])
        statistics["labels"].append(doc["datetime"])
    return statistics


async def is_json(query_text):
    try:
        json.loads(query_text)
        return True
    except Exception as ex:
        print(ex)
        return False


async def process_query(loop, query):
    query_json = json.loads(query)
    query = loop.create_task(is_query_valid(query_json))
    await asyncio.wait([query])
    if query.result():
        dt_from = datetime.strptime(query_json["dt_from"], "%Y-%m-%dT%H:%M:%S")
        dt_upto = datetime.strptime(query_json["dt_upto"], "%Y-%m-%dT%H:%M:%S")
        group_type = query_json["group_type"]

        dates_range = asyncio.create_task(get_dates_range(dt_from, dt_upto, group_type))
        await asyncio.wait([dates_range])

        pipeline = loop.create_task(
            get_pipeline(dt_from, dt_upto, group_type, dates_range.result())
        )
        await asyncio.wait([pipeline])

        result_cursor = loop.create_task(get_result_cursor(pipeline.result()))
        await asyncio.wait([result_cursor])

        statistics = loop.create_task(get_results(result_cursor.result()))
        await asyncio.wait([statistics])

        return statistics
    else:
        return (
            "Допустимо отправлять только следующие запросы:\n"
            + '{"dt_from": "2022-09-01T00:00:00", "dt_upto": '
            + '"2022-12-31T23:59:00", "group_type": "month"}\n'
            + '{"dt_from": "2022-10-01T00:00:00", "dt_upto": '
            + '"2022-11-30T23:59:00", "group_type": "day"}\n'
            + '{"dt_from": "2022-02-01T00:00:00", "dt_upto": '
            + '"2022-02-02T00:00:00", "group_type": "hour"}\n'
        )
