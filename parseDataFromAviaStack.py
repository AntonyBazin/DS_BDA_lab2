import os
from datetime import datetime
import json

"""
Get all airport - country pairs
from https://aviationstack.com
"""


def get_data():
    now = datetime.now().strftime("%Y-%m-%d_%H:%M")
    with open("./mock_data/flights.json") as fd:
        api_response = json.load(fd)
    pagination = api_response["pagination"]
    total = int(pagination["total"])
    limit = int(pagination["limit"])
    with open("./data/routes" + now + ".txt", "w") as fd:
        for i in range(100):
            for result in api_response["data"]:
                flight = result["flight"]["number"].strip()
                dep_time = result["departure"]["scheduled"].strip()
                arr_icao = result["arrival"]["icao"].strip()
                dep_icao = result["departure"]["icao"].strip()
                arr_tz = result["arrival"]["timezone"].strip()[:2]
                dep_tz = result["departure"]["timezone"].strip()[:2]

                fd.write(
                    flight
                    + ","
                    + dep_time
                    + ","
                    + arr_icao
                    + ","
                    + dep_icao
                    + ","
                    + arr_tz
                    + ","
                    + dep_tz
                    + "\n"
                )
    print(pagination, total, limit)


if __name__ == "__main__":
    get_data()
