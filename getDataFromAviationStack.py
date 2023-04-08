import os
import requests
from datetime import datetime

"""
Get all airport - country pairs
from https://aviationstack.com
"""

parameters = {"access_key": os.environ["MY_AVIA_TOKEN"]}


def get_data():
    now = datetime.now().strftime("%Y-%m-%d_%H:%M")
    api_result = requests.get("http://api.aviationstack.com/v1/flights", parameters)
    print(api_result)
    api_response = api_result.json()
    # print(api_response)
    # if api_response["error"]:
    #     print("WARNING: API responded with error!")
    #     return
    pagination = api_response["pagination"]
    total = int(pagination["total"])
    limit = int(pagination["limit"])
    with open("./data/routes" + now + ".txt", "w") as fd:
        for i in range(100):
            for result in api_response["data"]:
                flight = result["flight"]["number"].strip() if result["flight"]["number"] else "0000"
                dep_time = result["departure"]["scheduled"].strip() if result["departure"]["scheduled"] else "Unknown"
                arr_icao = result["arrival"]["icao"].strip() if result["arrival"]["icao"] else "XXXX"
                dep_icao = result["departure"]["icao"].strip() if result["departure"]["icao"] else "XXXX"
                arr_tz = result["arrival"]["timezone"].strip()[:2] if result["arrival"]["timezone"] else "XX"
                dep_tz = result["departure"]["timezone"].strip()[:2] if result["departure"]["timezone"] else "XX"

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
