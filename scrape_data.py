import json
from urllib.request import urlopen
import pandas as pd
from tqdm import tqdm
from google.cloud import bigquery
import pytz
import datetime
import numpy as np

# Construct a BigQuery client object.
client = bigquery.Client()
table_id = "hamtraffic.all_data.bike_stations"

job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        #    # Specify the type of columns whose type cannot be auto-detected. For
        #    # example the "title" column uses pandas dtype "object", so its
        #    # data type is ambiguous.
        # bigquery.SchemaField(
        #    "observedAreaCoordinates", bigquery.enums.SqlTypeNames.S
        # ),
        # bigquery.SchemaField("result", bigquery.enums.SqlTypeNames.FLOAT64),
        # bigquery.SchemaField("resultTime", bigquery.enums.SqlTypeNames.STRING),
    ],
    #    bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
    #    # Indexes are written if included in the schema by name.
    #    bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
    # ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    # write_disposition="WRITE_TRUNCATE",
)


base_url_bikes = "https://iot.hamburg.de/v1.1/Things?$skip=0&$top=5000&$filter=((properties%2Ftopic+eq+%27Transport+und+Verkehr%27)+and+(properties%2FownerThing+eq+%27DB+Connect%27))"

response = urlopen(base_url_bikes)
all_stations = pd.DataFrame(json.loads(response.read())["value"])

for rowindex, station in tqdm(all_stations.iterrows()):
    current_station = pd.DataFrame()  # used for uploading
    try:
        datastream = pd.DataFrame(
            json.loads(urlopen(station["Datastreams@iot.navigationLink"]).read())[
                "value"
            ]
        ).iloc[0]
        obs_url = (
            datastream["Observations@iot.navigationLink"]
            + "?$top=5000&$skip={}&$orderby=phenomenonTime+desc"
        )
    except Exception("No Datastream found!"):
        continue
    try:
        for i in range(0, 10000000, 5000):
            obs_iter = pd.DataFrame(
                json.loads(urlopen(obs_url.format(i)).read())["value"]
            )
            if len(obs_iter) == 0:
                current_station.rename(
                    columns={"@iot.id": "observationID"},
                    inplace=True,
                )
                current_station["resultTime"] = pd.to_datetime(
                    current_station["resultTime"]
                )
                current_station["result"] = pd.to_numeric(
                    current_station["result"], errors="coerce"
                )
                [5, 5]
                current_station["thingID"] = station["@iot.id"]
                current_station["thingDescriptipn"] = station["name"]

                current_station["observedAreaCoordinates"] = ",".join(
                    [str(x) for x in datastream.observedArea["coordinates"][-1]]
                )
                job = client.load_table_from_dataframe(
                    current_station,
                    table_id,
                    job_config=job_config,
                )
                job.result()
                raise Exception("All Data for station has been uploaded to GBQ")

            current_station = current_station.append(
                obs_iter.filter(
                    items=[
                        "@iot.id",
                        "result",
                        "resultTime",
                    ]
                )
            )

    except Exception:
        continue
