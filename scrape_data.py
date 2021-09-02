import json
from urllib.request import urlopen
import pandas as pd
from tqdm import tqdm
from google.cloud import bigquery
import pytz
import datetime

# Construct a BigQuery client object.
client = bigquery.Client()
table_id = "hamtraffic.all_data.iot_values"

job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        #    # Specify the type of columns whose type cannot be auto-detected. For
        #    # example the "title" column uses pandas dtype "object", so its
        #    # data type is ambiguous.
        bigquery.SchemaField("iot_id", bigquery.enums.SqlTypeNames.STRING),
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


data = pd.DataFrame()

url = "https://iot.hamburg.de/v1.1/Observations?$skip={}"

for i in tqdm(range(0, 1010000, 100)):
    try:
        response = urlopen(url.format(i))
        df_slice = pd.DataFrame(json.loads(response.read())["value"])
        data = data.append(
            df_slice.filter(
                items=[
                    "@iot.id",
                    "result",
                    "resultTime",
                ]
            )
        )
        if i % 10000 == 0:
            data["iot_id"] = data["@iot.id"].astype(str)
            data.drop("@iot.id", axis=1, inplace=True)
            data["resultTime"] = pd.to_datetime(data["resultTime"])
            data["result"] = pd.to_numeric(data["result"], errors="coerce")

            job = client.load_table_from_dataframe(
                data,
                table_id,
                job_config=job_config,
            )
            job.result()
            data = pd.DataFrame()
    except:
        raise Exception("URL could not be found")
