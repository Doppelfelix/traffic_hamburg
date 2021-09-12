import apache_beam as beam
import requests
import json
import logging
from urllib.request import urlopen
import pandas as pd


table_spec = "hamtraffic.all_data.car_traffic"
base_url_cars = "https://iot.hamburg.de/v1.1/Things?$skip=0&$top=5000&$filter=((properties%2Ftopic+eq+%27Transport+und+Verkehr%27)+and+(properties%2FownerThing+eq+%27Freie+und+Hansestadt+Hamburg%27))"


# beam_options = PipelineOptions()
p = beam.Pipeline()


class get_stations(beam.DoFn):
    def __init__(self):
        logging.debug("fetching stations")

    def process(self, date):
        logging.debug(f"Now fetching data for {date}")
        response = json.loads(urlopen(base_url_cars).read())
        return response["value"]


class get_station_urls(beam.DoFn):
    def process(self, element):
        try:
            thingID = element["@iot.id"]
            datastream_url = element["Datastreams@iot.navigationLink"]
            description = element["description"]
        except ValueError:
            logging.error("Not valid value found.")

        return [
            {
                "thingID": thingID,
                "datastream_url": datastream_url,
                "description": description,
            }
        ]


class get_obs_stream(beam.DoFn):
    def process(self, element):
        logging.debug(f"Now fetching streams for {element['thingID']}")
        try:
            response = json.loads(urlopen(element["datastream_url"]).read())["value"]
            keep_datastream = [
                x for x in response if x["properties"]["aggregateDuration"] == "PT15M"
            ][0]
            obs_stream = keep_datastream["Observations@iot.navigationLink"]
            observedAreaCoordinates = ",".join(
                [str(x) for x in keep_datastream["observedArea"]["coordinates"]]
            )
            return [
                {
                    "thingID": element["thingID"],
                    "description": element["description"],
                    "obs_stream": obs_stream,
                    "observedAreaCoordinates": observedAreaCoordinates,
                }
            ]
        except KeyError:
            logging.error("No proper observation stream found.")


class get_obs(beam.DoFn):
    def process(self, element):
        logging.debug(f"Now fetching observations for {element['thingID']}")
        obs = json.loads(urlopen(element["obs_stream"]).read())["value"]

        return [
            {
                "thingID": element["thingID"],
                "description": element["description"],
                "observedAreaCoordinates": element["observedAreaCoordinates"],
                "obs": obs,
            }
        ]


class split_obs(beam.DoFn):
    def process(self, element):
        logging.debug(f"Now splitting observations for {element['thingID']}")
        result = element["obs"]["result"]
        resultTime = element["obs"]["resultTime"]

        return [
            {
                "thingID": element["thingID"],
                "description": element["description"],
                "observedAreaCoordinates": element["observedAreaCoordinates"],
                "result": result,
                "resultTime": resultTime,
            }
        ]


ingest_data = (
    p
    | "pass date" >> beam.Create(["2021-09-11"])
    | "fetch stations data" >> beam.ParDo(get_stations())
    | "get station urls" >> beam.ParDo(get_station_urls())
    | "get obs stream" >> beam.ParDo(get_obs_stream())
    | "get obs" >> beam.ParDo(get_obs())
    | "split obs" >> beam.ParDo(split_obs())
    # | 'write into gbq' >> beam.io.gcp.bigquery.WriteToBigQuery(table = table_spec, schema=table_schema, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE ,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    | "write to text" >> beam.io.WriteToText("./test_v2.csv")
)


"""
quotes = p | beam.Create([
    {
        'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'
    },
    {
        'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'."
    },
])

table_schema = {
    'fields': [{
        'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'
    }, {
        'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'
    }]
}

quotes | beam.io.WriteToBigQuery(
    table_spec,
    schema=table_schema,
    custom_gcs_temp_location="gs://hamtraf_bucket",
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    """
