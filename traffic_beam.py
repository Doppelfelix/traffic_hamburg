import apache_beam as beam
import requests
import json
import logging
from urllib.request import urlopen
import pandas as pd
from datetime import date, timedelta

yesterday = date.today() - timedelta(days=1)
yesterday = yesterday.strftime("%Y-%m-%d")
table_spec = "hamtraffic:all_data.car_traffic_daily_updates"
base_url_cars = "https://iot.hamburg.de/v1.1/Things?$skip=0&$top=5000&$filter=((properties%2Ftopic+eq+%27Transport+und+Verkehr%27)+and+(properties%2FownerThing+eq+%27Freie+und+Hansestadt+Hamburg%27))"


# beam_options = PipelineOptions()


def get_stations():
    logging.debug(f"Now fetching all stations data.")
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
            logging.error(
                f"No proper observation stream for {element['thingID']=} found."
            )


class get_obs(beam.DoFn):
    def process(self, element, date):
        logging.debug(f"Now fetching observations for {element['thingID']}")

        obs_url = (
            element["obs_stream"]
            + "?$top=5000&$skip={}&$filter=date(phenomenontime)+eq+date('{}')"
        )
        list_of_obs = []
        for i in range(0, 10000000, 5000):
            obs_iter = json.loads(urlopen(obs_url.format(i, date)).read())["value"]
            if len(obs_iter) == 0:
                break
            list_of_obs = list_of_obs + obs_iter

        for obs in list_of_obs:
            obs.update(
                {
                    "thingID": element["thingID"],
                    "description": element["description"],
                    "observedAreaCoordinates": element["observedAreaCoordinates"],
                }
            )

        return list_of_obs


class clean_obs(beam.DoFn):
    def process(self, element):
        logging.debug(f"Now cleaning observations for {element['thingID']}")
        observationID = element["@iot.id"]
        resultTime = element["resultTime"]
        result = element["result"]

        return [
            {
                "thingID": element["thingID"],
                "description": element["description"],
                "observedAreaCoordinates": element["observedAreaCoordinates"],
                "result": result,
                "resultTime": resultTime,
                "observationID": observationID,
            }
        ]


table_schema = {
    "fields": [
        {"name": "thingID", "type": "INT64"},
        {"name": "description", "type": "STRING"},
        {"name": "observedAreaCoordinates", "type": "STRING"},
        {"name": "result", "type": "FLOAT64"},
        {"name": "observationID", "type": "INT64"},
        {"name": "resultTime", "type": "TIMESTAMP"},
    ]
}


def run():
    p = beam.Pipeline()

    all_stations = get_stations()

    ingest_data = (
        p
        | "pass stations" >> beam.Create(all_stations)
        | "get station urls" >> beam.ParDo(get_station_urls())
        | "get obs stream" >> beam.ParDo(get_obs_stream())
        | "get obs"
        >> beam.ParDo(
            get_obs(), yesterday
        )  # BASH: date -d "yesterday 13:00" '+%Y-%m-%d'
        | "clean obs" >> beam.ParDo(clean_obs())
        | "write into gbq"
        >> beam.io.gcp.bigquery.WriteToBigQuery(
            table=table_spec,
            schema=table_schema,
            custom_gcs_temp_location="gs://hamtraf_bucket",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
    )
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
