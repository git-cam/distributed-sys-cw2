import azure.functions as func
import json
import requests
from azure.functions import SqlRowList

app = func.FunctionApp()

@app.function_name(name="SensorTrigger")
@app.sql_trigger(arg_name="sensorTrigger",
                        table_name="dbo.SensorReadings",
                        connection_string_setting="SqlConnectionString")
def sensorTrigger(sensorTrigger: str) -> None:
    requests.get(
        "http://localhost:7071/api/task2"
    )

@app.function_name(name="SensorStatistics")
@app.route(route="task2", auth_level=func.AuthLevel.ANONYMOUS)
@app.sql_input(
    arg_name="rows",
    command_text="""
        SELECT 
            sensorID,
            MIN(temp)     AS minTemp,
            MAX(temp)     AS maxTemp,
            AVG(temp)     AS avgTemp,
            MIN(wind)     AS minWind,
            MAX(wind)     AS maxWind,
            AVG(wind)     AS avgWind,
            MIN(humidity) AS minHumidity,
            MAX(humidity) AS maxHumidity,
            AVG(humidity) AS avgHumidity,
            MIN(co2)      AS minCo2,
            MAX(co2)      AS maxCo2,
            AVG(co2)      AS avgCo2
        FROM dbo.SensorReadings
        GROUP BY sensorID
    """,
    connection_string_setting="SqlConnectionString",
    command_type="Text"
)

def sensor_statistics(req: func.HttpRequest, rows: SqlRowList) -> func.HttpResponse:
    result = []
    for row in rows:
        result.append({
            "sensorID": row["sensorID"],
            "temp": {
                "min": row["minTemp"],
                "max": row["maxTemp"],
                "avg": row["avgTemp"],
            },
            "wind": {
                "min": row["minWind"],
                "max": row["maxWind"],
                "avg": row["avgWind"],
            },
            "humidity": {
                "min": row["minHumidity"],
                "max": row["maxHumidity"],
                "avg": row["avgHumidity"],
            },
            "co2": {
                "min": row["minCo2"],
                "max": row["maxCo2"],
                "avg": row["avgCo2"],
            }
        })
    return func.HttpResponse(
        json.dumps(result, default=str),
        mimetype="application/json",
        status_code=200
    )
