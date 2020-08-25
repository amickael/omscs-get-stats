import os
import time
import datetime as dt
from collections import defaultdict
from decimal import Decimal
from typing import Any
import json
import boto3
from boto3.dynamodb.conditions import Key

try:
    import dotenv

    dotenv.load_dotenv("../.env")
except ImportError:
    pass


########################################################################################################################
# Configuration
########################################################################################################################
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE")
DYNAMODB_KEY = os.getenv("DYNAMODB_KEY")
DYNAMODB_SORT = os.getenv("DYNAMODB_SORT")
MATRICULATION = os.getenv("MATRICULATION")


########################################################################################################################
# Utils
########################################################################################################################
class BetterJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Decimal):
            return int(o)
        else:
            return super().default(o)


########################################################################################################################
# Application
########################################################################################################################
def lambda_handler(event: dict, context):
    # Pull data from DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DYNAMODB_TABLE)
    response = table.query(
        Select="ALL_ATTRIBUTES",
        ScanIndexForward=False,
        KeyConditionExpression=Key(DYNAMODB_KEY).eq(MATRICULATION)
        & Key(DYNAMODB_SORT).gte(int(int(time.time() * 1000) - 4.32e8)),
    )

    # Parse items to get the latest info for each day
    dates = defaultdict(list)
    items = response["Items"]
    for item in items:
        timestamp = dt.datetime.fromtimestamp(int(item["ProcessEpoch"]) / 1000)
        dates[timestamp.strftime("%Y-%m-%d")].append(item)
    payload = []
    for value in dates.values():
        if value:
            payload.append(value[0])

    return {
        "statusCode": 200,
        "body": json.dumps(payload, cls=BetterJSONEncoder),
        "contentType": "application/json",
    }
