import os
import time
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
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DYNAMODB_TABLE)
    response = table.query(
        Select="ALL_ATTRIBUTES",
        ScanIndexForward=False,
        KeyConditionExpression=Key(DYNAMODB_KEY).eq(MATRICULATION)
        & Key(DYNAMODB_SORT).gte(int(int(time.time() * 1000) - 4.32e8)),
    )
    items = response["Items"]
    return {
        "statusCode": 200,
        "body": json.dumps(items, cls=BetterJSONEncoder),
        "contentType": "application/json",
    }
