try:
    import unzip_requirements
except ImportError:
    pass

from affiliate import get_points_from_bbox as affiliate
from patch import get_points_from_bbox as patch
import json,boto3

def runner(uuid):
    bbox = get_bounds_from_dynamo(uuid)
    try:
        x = affiliate(bbox,uuid)
    except Exception as e:
        print(e)
        x = []
    try:
        y = patch(bbox,uuid)
    except Exception as e:
        print(e)
        y = []
    return {"patch": y, "affiliate": x}

def get_bounds_from_dynamo(uuid):
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table('bounds-objects')
    resp = table.get_item(Key={"uuid": uuid})
    bbox = resp['Item']['bbox']
    return {k: [float(v[0]),float(v[1])] for k,v in bbox.items()}


def handler(event, context):
    # add s3 code for uploading data
    return runner(event["body"])


if __name__ == "__main__":
    output = handler({"body": "0fe7c2ad-f12c-461e-ab93-a580072fe255"},"")
    with open("output.json","w") as f:
        f.write(json.dumps(output))
