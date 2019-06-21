import boto3,uuid
from boto3.dynamodb.types import TypeSerializer

def get_bounds_from_dynamo(uuid):
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table('bounds-objects')
    resp = table.get_item(Key={"uuid": uuid})
    bbox = resp['Item']['bbox']
    return {k: [float(v[0]),float(v[1])] for k,v in bbox.items()}

def create_bounds(bbox):
    id = str(uuid.uuid4())
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table('bounds-objects')
    table.put_item(Item={
        "uuid": id,
        "bbox": bbox
    })
    return id

def main():
    bbox = {"nw": ['-71.30126953125','42.53486817758702'], "se": ['-70.7958984375','42.204107493733176'] }
    id = create_bounds(bbox)
    print(id)
    print(get_bounds_from_dynamo(id))

if __name__ == "__main__":
    #main()
    print(get_bounds_from_dynamo("0fe7c2ad-f12c-461e-ab93-a580072fe255"))