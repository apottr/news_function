import pandas as pd
import geopandas as gp
from shapely.wkt import loads
from pathlib import Path
from bs4 import BeautifulSoup
import botocore.vendored.requests as requests
from botocore.exceptions import ClientError
import arrow,boto3,json,re

def grab_page(url,geometry,stamp,uuid):
    fname = re.sub(r"[\/\?:\.\\&#]","_",url)
    s3 = boto3.client('s3')
    with open(f'/tmp/{fname}',"wb") as f:
        r = requests.get(url)
        f.write(r.content)
    try:
        resp = s3.upload_file(f'/tmp/{fname}', 'collector-storage', f'news/{uuid}/{stamp.year}/{stamp.month}/{stamp.day}/{stamp.hour}/{fname}',
                                ExtraArgs={'Metadata': {'footprint': geometry}})
    except ClientError as e:
        print(e)
        return False
    return True

def renderer(row,uuid):
    url = getattr(row,"url")
    geometry = getattr(row,"geometry")
    stamp = arrow.utcnow()
    on_s3 = grab_page(url,geometry,stamp,uuid)
    return {
        "url": url,
        "geo": geometry,
        "timestamp": stamp.format(),
        "on_s3": on_s3
    }

def load_df():
    df = pd.read_csv(str(Path(__file__).parent / "output.csv"))
    df["geometry"] = df.apply(lambda x: loads(x.geometry), axis=1)
    gdf = gp.GeoDataFrame(df)
    return gdf

def get_points_from_bbox(bbox,uuid):
    df = load_df()
    nw,se = bbox["nw"],bbox["se"]
    filtered = df.cx[nw[0]:se[0],nw[1]:se[1]]
    filtered["geometry"] = filtered.apply(lambda x: x.geometry.wkt, axis=1)
    out = []
    for row in filtered.itertuples():
        out.append(renderer(row,uuid))
    return out

def get_bounds_from_dynamo(uuid):
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table('bounds-objects')
    resp = table.get_item(Key={"uuid": uuid})
    bbox = resp['Item']['bbox']
    return {k: [float(v[0]),float(v[1])] for k,v in bbox.items()}

if __name__ == "__main__":
    uuid = "0fe7c2ad-f12c-461e-ab93-a580072fe255"
    bbox = get_bounds_from_dynamo(uuid)
    with open("output.json","w") as f:
        f.write(json.dumps(get_points_from_bbox(bbox,uuid)))
