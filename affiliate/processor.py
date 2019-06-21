import re,arrow,boto3,json
import geopandas as gp
import pandas as pd
from shapely.geometry import Point
from bs4 import BeautifulSoup
from pathlib import Path
import botocore.vendored.requests as requests
import urllib3; urllib3.disable_warnings()

directory = Path(__file__).resolve().parent

def load_csv():
    df = pd.read_csv(str(directory / "aff-final.csv"),sep=",")
    df["geometry"] = df.apply(lambda z: Point(z.lon,z.lat),axis=1)
    return gp.GeoDataFrame(df)

def bbox_intersects(gdf,bbox):
    nw,se = bbox["nw"],bbox["se"]
    return gdf.cx[nw[0]:se[0],nw[1]:se[1]]

def grab_page(url,geometry,stamp,uuid):
    fname = re.sub(r"[\/\?:\.\\&#]","_",url)
    s3 = boto3.client('s3')
    with open(f'/tmp/{fname}',"wb") as f:
        r = requests.get(url,headers={"User-Agent": "firefox"},verify=False)
        f.write(r.content)
    try:
        resp = s3.upload_file(f'/tmp/{fname}', 'collector-storage', f'news/{uuid}/{stamp.year}/{stamp.month}/{stamp.day}/{stamp.hour}/{fname}',
                                ExtraArgs={'Metadata': {'footprint': geometry}})
    except ClientError as e:
        print(e)
        return False
    return True

def renderer(row,uuid):
    url = getattr(row,"website")
    geo = Point(getattr(row,"lon"),getattr(row,"lat")).wkt
    timestamp = arrow.utcnow()
    return {
        "callsign": getattr(row,"callsign"),
        "geo": geo,
        "url": url,
        "thumbnail": getattr(row,"logo"),
        "on_s3": grab_page(url,geo,timestamp,uuid)
    }

def get_points_from_bbox(bbox,uuid):
    df = load_csv()
    d = bbox_intersects(df,bbox)
    out = []
    for row in d.itertuples():
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