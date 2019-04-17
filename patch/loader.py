import pandas as pd
import geopandas as gp
from shapely.wkt import loads
from pathlib import Path
from bs4 import BeautifulSoup
import botocore.vendored.requests as requests

def getter(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text,"xml")
    return [item.text for item in soup.select("item > title")]
def renderer(row):
    return {
        "url": getattr(row,"url"),
        "geo": getattr(row,"geometry"),
        "headlines": getter(getattr(row,"url"))
    }

def load_df():
    df = pd.read_csv(str(Path(__file__).parent / "output.csv"))
    df["geometry"] = df.apply(lambda x: loads(x.geometry), axis=1)
    gdf = gp.GeoDataFrame(df)
    return gdf

def get_points_from_bbox(bbox):
    df = load_df()
    nw,se = bbox["nw"],bbox["se"]
    filtered = df.cx[nw[0]:se[0],nw[1]:se[1]]
    filtered["geometry"] = filtered.apply(lambda x: x.geometry.wkt, axis=1)
    out = []
    for row in filtered.itertuples():
        out.append(renderer(row))
    return out

if __name__ == "__main__":
    print(get_points_from_bbox({"nw": [-71.30126953125,42.53486817758702], "se": [-70.7958984375,42.204107493733176] }))
