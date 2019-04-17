try:
    import unzip_requirements
except ImportError:
    pass

from affiliate import get_points_from_bbox as affiliate
from patch import get_points_from_bbox as patch
import json
def runner(bbox):
    try:
        x = affiliate(bbox)
    except:
        x = []
    try:
        y = patch(bbox)
    except:
        y = []
    return x+y

def handler(event, context):
    b = json.loads(event["body"])
    return {"data": runner(b["bbox"])}


if __name__ == "__main__":
    bounds = json.dumps({"bbox": {"nw": [-71.30126953125,42.53486817758702], "se": [-70.7958984375,42.204107493733176] }})
    output = handler({"body": bounds},"")
    with open("output.json","w") as f:
        f.write(json.dumps(output))
