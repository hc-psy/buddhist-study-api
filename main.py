from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

import pandas as pd
from db_query import get_geo_by_location, get_weekly_by_location

# app configuration
app = FastAPI()

origins = ["http://localhost:3000", "http://localhost:3000/"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# data cache
# df = pd.read_csv("./data/small_data.csv")
# df['media_type'] = df['media_type'].apply(
#     lambda x: str(x).split("=")[-1].strip())
# df['biliography_language'] = df['biliography_language'].apply(
#     lambda x: str(x).split("=")[-1].strip())


@app.get("/")
def index():
    return {"message": "Hello World!"}


@app.get('/getUuserUclickGeo/')
async def get_uuser_uclick_geo(reqContinent: str, reqCountry: str, reqCity: str):
    try:
        results = get_geo_by_location(reqContinent, reqCountry, reqCity)
        return results
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")


@app.get('/getWeeklyGeo/')
async def get_uuser_uclick_geo(reqContinent: str, reqCountry: str):
    try:
        results = get_weekly_by_location(reqContinent, reqCountry)
        return results
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")

if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, log_level="info", reload=True)
