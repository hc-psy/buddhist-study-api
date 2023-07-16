from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

import pandas as pd


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
df = pd.read_csv("./data/small_data.csv")
df['media_type'] = df['media_type'].apply(
    lambda x: str(x).split("=")[-1].strip())
df['biliography_language'] = df['biliography_language'].apply(
    lambda x: str(x).split("=")[-1].strip())


@app.get("/")
def index():
    return {"message": "Hello World!"}


@app.get('/getUuserUclickGeo/')
async def get_uuser_uclick_geo(reqContinent: str, reqCountry: str, reqCity: str):
    try:
        # Conditionally filter the DataFrame based on the provided values
        if reqContinent == "All Continents" and reqCountry == "All Countries" and reqCity == "All Cities":
            filtered_df = df.copy()
        else:
            filtered_df = df[
                (df.continent == reqContinent if reqContinent != "All Continents" else True) &
                (df.country == reqCountry if reqCountry != "All Countries" else True) &
                (df.city == reqCity if reqCity != "All Cities" else True)
            ]

        grouped = filtered_df.groupby(['lon', 'lat']).agg({'ip': pd.Series.nunique, 'lon': 'count'}).rename(
            columns={'ip': 'userscount', 'lon': 'datacount'}).reset_index()

        return {"result": grouped.values.tolist()}
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")

if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, log_level="info", reload=True)
