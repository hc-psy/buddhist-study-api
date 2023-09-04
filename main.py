from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# from db_query import get_geo_by_location, get_weekly_by_location
from pd_query import (get_geo_by_location, get_weekly_by_location,
                      get_weekly_id_geo, get_search, get_network, get_arcs_arcs, get_arcs_points)

# app configuration
app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
async def get_weekly_geo(reqContinent: str, reqCountry: str):
    try:
        results = get_weekly_by_location(reqContinent, reqCountry)
        return results
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")


@app.get('/getWeeklyMap/')
async def get_weekly_map():
    try:
        results = get_weekly_id_geo()
        return results
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")


@app.get("/search/")
async def getSearch(query: str):
    try:
        results = get_search(query)
        return results
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")


@app.get("/network/")
async def getNetwork(method: str, query: str):
    try:
        query = query.split(',')
        results = get_network(query, method)
        return results
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")


@app.get("/arcs/points/")
async def getArcsPoints():
    try:
        results = get_arcs_points()
        return results
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")


@app.get("/arcs/arcs/")
async def getArcsArcs(lat_lon: str):
    try:
        results = get_arcs_arcs(lat_lon)
        return results
    except:
        raise HTTPException(status_code=404, detail="Data Not Found!")


# if __name__ == "__main__":
#     uvicorn.run("main:app", port=8000, log_level="info", reload=True)
