from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import shapely.wkb
from overturemaps.core import record_batch_reader
import json
from fastapi.middleware.cors import CORSMiddleware # Import CORSMiddleware

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins - modify this for production
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

@app.get("/api/download")
async def download_data(
    bbox: str = Query(..., description="Bounding box in format: minlon,minlat,maxlon,maxlat"),
    output_format: str = Query("geojson", description="Output format (geojson, geojsonseq, geoparquet)"),
    type_: str = Query(..., description="Overture data type (e.g., building, place, address)")
):
    try:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(','))

        reader = record_batch_reader(type_, (min_lon, min_lat, max_lon, max_lat))
        if reader is None:
            return JSONResponse(content={"error": "No data found for the specified parameters."}, status_code=404)

        if output_format == "geojson":
            geojson_data = {"type": "FeatureCollection", "features": []}
            for batch in reader:
                try:
                    for row in batch.to_pylist():
                        try:
                            geojson_data["features"].append(row_to_geojson_feature(row))
                        except Exception as e:
                            print(f"Error converting row to GeoJSON: {e}")
                            print(f"Row data: {row}")
                except Exception as e:
                    print(f"Error processing batch: {e}")
                    print(f"Batch data: {batch}")
            return JSONResponse(content=geojson_data) # Return GeoJSON directly

        elif output_format in ("geojsonseq", "geoparquet"):
            return JSONResponse(content={"error": f"Output format '{output_format}' not yet implemented."}, status_code=501)

        else:
            return JSONResponse(content={"error": "Invalid output format."}, status_code=400)

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

def row_to_geojson_feature(row):
    geometry = shapely.wkb.loads(row.pop("geometry"))
    row.pop("bbox")
    properties = {k: v for k, v in row.items() if k != "bbox" and v is not None}
    return {
        "type": "Feature",
        "geometry": geometry.__geo_interface__,
        "properties": properties,
    }

@app.get("/")
async def root():
    return {"message": "Welcome to the Overture Maps Data API"}
