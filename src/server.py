import fastapi

app = fastapi.FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/start-collector/")
async def start_collector():
    ticker.start()
    return {"status": "Ticks collector started"}
