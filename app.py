from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from uvicorn import run as app_run


from typing import Optional

from us_visa.constants import APP_HOST, APP_PORT
from us_visa.pipline.prediction_pipeline import USvisaData, USvisaClassifier
from us_visa.pipline.training_pipeline import TrainingPipeline


app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# form handler
class DataForm:
    def __init__(self, request: Request):
        self.request = request

    async def get_data(self):
        form = await self.request.form()

        def to_int(value, default=0):
            try:
                return int(value)
            except (TypeError, ValueError):
                return default
            
        def to_float(value, default=0.8):
            try:
                return float(value)
            except (TypeError, ValueError):
                return default
            

        return {
            "continent": form.get("continent"),
            "education_of_employee": form.get("education_of_employee"),
            "has_job_experience": form.get("has_job_experience"),
            "requires_job_training": form.get("requires_job_training"),
            "no_of_employees": to_int(form.get("no_of_employees")),
            "region_of_employment": form.get("region_of_employment"),
            "prevailing_wage": to_float(form.get("prevailing_wage")),
            "unit_of_wage": form.get("unit_of_wage"),
            "full_time_position": form.get("full_time_position"),
            "yr_of_estab": to_int(form.get("yr_of_estab"))
        }

# routes
@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        "usvisa.html",
        {"request": request, "context": "Rendering"}
    )

@app.get("/train")
async def train():
    try:
        pipeline = TrainingPipeline()
        pipeline.run_pipeline()
        return Response("Training successful")
    except Exception as e:
        return Response(f"Training failed: {e}")
    

classifier = USvisaClassifier()

@app.post("/")
async def predict(request: Request):
    try:
        form = DataForm(request)
        data = await form.get_data()

        usvisa_data = USvisaData(**data)
        df = usvisa_data.get_usvisa_input_data_frame()

        # classifier = USvisaClassifier()
        prediction = classifier.predict(df)[0]

        status = "Visa-approved" if prediction == 0 else "Visa Not-Approved"

        return templates.TemplateResponse(
            "usvisa.html",
            {"request": request, "context": status}
        )
    except Exception as e:
        return {"status": False, "error": str(e)}
    
if __name__ == "__main__":
    app_run(app, host=APP_HOST, port=APP_PORT, log_level="info")