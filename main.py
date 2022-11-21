
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from utils import get_form_data

app = FastAPI()


@app.get("/")
async def docs_redirect():
    """Redirects the main page to the events

    Returns:
        RedirectResponse: A reditect
    """
    return RedirectResponse(url='/health')


@app.get("/health")
async def get_health():
    return {'result': 'Very Healthy'}


@app.get("/form/{form_id}")
def get_form(form_id: str):
    result = get_form_data(form_id=form_id)
    return result
