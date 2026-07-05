from fastapi import FastAPI
from pydantic import BaseModel

from .workers.tasks import payment_email, welcome_email


class Response(BaseModel):
    success:str="Response Sent"

app = FastAPI()



@app.post("/register",response_model=Response)
def get_response():
    result = welcome_email.delay()
    print(result.id) # in future we can get task resulyt by using the AsyncResult. Just need to pass id and app
    return Response()

@app.post('/payment')
def get_response():
    result = payment_email.delay()
    print(result.id)
    return Response(success="Payment Sent")