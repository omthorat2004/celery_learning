from celery_learning.celery_app import app
from celery_learning.celery_app import app
import time

@app.task
def welcome_email() ->str:
    # Implement the welcome email
    time.sleep(10)
    print("Welcome Email sent!")
    return "Email response"
    
@app.task
def payment_email() ->str:
    # Implement the payment email
    time.sleep(10)
    print("Implement Payment Email")
    return "Payment Response"
