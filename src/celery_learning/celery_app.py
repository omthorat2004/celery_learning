from celery import Celery


app = Celery(__name__)

app.config_from_object("celery_learning.config.celery_config")




