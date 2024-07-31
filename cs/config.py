import os

AWS_CONFIG = {
  "stg": {
    "region": "us-east-2",
    "topic": "arn:aws:sns:us-east-2:860782241405:data-groceries-recommendation-crossselling-stg"
  },
  "pro": {
    "region": "us-east-1",
    "topic": "arn:aws:sns:us-east-1:860782241405:data-groceries-recommendation-crossselling-live"
  }
}

SECRET_CONFIG = {
  "stg": {
    "project": "peya-food-and-groceries",
    "secret_id": "peya-sa-groceries-recommendation-crossselling-stg-key"
  },
  "pro": {
    "project": "peya-food-and-groceries",
    "secret_id": "peya-sa-groceries-recommendation-crossselling-live-key"
  }
}


ENV = os.getenv("env")
PROJECT_ID='peya-food-and-groceries'
DATA_SET ='cross_selling'


MIN_THRESHOLD = 1
METRIC='lift'

FROM_DATE=-60
TO_DATE=-1
