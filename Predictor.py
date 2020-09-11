# -*- coding:utf-8 -*-
import requests, json, base64, os
import numpy as np
import random as rd

from io import BytesIO

# PIL library comes as a dependancy
from PIL import Image

# Internal library to work with Object Storage
from com.obs.client.obs_client import ObsClient

def handler(event, context):
    """
    Function triggers each time there is a new object created in the Object Storage bucket
    Input:
        event: JSON-structure with details of the event which triggered the function
        context: security context of the function
    Output:
        it's optional (we will return a completion message string)
    """
    
    # Get Access Key/Secret Key to work with OBS from the security context 
    ak = context.getAccessKey()
    sk = context.getSecretKey()
    
    # Set up endpoint for OBS
    endpoint = "obs.ru-moscow-1.hc.sbercloud.ru"

    # Set up logger
    logger = context.getLogger()
    
    # Get bucket name, file name and file size from the event details
    bucket_name = event['Records'][0]['obs']['bucket']["name"]
    file_name = event['Records'][0]['obs']['object']["key"]
    file_size = event['Records'][0]['obs']['object']["size"]
    logger.info("File %s received, size is %s" % (file_name, file_size))

    # We will only process files of non-zero size
    if (file_size > 0):
        # Get Auth token for Christofari
        X_API_KEY = os.environ['X_API_KEY']
        EMAIL = os.environ['EMAIL']
        PASSWORD = os.environ['PASSWORD']        

        resp = requests.post("https://api.aicloud.sbercloud.ru/public/v1/auth", 
            json={
                "email": EMAIL,
                "password": PASSWORD
            },
            headers={
                "X-Api-Key": X_API_KEY
            }
        )

        if (resp.status_code == 200):
            data = resp.json()
            ACCESS_TOKEN = data['token']['access_token']
        else:
            print("Couldnt authorize with these args")
            return "Failed to authorize with Christofari"

        # Open connection to OBS
        conn = ObsClient(access_key_id=ak, secret_access_key=sk, server=endpoint, path_style=True, region="ru-moscow-1")

        # Construct the local path of the incoming file
        local_incoming_file_name = os.path.join(os.sep, "tmp", file_name)
        
        # Download the file from the bucket
        resp = conn.getObject(bucket_name, file_name, local_incoming_file_name)

        # Open the image and convert to black & white
        img = Image.open(local_incoming_file_name).convert("L")

        # Thumbnail the image 
        original_width, original_height = img.size
        new_width = 28
        new_height = int(original_height * new_width / original_width)

        img.thumbnail((new_width, new_height), Image.ANTIALIAS)

        imgByteArr = BytesIO()
        img.save(imgByteArr, format='PNG')
        imgByteArr = imgByteArr.getvalue()

        img_b64 = base64.b64encode(imgByteArr)

        encoded_img_b64 = img_b64.decode("utf-8")

        resp = requests.post("https://api.aicloud.sbercloud.ru/public/v1/inference/v1/predict/kfserving-1599815581/kfserving-1599815581/", 
            headers={
                "X-Api-Key": X_API_KEY,
                "Authorization": ACCESS_TOKEN,
                "Content-Type": "application/json"
                
            },
            data=json.dumps({
                "instances": [
                    {
                        "image": {"b64": encoded_img_b64 }
                    }
                ]
                })
        )
            
        if (resp.status_code == 200):
            data = json.loads(resp.json()["body"])
            prediction = data["Prediction"]
            print("*********")
            print(f"Christofari thinks that it's {prediction}")
            print("*********")
        else:
            print(resp.status_code)

    return "File processed."
