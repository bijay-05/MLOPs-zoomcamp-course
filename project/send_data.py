import requests

info = {

}

url = 'http://localhost:9696/predict'
response = requests.post(url, json=info)
print(response.json())