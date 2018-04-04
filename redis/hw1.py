import requests
import json

params  = {"date":"2017-01-24"}
r = requests.get('https://api.nasa.gov/planetary/apod?api_key=Ch6kzRgOWgob3Oxm3wWbNHb0GZpYW5fe2UGMtokg', params)

json_string = r.text
data = json.loads(json_string)

print(data['url'])

