"""


Hit RUN to see this project in action!

This is a Python script that showcases the scraping capabilities of Python.

It scrapes the current temperature from weather.com and prints it to the console. It keeps doing it every 30 seconds.

Feel free to edit the code to scrape other websites depending on your needs!
"""

import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json

import asyncio

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = ""
EVENT_HUB_NAME = "getdata"

city = "USNY0996:1:US"  # New York city code
city_name = "New York"

def get_time_tag(tag):

  return tag.has_attr('class') and any(
      "CurrentConditions--timestamp--" in str(i) for i in tag['class'])

def get_temp_tag(tag):
  # this may or may not be still valid; keep in mind
  # that yahoo may change their html layout in the future
  # and thus this script may no longer work; check
  # if the rule below still works in case of errors
  return tag.has_attr('class') and any(
      "CurrentConditions--tempValue--" in str(i) for i in tag['class'])

def get_sky_tag(tag):

  return tag.has_attr('class') and any(
    "CurrentConditions--phraseValue--" in str(i) for i in tag['class'])

def fetch_weather(city):
  url = f"https://weather.com/weather/today/l/{city}"
  response = requests.get(url)
  soup = BeautifulSoup(response.content, 'html.parser')
  
  time_element = soup.find(get_time_tag)
  time = time_element.text if time_element else "NA"
  
  temp_element = soup.find(get_temp_tag)
  temp = temp_element.text if temp_element else "NA"

  sky_element = soup.find(get_sky_tag)
  sky = sky_element.text if sky_element else "NA"

  weather_df = pd.DataFrame([[time, temp, sky]], columns=['Time', 'Temperature', 'Sky'])
  
  return weather_df.to_dict('records')

async def send_json_msg():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData(body=json.dumps(current_weather_json)))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)
        print("Message sent successfully.")



print(
    "Weather script is running."
    f" Will keep checking the current weather in {city_name} every 30 seconds...\n\n"
)

if __name__ == "__main__":
  while True:
    current_weather_json = fetch_weather(city) #change to json object
    print(json.dumps(current_weather_json, indent=4))
    asyncio.run(send_json_msg())
    time.sleep(30)
