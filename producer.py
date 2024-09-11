import json
import time
from kafka import KafkaProducer
import random
from datetime import datetime
import logging

# Configure logging to write into a file
logging.basicConfig(level=logging.INFO, filename='producer.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def serializer(message):
    return json.dumps(message).encode('utf-8')
                                      
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=serializer)

# Define weather data generation function
def generate_weather_data(city):
    if city == "Winnipeg":
        temperature = round(random.uniform(-30, 30), 1)
        wind_speed = round(random.uniform(0, 50), 1)
        humidity = round(random.uniform(20, 80), 1)
    elif city == "Vancouver":
        temperature = round(random.uniform(-10, 25), 1)
        wind_speed = round(random.uniform(0, 30), 1)
        humidity = round(random.uniform(30, 99), 1)
    else:
        raise ValueError("Invalid city")
    
    return {
        "city": city,
        "temperature": temperature,
        "wind_speed": wind_speed,
        "humidity": humidity
    }

if __name__ == "__main__":
    while True:
        for city in ["Winnipeg", "Vancouver"]:
            sent_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            weather_data = generate_weather_data(city)
            logger.info(f"Produced: {weather_data}")
            producer.send('weather-data', value=weather_data)
            print(f"Produced: {weather_data}")
            logger.info(f"Transmitted: {weather_data}, Sent at: {sent_at}")
        time.sleep(5)

