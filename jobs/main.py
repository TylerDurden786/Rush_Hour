# Importing pre-defined libraries
import json
import os
import random
import uuid
from datetime import datetime, timedelta
import time
from confluent_kafka import SerializingProducer

# Journey starts from Pune
PUNE_COORDINATES = {"latitude": 18.60, "longitude": 73.76}
# Journey ends in Mumbai
MUMBAI_COORDINATES = {"latitude": 19.02, "longitude": 72.84}

# Calculate movement increments
LATITUDE_INCREMENT = (MUMBAI_COORDINATES['latitude'] - PUNE_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (MUMBAI_COORDINATES['longitude'] - PUNE_COORDINATES['longitude']) / 100

# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42)

# Current time
start_time = datetime.now()
start_location = PUNE_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time = start_time + timedelta(seconds=random.randint(30, 60))

    return start_time


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(30, 40),
        'weather_condition': random.choice(['Sunny', 'Rainy', 'Cloudy', 'Snowy']),
        'precipitation': random.uniform(0, 40),
        'window_speed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'air_quality_index': random.uniform(0, 500)
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'incident_id': uuid.uuid4(),
        'timestamp': timestamp,
        'location': location,
        'type': random.choice(['Accident', 'Fire', 'Flood', 'Earthquake']),
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-West',
        'vehicle_type': vehicle_type
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'location': location,
        'camera_id': camera_id,
        'snapshot': 'Base64EncodedString'
    }


def simulate_vehicle_movement():
    global start_location

    # Move towards Mumbai-Dadar
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # Adding randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-West',
        'make': 'SHIVNERI',
        'model': 'MSRTC',
        'year': 2024,
        'fuel_type': 'Petrol'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not a JSON serializable.')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer_parameter, topic, data):
    # Convert timestamp string to datetime object
    timestamp_dt = datetime.fromisoformat(data['timestamp'])

    # Convert datetime object to milliseconds
    timestamp_ms = int(timestamp_dt.timestamp() * 1000)
    producer_parameter.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        timestamp=timestamp_ms,
        on_delivery=delivery_report
    )

    producer.flush()


def simulate_journey(producer_arg, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'NIKON')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incidence_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        if (vehicle_data['location'][0] >= MUMBAI_COORDINATES['latitude']
                and vehicle_data['location'][1] <= MUMBAI_COORDINATES['longitude']):
            print('Vehicle has reached Mumbai. Simulation ending...')
            break

        produce_data_to_kafka(producer_arg, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer_arg, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer_arg, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer_arg, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer_arg, EMERGENCY_TOPIC, emergency_incidence_data)

        time.sleep(5)


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-RUSH-HOUR-007')
    except KeyboardInterrupt:
        print('Simulation ended by the user.')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
