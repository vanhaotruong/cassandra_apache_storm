from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from streamparse import Bolt
import uuid
from datetime import datetime

class Wind_Brone_Bolt(Bolt):
    outputs = ["angle", "device_id", "recorded_date", "rpm", "window"]

    def initialize(self, conf, ctx):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('iotsolution')
        pass

    def process(self, tup):
        angle, device_id, recorded_date, rpm, window = tup.values

        angle = float(angle)
        device_id = str(device_id)
        recorded_date = datetime.strptime(recorded_date, '%m/%d/%Y')
        recorded_date = recorded_date.strftime('%Y-%m-%d')
        rpm = float(rpm)
        window = datetime.strptime(window, '%Y-%m-%dT%H:%M:%S.%f%z')
        window = window.strftime('%Y-%m-%d %H:%M:%S')

        self.logger.info(f"angle: {angle}, device_id: {device_id}, recorded_date: {recorded_date}, rpm: {rpm}, window: {window}")

        id = uuid.uuid4()
        query = "INSERT INTO wind_brone_table (id, angle, device_id, recorded_date, rpm, window) VALUES (%s, %s, %s, %s, %s, %s)"
        self.session.execute(query, (id, angle, device_id, recorded_date, rpm, window))

        self.emit((angle, device_id, recorded_date, rpm, window))


    def cleanup(self):
        if self.cluster is not None and not self.cluster.is_shutdown:
            self.cluster.shutdown()

class Weather_Brone_Bolt(Bolt):
    outputs = ["device_id", "humidity", "recorded_date", "temperature", "winddirection", "window", "windspeed"]

    def initialize(self, conf, ctx):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('iotsolution')

    def process(self, tup):
        device_id, humidity, recorded_date, temperature, winddirection, window, windspeed = tup.values

        device_id = str(device_id)
        humidity = float(humidity)
        recorded_date = datetime.strptime(recorded_date, '%m/%d/%Y')
        recorded_date = recorded_date.strftime('%Y-%m-%d')
        temperature = float(temperature)
        winddirection = str(winddirection)
        window = datetime.strptime(window, '%Y-%m-%dT%H:%M:%S.%f%z')
        window = window.strftime('%Y-%m-%d %H:%M:%S')
        windspeed = float(windspeed)

        id = uuid.uuid4()
        query = "INSERT INTO weather_brone_table (id, device_id, humidity, recorded_date, temperature, winddirection, window, windspeed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        self.session.execute(query, (id, device_id, humidity, recorded_date, temperature, winddirection, window, windspeed))
        
        self.emit((device_id, humidity, recorded_date, temperature, winddirection, window, windspeed))  

    def cleanup(self):
        self.cluster.shutdown()

class Wind_Silver_Bolt(Bolt):
    outputs = ["device_id", "window_hourly", "avg_angle", "avg_rpm"]

    def initialize(self, conf, ctx):
        self.data = {}
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('iotsolution')

    def process(self, tup):
        angle, device_id, recorded_date, rpm, window = tup.values
        angle = float(angle)
        device_id = str(device_id)
        recorded_date = str(recorded_date) 
        rpm = float(rpm)
        window = datetime.strptime(window, '%Y-%m-%d %H:%M:%S')
        window_hourly = window.strftime('%Y-%m-%dT%H')

        key = (device_id, window_hourly)
        if key not in self.data:
            self.data[key] = {'angle_sum': 0, 'angle_count': 0, 'rpm_sum': 0, 'rpm_count': 0}
        self.data[key]['angle_sum'] += angle
        self.data[key]['angle_count'] += 1
        self.data[key]['rpm_sum'] += rpm
        self.data[key]['rpm_count'] += 1
        avg_angle = self.data[key]['angle_sum'] / self.data[key]['angle_count']
        avg_rpm = self.data[key]['rpm_sum'] / self.data[key]['rpm_count']

        id = uuid.uuid4()
        query = "INSERT INTO wind_silver_table (id, device_id, window_hourly, avg_angle, avg_rpm) VALUES (%s, %s, %s, %s, %s)"
        self.session.execute(query, (id, device_id, window_hourly, avg_angle, avg_rpm))    

        self.emit((device_id, window_hourly, avg_angle, avg_rpm)) 

    def cleanup(self):
        self.cluster.shutdown()

class Weather_Silver_Bolt(Bolt):
    outputs = ["device_id", "window_hourly", "avg_temperature", "avg_humidity", "avg_windspeed"]

    def initialize(self, conf, ctx):
        self.data = {}
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('iotsolution')

    def process(self, tup):
        device_id, humidity, recorded_date, temperature, winddirection, window, windspeed = tup.values
        
        device_id = str(device_id)
        humidity = float(humidity)
        recorded_date = str(recorded_date) # recorded_date emit from *_Brone_Bolt is string with format '%Y-%m-%d', so we need to convert to datetime and modify format
        temperature = float(temperature)
        winddirection = str(winddirection)
        windspeed = float(windspeed)
        window = datetime.strptime(window, '%Y-%m-%d %H:%M:%S')
        window_hourly = window.strftime('%Y-%m-%dT%H')

        key = (device_id, window_hourly)
        if key not in self.data:
            self.data[key] = {'temperature_sum': 0, 'temperature_count': 0, 'humidity_sum': 0, 'humidity_count': 0, 'windspeed_sum': 0, 'windspeed_count': 0}
        self.data[key]['temperature_sum'] += temperature
        self.data[key]['temperature_count'] += 1
        self.data[key]['humidity_sum'] += humidity
        self.data[key]['humidity_count'] += 1
        self.data[key]['windspeed_sum'] += windspeed
        self.data[key]['windspeed_count'] += 1
        avg_temperature = self.data[key]['temperature_sum'] / self.data[key]['temperature_count']
        avg_humidity = self.data[key]['humidity_sum'] / self.data[key]['humidity_count']
        avg_windspeed = self.data[key]['windspeed_sum'] / self.data[key]['windspeed_count']

        id = uuid.uuid4()
        query = "INSERT INTO weather_silver_table (id, device_id, window_hourly, avg_temperature, avg_humidity, avg_windspeed) VALUES (%s, %s, %s, %s, %s, %s)"
        self.session.execute(query, (id, device_id, window_hourly, avg_temperature, avg_humidity, avg_windspeed))

        
        self.emit((device_id, window_hourly, avg_temperature, avg_humidity, avg_windspeed))

    def cleanup(self):
        self.cluster.shutdown()


        
class Golden_Bolt(Bolt):
    outputs = ["device_id", "window_hourly", "avg_temperature", "avg_humidity", "avg_windspeed", "avg_angle", "avg_rpm"]

    def initialize(self, conf, ctx):
        self.data = {}
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect('iotsolution')
        self.wind_data = {}
        self.weather_data = {}

    def process(self, tup):
        device_id = tup.values[0]
        window_hourly = tup.values[1]
        data = tup.values[2:]

        if tup.component == 'wind_silver_bolt':
            self.wind_data[(device_id, window_hourly)] = data
            avg_angle, avg_rpm = data
        elif tup.component == 'weather_silver_bolt':
            self.weather_data[(device_id, window_hourly)] = data
            avg_temperature, avg_humidity, avg_windspeed = data

        key = (device_id, window_hourly)
        if key in self.wind_data and key in self.weather_data:
            merged_data = key + self.wind_data[key] + self.weather_data[key]
            device_id, window_hourly, avg_angle, avg_rpm, avg_temperature, avg_humidity, avg_windspeed = merged_data
            # Insert merged_data into Cassandra
            id = uuid.uuid4()
            query = SimpleStatement(
                "INSERT INTO golden_table (id, device_id, window_hourly, avg_angle, avg_rpm, avg_temperature, avg_humidity, avg_windspeed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                consistency_level=ConsistencyLevel.ONE
            )
            self.session.execute(query, (id, device_id, window_hourly, avg_angle, avg_rpm, avg_temperature, avg_humidity, avg_windspeed))      

    def cleanup(self):
        self.cluster.shutdown()