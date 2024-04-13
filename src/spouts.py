from streamparse import Spout
import csv, os
import shutil

class Wind_Spout(Spout):
    outputs = ["angle", "device_id", "recorded_date", "rpm", "window"]

    def initialize(self, stormconf, context):

        csv_file_path = '/home/hp/Study/01_Ky_thuat_du_lieu/bai_tap_lon/raw_data/raw_wind_sensor.csv'
        self.file = open(csv_file_path, 'r')
        self.reader = csv.reader(self.file)
        next(self.reader, None)  # Skip the header

    def next_tuple(self):
        if self.file.closed:
            return
        try:
            line = next(self.reader)
            angle, device_id, recorded_date, rpm, window = line
            self.emit([angle, device_id, recorded_date, rpm, window])
        except StopIteration:
            self.file.close()

    def close(self):
        self.file.close()

class Weather_Spout(Spout):
    outputs = ["device_id", "humidity", "recorded_date", "temperature", "winddirection", "window", "windspeed"]

    def initialize(self, stormconf, context):
        # Open the CSV file
        csv_file_path ='/home/hp/Study/01_Ky_thuat_du_lieu/bai_tap_lon/raw_data/raw_weather_sensor.csv'
        self.file = open(csv_file_path, 'r')
        self.reader = csv.reader(self.file)
        next(self.reader, None)  # Skip the header

    def next_tuple(self):
        if self.file.closed:
            return
        try:
            line = next(self.reader)
            device_id, humidity, recorded_date, temperature, winddirection, window, windspeed = line
            self.emit([device_id, humidity, recorded_date, temperature, winddirection, window, windspeed])
        except StopIteration:
            self.file.close()

    def close(self):
        self.file.close()
