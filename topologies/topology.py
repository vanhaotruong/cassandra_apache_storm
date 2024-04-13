from streamparse import Grouping, Topology
from bolts import *
from spouts import *
from streamparse import run
from cassandra.cluster import Cluster

class MyTopology(Topology):
    wind_spout = Wind_Spout.spec()
    wind_brone_bolt = Wind_Brone_Bolt.spec(inputs={wind_spout: Grouping.fields("device_id")})
    wind_silver_bolt = Wind_Silver_Bolt.spec(inputs={wind_brone_bolt: Grouping.fields("device_id")})

    weather_spout = Weather_Spout.spec()
    weather_brone_bolt = Weather_Brone_Bolt.spec(inputs={weather_spout: Grouping.fields("device_id")})
    weather_silver_bolt = Weather_Silver_Bolt.spec(inputs={weather_brone_bolt: Grouping.fields("device_id")})
    golden_bolt = Golden_Bolt.spec(inputs={wind_silver_bolt: Grouping.fields("device_id"), 
                                           weather_silver_bolt: Grouping.fields("device_id")})


