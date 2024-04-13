# install cassandra docker
- At terminal: "docker pull cassandra"
- Reference: https://hub.docker.com/_/cassandra

# install storm 
- Reference: https://www.youtube.com/watch?v=2N2Jiepp_Y8

# install python library support for cassandra and storm
- pip install streamparse
- pip install cassandra-driver

# cd to working folder:
- In terminal: cd "path/to/this_folder"
# Run cassandra:
- In terminal: "sudo docker compose up -d "

# Run ZooKeeper:
- In terminal: "zkServer.sh start"

# Run storm nimbus, storm supervisor, storm ui
- In terminal: "storm nimbus"
- In new terminal: "storm supervisor"
- In new terminal: "storm ui"
- Open webbrowser, key in "localhost:8080", you should see the storm ui

# Create empty table for cassandra by running all command in "de_project.cql"
- In terminal: "python3 create_table.py"
- In terminal:
    - To check any topology is running: "storm list"
    - To kill the topology: "storm kill topology_name"
    - To create the storm topology: "sparse run"
    - Normal, after we edit the code in Spout_Class, Bolt_Class, Topology_Class, we will check if old topology is running. If yes, kill it, then run "sparse run" in terminal to create a new topology

# Check the result :
- In terminal: "cqlsh" to enter the cqlsh commander
- "Use iotsolution" (iotsolution is the keyspace)
- "Select * from wind_brone_table limit 10"; (get 10 rows only)
- "Select * from weather_brone_table limit 10"; (get 10 rows only)
- "Select * from wind_silver_table limit 10"; (get 10 rows only)
- "Select * from weather_silver_table limit 10"; (get 10 rows only)
- "Select * from golden_table limit 10"; (get 10 rows only)




