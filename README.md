# cassandra_storm
cassandra &amp; storm docker compose

# install cassandra docker
- At terminal: "docker pull cassandra"
- Reference: https://hub.docker.com/_/cassandra

# install storm docker
- At terminal: docker pull storm
- Reference: https://hub.docker.com/_/storm

# Run docker compose:
- sudo docker images
- sudo docker ps
- sudo docker compose up -d
    - It will show up something like:
        ✔ Container cassandra   Running                                           0.0s 
        ✔ Container zookeeper   Running                                           0.0s 
        ✔ Container nimbus      Running                                           0.0s 
        ✔ Container supervisor  Running                                           0.0s 
        Attaching to cassandra, nimbus, supervisor, zookeeper
    - Wait until all containers are up and running



# Run the main.py to insert data into cassandra
- python3 main.py


