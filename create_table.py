from cassandra.cluster import Cluster

if __name__ == "__main__":
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Read the CQL commands from the de_project.cql file
    with open('de_project.cql', 'r') as file:
        cql_commands = file.read().split(';')

    # Execute each CQL command
    for command in cql_commands:
        if command.strip():  # Ignore empty commands
            session.execute(command)

