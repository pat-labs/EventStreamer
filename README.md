-- create task 2 managers
docker compose -f flink-session.yaml up --scale taskmanager=2 -d
--jobmanager console
flink run /opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar
--hostname host.docker.internal --port 9000

--test
SOCKET_HOST=host.docker.internal mvn test
--five permissions
data chmod -R 777
--create socket server
nc -lk 9000