--five permissions
--create socket server
nc -lk 9000

data chmod -R 777
-- create task 2 managers
docker compose -f flink-session.yaml up --scale taskmanager=2 -d
--jobmanager using the docker console
flink run /opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar