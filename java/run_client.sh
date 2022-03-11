docker run -it -v $PWD:/workspace -w /workspace -p 5055:5055 bench:latest java -jar /java/target/client.jar-jar-with-dependencies.jar -f /mnt/data/flight_dataset -h 127.0.0.1 -p 5055
