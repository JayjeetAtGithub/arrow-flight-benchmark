docker run -it -v $PWD:/workspace -v /mnt:/mnt -w /workspace -p 5055:5055 bench:latest java -jar /java/target/server.jar-jar-with-dependencies.jar -h 0.0.0.0 -p 5055
