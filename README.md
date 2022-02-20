# Benchmarking

1. Deploy data
```bash
./common/deploy_data.sh
```

## C++

```bash
cd cpp
./setup-env.sh
make

./server [host] 
./client [host] [port] [dataset path]
```

## Python

```bash
cd python
pip install -r req.txt

python3 server.py [host] [port]
python3 client.py [host] [port] [dataset path]
```

## Java

```bash
cd java
./setup-env.sh
./bat.sh

cd target
java -jar server.jar-with-dependencies.jar -h [host] -p [port]
java -jar client.jar-with-dependencies.jar -f [file path] -h [host] -p [port]
