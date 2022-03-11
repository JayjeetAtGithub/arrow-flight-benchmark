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

# Run the server
make run_server

# Run the client
make run_client
```

## Python

```bash
cd python
pip install -r req.txt

# Run the server
python3 server.py [host] [port]

# Run the client
python3 client.py [host] [port] [dataset path]
```

## Java
```bash
curl -o- https://get.docker.com | bash
cd java/
docker build -t bench .

# Run server
./run_server.sh

# Run client
./run_client.sh
```
