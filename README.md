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

./server [host] [port]
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
curl -o- https://get.docker.com | bash
cd java/
docker build -t bench .

# In server node
./run_server.sh

# In client node
./run_client.sh
```
