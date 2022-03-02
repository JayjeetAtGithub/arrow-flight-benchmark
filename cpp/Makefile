all:
	apt install -y python3-pip
	rm -rf server 
	rm -rf client
	
	pip install pyarrow==6.0.1
	cp /usr/local/lib/python3.8/dist-packages/pyarrow/libarrow.so.600 /usr/local/lib/python3.8/dist-packages/pyarrow/libarrow.so
	cp /usr/local/lib/python3.8/dist-packages/pyarrow/libarrow_dataset.so.600 /usr/local/lib/python3.8/dist-packages/pyarrow/libarrow_dataset.so
	cp /usr/local/lib/python3.8/dist-packages/pyarrow/libparquet.so.600 /usr/local/lib/python3.8/dist-packages/pyarrow/libparquet.so
	cp /usr/local/lib/python3.8/dist-packages/pyarrow/libarrow_flight.so.600 /usr/local/lib/python3.8/dist-packages/pyarrow/libarrow_flight.so

	g++ -O3 -std=c++11 server.cc -I/usr/local/lib/python3.8/dist-packages/pyarrow/include -L/usr/local/lib/python3.8/dist-packages/pyarrow -larrow -larrow_dataset -larrow_flight -lparquet -o server -D_GLIBCXX_USE_CXX11_ABI=0
	g++ -O3 -std=c++11 client.cc -I/usr/local/lib/python3.8/dist-packages/pyarrow/include -L/usr/local/lib/python3.8/dist-packages/pyarrow -larrow -larrow_dataset -larrow_flight -lparquet -o client -D_GLIBCXX_USE_CXX11_ABI=0

server:
	LD_LIBRARY_PATH=/usr/local/lib/python3.8/dist-packages/pyarrow ./server 10.10.1.12 33005 


client:
	LD_LIBRARY_PATH=/usr/local/lib/python3.8/dist-packages/pyarrow ./client 10.10.1.12 33005 /mnt/data/flight_dataset
