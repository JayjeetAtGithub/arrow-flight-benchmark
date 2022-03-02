all:
	rm -rf dataset_bench 
	pip install pyarrow==6.0.1
	g++ -O3 -std=c++11 dataset_bench.cc -I/usr/local/lib/python3.8/dist-packages/pyarrow/include -L/usr/local/lib/python3.8/dist-packages/pyarrow -larrow -larrow_dataset -lparquet -o dataset_bench -D_GLIBCXX_USE_CXX11_ABI=0

dataset:
	../common/deploy_data.sh

run_cpp:
	LD_LIBRARY_PATH=/usr/local/lib/python3.8/dist-packages/pyarrow ./dataset_bench

run_python:
	python3 dataset_bench.py