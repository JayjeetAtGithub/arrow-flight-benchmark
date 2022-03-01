all:
	rm -rf dataset_bench 
	g++ -std=c++11 dataset_bench.cc -I/usr/local/lib/python3.8/dist-packages/pyarrow/include -L/usr/local/lib/python3.8/dist-packages/pyarrow -larrow -larrow_dataset -lparquet -o dataset_bench

run_cpp:
	./dataset_bench

run_python:
	python3 dataset_bench.py