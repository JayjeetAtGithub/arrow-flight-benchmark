all:
	rm -rf dataset_bench 
	g++ -std=c++11 dataset_bench.cc -larrow -larrow_dataset -lparquet -o dataset_bench

run_cpp:
	./dataset_bench

run_python:
	python3 dataset_bench.py