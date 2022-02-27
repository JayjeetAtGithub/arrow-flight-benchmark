all:
	rm -rf dataset_bench 
	g++ -std=c++11 dataset_bench.cc -larrow -larrow_dataset -lparquet -o dataset_bench

run:
	./dataset_bench
