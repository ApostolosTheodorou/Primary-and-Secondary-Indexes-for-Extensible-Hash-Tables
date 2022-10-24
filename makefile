sht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/hash_file.c ./src/sht_file.c ./src/filetable.c -lbf -o ./build/runner1 -O2 -lm

	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main_city.c ./src/hash_file.c ./src/sht_file.c ./src/filetable.c -lbf -o ./build/runner2 -O2 -lm

ht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/sht_file.c ./src/hash_file.c ./src/filetable.c -lbf -o ./build/runner0 -O2 -lm

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c ./src/filetable.c -lbf -o ./build/runner -O2 -lm

clean: 
	rm data.db
	rm data2.db
	rm data3.db