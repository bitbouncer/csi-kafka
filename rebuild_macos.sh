rm -rf build
mkdir build 
cd build
cmake -DCMAKE_BUILD_TYPE=Release -D__BUILD_EXAMPLES__=1 .. 
make -j8
cd ..

