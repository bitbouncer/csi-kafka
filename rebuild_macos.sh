rm -rf build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. 
make -j8
cd ..

