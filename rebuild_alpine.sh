rm -rf build bin lib
mkdir build
cd build
cmake -DALPINE_LINUX=1 -DCMAKE_BUILD_TYPE=Release -D__BUILD_EXAMPLES__=1 ..
make 
cd ..

