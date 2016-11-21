@ECHO OFF

REM git describe --always

rmdir /S /Q bin\x64
rmdir /S /Q lib\x64
rmdir /S /Q  build

ECHO ===== CMake for 64-bit ======
call "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64
mkdir  build 
cd build

cmake -G "Visual Studio 14 Win64" -DCSI_INCLUDE_PATH=../csi-async -DBoost_INCLUDE_DIRS=../boost -DBoost_LIBRARY_DIRS=../boost/stage/lib -D__BUILD_EXAMPLES__=1 .. 
msbuild ALL_BUILD.vcxproj /p:Configuration=Debug /p:Platform=x64 
msbuild ALL_BUILD.vcxproj /p:Configuration=Release /p:Platform=x64
cd ..


