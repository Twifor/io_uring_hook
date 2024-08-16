#!/usr/bin/bash

cmake . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DWITH_GLOG=ON
cmake --build build -j8 --target test uringhook
mv build/test ./