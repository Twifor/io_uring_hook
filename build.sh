#!/usr/bin/bash

cmake . -B build -DCMAKE_BUILD_TYPE=Release -DWITH_GLOG=ON
cmake --build build -j8
mv build/test ./