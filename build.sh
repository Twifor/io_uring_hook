#!/usr/bin/bash

if [ ! -d "brpc" ]; then
    echo "Directory brpc does not exist."
    echo "You need to clone or download brpc library."
    exit 1
fi
if [ ! -d "liburing" ]; then
    echo "Directory liburing does not exist."
    echo "You need to clone or download liburing library."
    exit 1
fi

cp event_dispatcher_epoll.cpp brpc/src/brpc/event_dispatcher_epoll.cpp
cp event_dispatcher.h brpc/src/brpc/event_dispatcher.h

cmake . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DWITH_GLOG=ON -DUSING_PARQUET=ON
cmake --build build -j8 --target test uringhook