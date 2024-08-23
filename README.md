# IOUring Bthread Hook Library

## Usage

Firstly, you should clone or download the codes of [BRPC](https://github.com/apache/brpc) and [liburing](https://github.com/axboe/liburing) from the links. These directories should be renamed as `brpc` and `liburing` respectively.

Using the following command to build `liburinghook.so` and `test`:
```bash
./build.sh
```

For the cases where CSV (instead of parquet) is used, consider deleting `-DUSING_PARQUET=ON` to achieve it.