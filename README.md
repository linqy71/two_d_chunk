# lsedb

install msgpack first

```
git clone https://gitee.com/lgmcode/msgpack-c.git -b cpp_master
cd msgpack-c
mkdir build
cd build
cmake ..
sudo cmake --build . --target install
```

build

```
cd two_d_chunk
mkdir build && cd build
cmake ..
make
```

export to PYTHONPATH

```
export PYTHONPATH=/path/to/two_d_chunk/build:$PYTHONPATH
```
