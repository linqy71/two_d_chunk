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

build lsedb and lse_bind

```
cd lsedb
mkdir build && cd build
cmake ..
make
```

export lse_bind to PYTHONPATH

```
export PYTHONPATH=/path/to/lsedb/build:$PYTHONPATH
```

verify availability

```
python -c 'import lse_bind'
```