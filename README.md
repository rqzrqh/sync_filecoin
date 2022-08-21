核心设计思想是用chain这样一个数据结构来描述本地的链和远程链在本地的数据，通过对比两者数据差异实现同步。数据库里记录所有分支数据，持久化缓存里记录单分支的数据索引，客户端的请求先查持久化缓存，再查数据库，从而提高响应速度，并实现对外数据的一致性。



注意版本依赖，现在后端是依赖lotus，把整个lotus当成模块进行依赖，为了避免出现兼容问题，lotus的版本和后端代码的各种依赖版本必须一致。

一旦执行了go mod tidy，本地submodule里的lotus版本必须和go.sum里的lotus版本一致。

变更依赖的lotus版本时，submodule和go.sum的变更必须要在同一个commit里提交，从而体现版本变更的一致性！！！



因为项目只依赖lotus的api接口定义、类型定义，不依赖具体的实现，所以不应该遇到编译问题，但是实际操作中依赖的某些lotus模块会依赖ffi导致编译不过，比如github/filecoin-project/lotus/api/client，github/filecoin-project/lotus/cli，这些是因为lotus没有做好的缘故。为了和解决编译问题，把其中一些代码拷贝出来，放在util/client.go，util/util.go，把一些逻辑改为使用本地util下的代码就能解决。



util是从早期的lotus_chainwatch复制过来，基本没变动。

porter参考了lotus_chainwatch，核心处理逻辑没变化，增加了其他一些更好的性能、并行、写库等机制。



compile:

go build



run:

```
./sync_filecoin initdb xxxx
```

more detail see cmd_initdb.go



if init success, run

```
./sync_filecoin porter xxxx
```

more detail see cmd_porter.go