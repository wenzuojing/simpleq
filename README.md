# simpleq
golang实现的消息队列服务，支持消费主题(topic)以及消费者分组(group)

目前实现了基本的功能模块，后面的精力主要放在主从复制，数据清除，还有性能优化。

## 使用方式

### 安装

需要go1.2或以上版本
```sh
go get github.com/wenzuojing/simpleq
cd $GOPATH/src/github.com/wenzuojing/simpleq
sh build.sh
```

### 启动

```sh
./simpleq -conf simpleq.conf
```

### 发布消息

```golang

  client, _  := SimpleqClient("localhost", 9090, 1)
  
	client.Publish([]byte("topic1"), []byte("hi wens"))
```

### 消费消息

```golang

  client, _ := SimpleqClient("localhost", 9090, 1)

	msgs, _ := client.Consume([]byte("topic1"), []byte("g2"), 20)

	for _ , msg := range msgs {
		//do something
	}
	
```
