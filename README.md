# transform-data

generated using Luminus version "2.9.12.62"

FIXME

打包
lein uberjar  #打包
部署
target/uberjar/transform-data.jar
放到服务器上
运行前必须有dev.edn文件
$> nohup java -Dconf=dev.edn -jar transform-data.jar &
查看日志到  项目运行目录里的 ./log/transform-data.log



## Prerequisites

You will need [Leiningen][1] 2.0 or above installed.

[1]: https://github.com/technomancy/leiningen

## Running

To start a web server for the application, run:

    lein run

## License

Copyright © 2018 FIXME
