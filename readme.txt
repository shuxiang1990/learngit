常用方法（util.py）
- op_signal 类处理程序被中断的情况，其中包含一个简易秒钟滴答方法
- exec_shell_local 方法执行本地 shell 命令，返回结果类似 shell 的返回结果
- get_proper_file 方法利用 find 命令返回满足需求的结果

命令行解析工具
- docopt：以文档形式提供，方便给人使用
- click：用装饰器的方式解析命令行参数，代码看上去会比较紧凑
- argparse：不借助第三方工具，直接可以从标准库导入

配置文件解析工具
- ConfigParser

多线程包的使用
- threading

多进程包的使用
- muiltiprocess

连接DB
- mysqldb
- pymysql
