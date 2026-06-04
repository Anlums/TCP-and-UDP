============================================
  TCP Socket Programming — Text Reverse Service
  计网课程实习 Task1
============================================

一、运行环境
-----------
- 操作系统: Windows 11 / Linux Ubuntu
- Python 版本: Python 3.14+ (需支持 struct, socket, threading 标准库)
- 依赖: 无第三方库（仅使用 Python 标准库）
- 网络: 需要 client 和 server 之间网络互通（同一台机器 127.0.0.1 或跨机器均可）


二、文件说明
-----------
reversetcpserver.py    TCP 服务器端程序
reversetcpclient.py    TCP 客户端程序
input.txt              测试输入文件（英文 ASCII 文本）
output.txt             运行后自动生成，文件的反转结果
run_log.txt            运行后自动生成，收发日志（带微秒时间戳）
readme.txt             本文件


三、使用方法
-----------

1. 启动 Server

   python reversetcpserver.py <port>

   示例:
   python reversetcpserver.py 8888

2. 启动 Client

   python reversetcpclient.py <serverIP> <serverPort> <file> <Lmin> <Lmax> <chunk_seed>

   参数说明:
     serverIP   服务器 IP 地址（如 127.0.0.1 或虚拟机 IP）
     serverPort 服务器端口号（必须与 server 一致）
     file       输入文件路径（如 input.txt）
     Lmin       每块最小长度（字节），如 50
     Lmax       每块最大长度（字节），如 100
     chunk_seed 分块随机种子，保证结果可重现，如 42

   示例:
   python reversetcpclient.py 127.0.0.1 8888 input.txt 50 100 42
   python reversetcpclient.py 192.168.83.128 8888 input.txt 50 100 42


四、自定义协议格式
-----------------
所有报文使用 struct 打包，网络字节序（大端序 Big-Endian）。

  1) Initialization (Type=1, client→server):
     Type(2B) | N(4B)
     N = 文件分块的总块数

  2) agree (Type=2, server→client):
     Type(2B)

  3) reverseRequest (Type=3, client→server):
     Type(2B) | Length(4B) | Data
     Data 是要反转的原始文本块

  4) reverseAnswer (Type=4, server→client):
     Type(2B) | Length(4B) | reverseData
     reverseData 是反转后的文本块


五、分块算法说明（验收重点）
-------------------------
  1. 使用 random.seed(chunk_seed) 设置随机种子
  2. 每次调用 random.randint(Lmin, Lmax) 生成块长度
  3. 累加块长度直到覆盖整个文件
  4. 当剩余字节 < Lmin 时，最后一块取剩余全部（不随机）
  5. 块数 N = 生成的块数量

  口算示例:
    文件 617 字节, Lmin=50, Lmax=100, seed=42
    → 依次生成: 90, 57, 51, 97, 67, 65, 64, 58, 68
    → N = 9, 最后一块长度 = 68

  对应函数: chunk_file() 在 reversetcpclient.py 第 57 行


六、输出说明
-----------
- 控制台: 每收到一个反转块打印 "块号: 反转文本"
- output.txt: 所有反转块拼接后的完整文件
- run_log.txt: 每次收发事件的详细日志（与 Wireshark 时间戳对照）


七、多客户端支持
---------------
Server 使用 threading 多线程，可同时处理多个客户端连接。
在同一 server 上运行多个 client 即可测试。


八、注意事项
-----------
- 输入文件须为纯英文 ASCII 文本
- Lmin 应小于等于 Lmax，且 Lmin > 0
- 同一 seed 保证分块结果完全相同
- run_log.txt 的微秒时间戳可与 Wireshark 抓包结果对照验证

============================================
