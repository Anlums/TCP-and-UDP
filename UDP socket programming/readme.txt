============================================
  UDP Socket Programming — GBN 可靠数据传输
  计网课程实习 Task2
============================================

一、运行环境
-----------
- 操作系统: Windows 11 / Linux Ubuntu
- Python 版本: Python 3.14+ (需支持 struct, socket, threading 标准库)
- 依赖: pandas（用于 RTT 统计计算）
  安装命令: pip install pandas 或 uv pip install pandas
- 网络: 需要 client 和 server 之间网络互通


二、文件说明
-----------
udpserver.py    UDP 服务器端程序（GBN 接收方）
udpclient.py    UDP 客户端程序（GBN 发送方 + 统计汇总）
run_log.txt     运行后自动生成，收发日志（带微秒时间戳）
readme.txt      本文件


三、使用方法
-----------

1. 启动 Server

   python udpserver.py <port>

   示例:
   python udpserver.py 9999

2. 启动 Client

   python udpclient.py <serverIP> <serverPort>

   示例:
   python udpclient.py 127.0.0.1 9999
   python udpclient.py 192.168.83.128 9999


四、自定义协议：SRUDP (Simple Reliable UDP)
------------------------------------------
所有报文使用固定 12 字节头部，网络字节序（大端序 Big-Endian）。

头部格式:
  Offset  Size  Field       说明
  0       2     hdr_tag     协议标识 0x5CA3
  2       1     proto_ver   协议版本 0x01
  3       1     pkt_type    报文类型
  4       2     data_A      类型相关字段A
  6       2     data_B      类型相关字段B
  8       4     time_val    时间戳（Unix 秒）

报文类型:
  0x21  SYN       连接请求      data_A = StudentID
  0x22  SYN_ACK   连接确认      data_A = StudentID, time_val = server_time
  0x31  DAT       数据包        data_A = seq_num, data_B = payload_len
  0x32  DAT_ACK   数据确认      data_A = cumulative_ack, time_val = server_time
  0x41  FIN       结束          (无额外字段)


五、GBN 协议参数
---------------
  发送窗口大小: 400 字节（最大 in-flight payload）
  每包数据大小: 40~80 字节（随机）
  窗口内包数:  5~10 个
  超时时间:    300 ms
  总数据包数:  30 个
  模拟丢包率:  20%（server 端随机不响应）


六、StudentID 验证
------------------
  StudentID = 学号后4位 XOR 0x5A3C
  例: 2103 ^ 0x5A3C = 21003
  Server 验证: 21003 ^ 0x5A3C = 2103 (在 0~9999 范围内，通过)

  本客户端默认学号后4位 = 2103
  如需修改，请更改文件中的 STUDENT_LAST4 常量


七、交互流程
-----------
  Phase 1 — 连接建立:
    Client → Server: SYN (含 StudentID)
    Server → Client: SYN_ACK (验证 StudentID)

  Phase 2 — GBN 数据传输:
    Client 按窗口发送多个 DAT 包
    Server 按序接收，发送累积 ACK（模拟 20% 丢包）
    Client 超时未收到 ACK 则重传整个窗口

  Phase 3 — 结束:
    Client → Server: FIN
    Server → Client: FIN_ACK

  Phase 4 — 统计汇总:
    丢包率、RTT 最大/最小/平均/标准差（pandas 计算）


八、输出说明
-----------
  控制台输出（中文格式）:
    "第 n 个（第 x~y 字节）client端已经发送"
    "第 n 个（第 x~y 字节）server端已经收到，RTT 是 xxx ms"
    "重传第 n 个（第 x~y 字节）数据包"
    汇总表格（丢包率 + RTT 统计）

  run_log.txt: 详细日志（SEND/RECV/TIMEOUT/RETX）


九、配置调整
-----------
  可在 udpclient.py 和 udpserver.py 头部修改:

  LOSS_RATE = 0.20      # server 丢包率 (0.0=无丢包, 1.0=全丢)
  TIMEOUT_SEC = 0.3     # 超时时间（秒）
  TOTAL_PKTS = 30       # 总数据包数
  WIN_MAX_BYTES = 400   # 窗口大小（字节）
  STUDENT_LAST4 = 2103  # 学号后4位


十、注意事项
-----------
- 运行 client 前需先启动 server
- 同一台机器运行时，server 和 client 会共享 run_log.txt
- 跨机器运行时，每台机器有独立的 run_log.txt
- 丢包率和 RTT 值会因网络环境和随机因素而不同

============================================
