#!/usr/bin/env python3
"""
TCP Client for Text Reverse Service
==============================
Usage:
  python reversetcpclient.py <serverIP> <serverPort> <file> <Lmin> <Lmax> <chunk_seed>

Example:
  python reversetcpclient.py 127.0.0.1 8888 input.txt 50 100 42
"""

# 导入所需的模块
import socket      # 网络套接字编程模块
import struct      # 二进制数据打包和解包模块
import random      # 随机数生成模块
import sys         # 系统相关功能模块
from datetime import datetime  # 日期和时间处理模块

# ---------------------------------------------------------------------------
# 日志系统
# ---------------------------------------------------------------------------
def _log(msg: str) -> None:
    """日志记录函数，同时输出到控制台和 run_log.txt 文件
    参数:
        msg: 要记录的日志消息字符串
    """
    ts = datetime.now().strftime('%H:%M:%S.%f')  # 获取当前时间戳，格式化为 时:分:秒.微秒
    print(f'[{ts}] {msg}')  # 在控制台打印带时间戳的日志消息
    with open('run_log.txt', 'a', encoding='utf-8') as f:  # 以追加模式打开日志文件
        f.write(f'[{ts}] {msg}\n')  # 将带时间戳的日志消息写入文件并换行

# ---------------------------------------------------------------------------
# TCP 辅助函数
# ---------------------------------------------------------------------------
def recv_exactly(sock: socket.socket, size: int) -> bytes:
    """从 TCP 连接中精确读取指定字节数的数据（处理 TCP 流的分片问题）
    
    参数:
        sock: TCP 套接字对象
        size: 需要读取的字节数
    返回:
        包含指定字节数的字节串
    异常:
        ConnectionError: 当服务器断开连接时抛出
    """
    buf = bytearray()  # 创建可变字节数组作为缓冲区
    while len(buf) < size:  # 循环直到缓冲区填满所需字节数
        chunk = sock.recv(size - len(buf))  # 接收剩余需要的字节数
        if not chunk:  # 如果接收到空字节串，说明服务器已断开
            raise ConnectionError('Server disconnected (received 0 bytes)')  # 抛出连接错误异常
        buf.extend(chunk)  # 将接收到的数据片段追加到缓冲区
    return bytes(buf)  # 将字节数组转换为不可变的字节串并返回

# ---------------------------------------------------------------------------
# 文件分块逻辑  ←  注意：这是口试会问到的重点内容
# ---------------------------------------------------------------------------
def chunk_file(content: bytes, lmin: int, lmax: int, seed: int) -> list[bytes]:
    """
    使用种子化伪随机数生成器将内容分割成可变长度的块
    算法步骤
    ---------
    1. 设置随机种子以保证可重现性
    2. 重复抽取 chunk_len ∈ [lmin, lmax]
    3. 当剩余字节 < lmin 时，最后一块取走所有剩余数据（不再随机）
    4. 返回块的列表，N = len(chunks)
    参数:
        content: 要分割的原始字节内容
        lmin: 最小块长度（字节）
        lmax: 最大块长度（字节）
        seed: 随机数种子，保证每次运行结果一致
    返回:
        包含多个字节串的列表，每个字节串是一个数据块
    """
    random.seed(seed)  # 设置随机数种子，确保相同的种子产生相同的随机序列
    chunks: list[bytes] = []  # 创建空列表用于存储分块结果
    pos = 0  # 当前位置指针，从0开始
    total = len(content)  # 获取内容总字节数

    while pos < total:  # 循环直到处理完所有字节
        remaining = total - pos  # 计算剩余未处理的字节数
        if remaining < lmin:  # 如果剩余字节数小于最小块长度
            # 最后一块 - 直接取走剩余部分，不再随机选择长度
            chunk_len = remaining  # 块长度等于剩余字节数
        else:  # 如果剩余字节数足够
            chunk_len = random.randint(lmin, lmax)  # 在[lmin, lmax]范围内随机选择块长度
            if chunk_len > remaining:  # 如果随机长度超过剩余字节数
                chunk_len = remaining  # 调整为剩余字节数，避免越界
        chunks.append(content[pos:pos + chunk_len])  # 切片提取当前块并添加到列表
        pos += chunk_len  # 移动位置指针到下一块的起始位置

    return chunks  # 返回所有分块的列表


# ---------------------------------------------------------------------------
# 程序入口点
# ---------------------------------------------------------------------------
def main() -> None:
    """客户端主函数：读取文件、分块、发送请求、接收响应、保存结果"""
    if len(sys.argv) != 7:  # 检查命令行参数数量是否正确（程序名 + 6个参数）
        print('Usage: python reversetcpclient.py <serverIP> <serverPort> <file> <Lmin> <Lmax> <chunk_seed>')  # 打印使用说明
        print('Example: python reversetcpclient.py 127.0.0.1 8888 input.txt 50 100 42')  # 打印示例命令
        sys.exit(1)  # 以错误状态码1退出程序

    # 解析命令行参数
    server_ip = sys.argv[1]      # 服务器 IP 地址
    server_port = int(sys.argv[2])  # 服务器端口号，转换为整数
    file_path = sys.argv[3]      # 输入文件路径
    lmin = int(sys.argv[4])      # 最小块长度，转换为整数
    lmax = int(sys.argv[5])      # 最大块长度，转换为整数
    seed = int(sys.argv[6])      # 随机种子，转换为整数

    # 清空日志文件（每次运行客户端时创建新的日志文件）
    with open('run_log.txt', 'w', encoding='utf-8') as f:  # 以写入模式打开日志文件（会清空原有内容）
        f.write('')  # 写入空字符串，清空文件

    # ---- 读取输入文件（二进制模式 - 文件是纯 ASCII 编码） ------------------
    with open(file_path, 'rb') as f:  # 以只读二进制模式打开输入文件
        content = f.read()  # 一次性读取整个文件内容到内存
    total_bytes = len(content)  # 获取文件总字节数
    _log(f'Read "{file_path}": {total_bytes} bytes')  # 记录文件读取日志

    # ---- 将文件内容分割成随机大小的块 ---------------------------------
    chunks = chunk_file(content, lmin, lmax, seed)  # 调用分块函数，传入内容和参数
    n = len(chunks)  # 获取分块总数 N

    _log(f'Parameters: Lmin={lmin}, Lmax={lmax}, seed={seed}')  # 记录分块参数日志
    offset = 0  # 初始化偏移量计数器
    for i, chunk in enumerate(chunks):  # 遍历每个块，i 为索引（从0开始），chunk 为块内容
        _log(f'  Chunk {i+1}: offset={offset}, length={len(chunk)} (bytes {offset}–{offset+len(chunk)-1})')  # 记录每个块的详细信息（块号从1开始）
        offset += len(chunk)  # 累加偏移量
    _log(f'Total chunks (N) = {n}')  # 记录总块数日志

    # ---- 连接到服务器 -----------------------------------------------

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 创建 IPv4 TCP 套接字
    sock.settimeout(None)          # 设置为阻塞模式（无限等待，不超时）
    try:
        sock.connect((server_ip, server_port))  # 连接到指定的服务器地址和端口
        _log(f'Connected to {server_ip}:{server_port}')  # 记录连接成功日志
    except Exception as e:  # 捕获连接异常
        _log(f'[ERROR] Connection failed: {e}')  # 记录连接失败日志
        sys.exit(1)  # 以错误状态码退出程序

    try:
        # ---- 1. 发送初始化消息: Type(2B)=1, N(4B) --------------------
        sock.sendall(struct.pack('!HI', 1, n))  # 打包并发送初始化消息：类型1（2字节）+ 块数量N（4字节）
        _log(f'[SEND] Initialization: N={n}')  # 记录发送初始化消息的日志

        # ---- 2. 接收服务器确认: Type(2B)=2 ---------------------------------
        raw = recv_exactly(sock, 2)  # 精确读取2字节的确认消息
        ptype = struct.unpack('!H', raw)[0]  # 解包获取消息类型（无符号短整型）
        if ptype != 2:  # 验证消息类型是否为确认消息(Type=2)
            _log(f'[ERROR] Expected agree(Type=2), got Type={ptype}')  # 记录错误日志
            return  # 如果类型不匹配，直接返回
        _log(f'[RECV] agree')  # 记录收到确认消息的日志

        # ---- 3. 对每个块进行反转请求/响应的交换 ----
        output_parts: list[bytes] = []  # 创建列表用于存储所有反转后的数据块

        for i, chunk in enumerate(chunks):  # 遍历每个待处理的块
            chunk_num = i + 1  # 块号从1开始计数（便于显示）

            # -- 发送反转请求: Type(3) + Length(4) + Data --
            sock.sendall(struct.pack('!HI', 3, len(chunk)) + chunk)  # 打包消息类型3和数据长度，拼接数据后发送
            _log(f'[SEND] reverseRequest chunk={chunk_num}, length={len(chunk)}')  # 记录发送请求的日志

            # -- 接收反转响应: Type(4) + Length(4) + reverseData --
            header = recv_exactly(sock, 6)  # 精确读取6字节的响应头部
            ptype, length = struct.unpack('!HI', header)  # 解包头部：获取消息类型和数据长度
            if ptype != 4:  # 验证消息类型是否为反转响应(Type=4)
                _log(f'[ERROR] Expected reverseAnswer(Type=4), got Type={ptype}')  # 记录错误日志
                break  # 如果类型不匹配，跳出循环

            revdata = recv_exactly(sock, length)  # 根据头部指定的长度精确读取反转后的数据
            revtext = revdata.decode('ascii')  # 将字节串解码为 ASCII 文本字符串
            _log(f'[RECV] reverseAnswer chunk={chunk_num}, length={length}')  # 记录接收响应的日志

            # 按规范要求在第8点输出的控制台显示
            print(f'{chunk_num}: {revtext}')  # 在控制台打印块号和反转后的文本

            output_parts.append(revdata)  # 将反转后的数据块添加到结果列表

        # ---- 写入最终输出文件（倒序写入实现完整反转） ---------------------
        # 原理: 原始文件 = chunk1 + chunk2 + ... + chunkN
        #       全部反转 = rev(chunkN) + ... + rev(chunk2) + rev(chunk1)
        #       因此需要将收集到的反转块倒序写入，才能得到 true full reversal
        out_path = 'output.txt'  # 定义输出文件路径
        with open(out_path, 'wb') as f:  # 以写入二进制模式打开输出文件
            for part in reversed(output_parts):  # 倒序遍历所有反转后的数据块
                f.write(part)  # 将每个数据块写入文件
        total_out = sum(len(p) for p in output_parts)  # 计算输出文件的总字节数
        _log(f'Written "{out_path}": {total_out} bytes')  # 记录文件写入日志

    except ConnectionError as e:  # 捕获连接错误异常
        _log(f'[ERROR] Connection lost: {e}')  # 记录连接丢失日志
    except Exception as e:  # 捕获其他所有异常
        _log(f'[ERROR] {e}')  # 记录错误日志
    finally:  # 无论是否发生异常都会执行的清理代码
        sock.close()  # 关闭套接字连接
        _log('Connection closed')  # 记录连接关闭日志


if __name__ == '__main__':
    main()
