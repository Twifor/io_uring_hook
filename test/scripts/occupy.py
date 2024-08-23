import sys

# 计算大约需要的元素数量，假设每个浮点数占用8字节
size_in_gb = 6.5
element_size_in_bytes = 8  # float64 类型的大小
num_elements = int((size_in_gb * 1024**3) / element_size_in_bytes)

# 创建一个占用6.5GB内存的列表
large_list = [0.0] * num_elements

# 保持程序运行，以便观察内存占用
print(f"Allocated approximately {size_in_gb} GB of memory.")
input("Press Enter to exit and free memory...")
