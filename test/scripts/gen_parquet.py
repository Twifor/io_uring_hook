import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import random
import string

# 设定目标文件大小为 500 MB
target_size_mb = 30

# 假设每行的数据大约占用 200 字节（增加数据类型后的估算）
bytes_per_row = 200
max_rows = 1_000_000  # 最大行数
estimated_rows = (target_size_mb * 1024 * 1024) // bytes_per_row
rows = min(estimated_rows, max_rows)

# 生成一个包含50列 double 类型数据的 DataFrame
column_count = 100
data = {f"col{i}": np.random.randn(rows) for i in range(1, column_count + 1)}

df = pd.DataFrame(data)

# 保存为 Parquet 文件
output_file = "output.parquet"
df.to_parquet(output_file, engine="pyarrow", compression="lz4")

# 检查文件大小
import os

file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
print(f"Generated Parquet file size: {file_size_mb:.2f} MB")
