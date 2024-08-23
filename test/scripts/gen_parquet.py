import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import random
import string

target_size_mb = 30
bytes_per_row = 200
max_rows = 1_000_000
estimated_rows = (target_size_mb * 1024 * 1024) // bytes_per_row
rows = min(estimated_rows, max_rows)

column_count = 100
data = {f"col{i}": np.random.randn(rows) for i in range(1, column_count + 1)}

df = pd.DataFrame(data)

output_file = "file.parquet"
df.to_parquet(output_file, engine="pyarrow", compression="lz4")

import os

file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
print(f"Generated Parquet file size: {file_size_mb:.2f} MB")
