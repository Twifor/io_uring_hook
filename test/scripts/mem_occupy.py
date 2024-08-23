import sys

size_in_gb = 6.5
element_size_in_bytes = 8  # float64 类型的大小
num_elements = int((size_in_gb * 1024**3) / element_size_in_bytes)

large_list = [0.0] * num_elements

print(f"Allocated approximately {size_in_gb} GB of memory.")
input("Press Enter to exit and free memory...")
