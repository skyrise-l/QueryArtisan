import os
import pandas as pd

def clean_string_columns(df):
    for col in df.columns:
        # 对于每个元素，如果它是字符串，就去除空格
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df

# 定义当前目录路径
current_directory = '/mnt/d/数据库/GPT3_project/src/GPT3/data'  # Replace with your actual directory path

# 定义bird_train_csv文件夹路径
bird_csv_directory = os.path.join(current_directory, 'bird_csv')

# 遍历bird_train_csv文件夹下所有子文件夹
bird_subfolders = [folder for folder in os.listdir(bird_csv_directory) if os.path.isdir(os.path.join(bird_csv_directory, folder))]

chunk_size = 10000  # 设置每个块的大小

# 遍历每个子文件夹的database目录
for subfolder in bird_subfolders:
    database_path = os.path.join(bird_csv_directory, subfolder)
    database_path = os.path.join(database_path, "database_description")
    print(database_path)
    # 检查database目录是否存在
    if os.path.exists(database_path):
        # 处理该目录中的所有CSV文件
        for file in os.listdir(database_path):
            print(file)
            if file.endswith('.csv'):
                file_path = os.path.join(database_path, file)
                temp_file_path = file_path + ".tmp"  # 创建一个临时文件路径

                with pd.read_csv(file_path, chunksize=chunk_size) as reader:
                    for i, chunk in enumerate(reader):
                        # 清理字符串列
                        chunk = clean_string_columns(chunk)

                        # 保存处理后的块到临时文件
                        mode = 'w' if i == 0 else 'a'  # 如果是第一块，则写入模式，否则追加模式
                        header = True if i == 0 else False  # 只对第一块写入头部
                        chunk.to_csv(temp_file_path, mode=mode, header=header, index=False)

                # 处理完所有块后，用新文件替换旧文件
                os.replace(temp_file_path, file_path)

print("处理完成")