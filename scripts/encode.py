import os
import pandas as pd

def clean_csv_file(file_path, encoding='Windows-1252', replacement_char='-'):
    try:
        # Reading CSV file with specified encoding
        df = pd.read_csv(file_path, encoding=encoding)

        # Replacing non-printable characters in all string columns
        df = df.applymap(lambda x: ''.join(replacement_char if ord(c) > 127 else c for c in x) if isinstance(x, str) else x)

        # Saving the cleaned data back to CSV
        df.to_csv(file_path, index=False, encoding=encoding)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

# 定义当前目录路径
current_directory = '/mnt/d/数据库/GPT3_project/src/GPT3/data'  # Replace with your actual directory path

# 定义bird_train_csv文件夹路径
bird_csv_directory = os.path.join(current_directory, 'bird_csv')

# 遍历bird_train_csv文件夹下所有子文件夹
bird_subfolders = [folder for folder in os.listdir(bird_csv_directory) if os.path.isdir(os.path.join(bird_csv_directory, folder))]

# 遍历每个子文件夹的database_description目录
for subfolder in bird_subfolders:
    database_description_path = os.path.join(bird_csv_directory, subfolder, 'database_description')
    
    # 检查database_description目录是否存在
    if os.path.exists(database_description_path):
        # 处理该目录中的所有CSV文件
        for file in os.listdir(database_description_path):
            if file.endswith('.csv'):
                clean_csv_file(os.path.join(database_description_path, file))

print("处理完成")
