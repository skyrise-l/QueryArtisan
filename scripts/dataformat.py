import os
import pandas as pd
from numpy import NaN
import warnings
warnings.filterwarnings('ignore', category=UserWarning)


def detect_data_format(column):
    # 删除NULL值以进行更准确的类型判断
    cleaned_column = column.dropna()

    # 首先检查清洗后的列是否为空，如果为空，则默认为'text'
    if cleaned_column.empty:
        return 'text'

    # 尝试将非NULL值转换为数值
    if column.dtype == "bool":
        return 'text'
    if pd.to_numeric(cleaned_column, errors='coerce').notnull().all():
        # 如果所有非NULL值都可以转换为数值
        # 检查是否所有值都是整数
        if cleaned_column.apply(lambda x: float(x).is_integer()).all():
            return 'integer'
        else:
            return 'double'
    try :
        result = pd.to_datetime(cleaned_column, errors='coerce').notnull().all()
        if result:
            return 'date'
        else:
            return 'text'
    except Exception as e:
        return 'text'
    

def process_folder(folder_path):
    # 遍历文件夹中的CSV文件
    for file in os.listdir(folder_path):
        if file.endswith('.csv') and file != 'database_description':
      
            print(file)
            file_path = os.path.join(folder_path, file)
            
            # 读取CSV文件的数据
            chunk_size = 1000
            data_formats = {}

            with pd.read_csv(file_path, chunksize=chunk_size) as reader:
                for i, chunk in enumerate(reader):
                    # 逐个分块处理数据
                    for column in chunk.columns:
                        # 如果列已经在data_formats中且被识别为'text'，跳过后续检查
                        if column in data_formats and data_formats[column] == 'text':
                            continue
                        
                        format_detected = detect_data_format(chunk[column])
                        if column not in data_formats:
                            data_formats[column] = format_detected
                        elif format_detected == 'text':
                            data_formats[column] = 'text'
                        
                        # 如果已经处理了前两个块，中断循环

            # 更新或创建database_description文件
            desc_path = os.path.join(folder_path, 'database_description', file)
            if os.path.exists(desc_path):
                desc_df = pd.read_csv(desc_path)
                
                for column in desc_df['original_column_name']:
                    if column not in data_formats:
                        print("出现了在表中没有的列")
                        print(column)

                updated_columns = set(desc_df['original_column_name'])

                for column, fmt in data_formats.items():
                    if column in updated_columns:
                        if desc_df.loc[desc_df['original_column_name'] == column, 'data_format'].isna().any():
                            desc_df.loc[desc_df['original_column_name'] == column, 'data_format'] = fmt
                    else:
                        # 添加新的列描述
                        new_row = {col: NaN for col in desc_df.columns}
                        new_row['original_column_name'] = column
                        new_row['data_format'] = fmt
                        print(new_row)
                        #raise Exception("error")
                        new_row_df = pd.DataFrame([new_row])
                        desc_df = pd.concat([desc_df, new_row_df], ignore_index=True)

               # desc_df.to_csv(desc_path, index=False)


def main(target_folder):

    flag = 0
    for folder in os.listdir(target_folder):
        subfolder_path = os.path.join(target_folder, folder)

        if os.path.isdir(subfolder_path):
            process_folder(subfolder_path)

main('/mnt/d/数据库/GPT3_project/src/GPT3/data/bird_csv')
