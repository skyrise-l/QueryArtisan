import logging

class ThreadLogging:
    def __init__(self, log_filename):
        self.logger = logging.getLogger(log_filename)
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            # 创建一个 FileHandler 来输出日志到文件
            file_handler = logging.FileHandler(log_filename)
            file_handler.setLevel(logging.INFO)

            # 创建一个 Formatter 来设置日志格式
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)

            # 将 Handler 添加到 Logger
            self.logger.addHandler(file_handler)

    def log(self, message):
        self.logger.info(message)

    def close_log(self):
        # 关闭所有的处理器
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)
