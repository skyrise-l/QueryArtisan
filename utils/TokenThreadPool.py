import threading
import queue

class TokenThreadPool:
    def __init__(self, tokens):
        self.tokens = queue.Queue()
        self.tasks = queue.Queue()
        self.active_tokens = set(tokens)  # 初始化有效 token 集合
        self.lock = threading.Lock()  # 线程同步锁

        for token in tokens:
            self.tokens.put(token)

    def add_task(self, task, args):
        self.tasks.put((task, args))

    def remove_token(self, token):
        with self.lock:  # 确保线程安全
            if token in self.active_tokens:
                self.active_tokens.remove(token)

    def worker(self):
        while True:
            with self.lock:
                if not self.active_tokens:
                    # 如果没有活动的 token，结束线程
                    return

            token = self.tokens.get()
            task, args = self.tasks.get()
            try:
                if token in self.active_tokens:
                    task(token, *args)  # 执行任务
            finally:
                self.tasks.task_done()
                self.tokens.put(token)

    def run(self):
        # 启动线程
        for _ in range(len(self.active_tokens)):
            threading.Thread(target=self.worker, daemon=True).start()

        self.tasks.join()  # 等待所有任务完成

class TokenThreadPool_one:
    def __init__(self, max_threads):
        self.tasks = queue.Queue()
        self.max_threads = max_threads  # 最大线程数

    def add_task(self, task, args):
        self.tasks.put((task, args))

    def worker(self):
        while True:
            task, args = self.tasks.get()
            try:
                task(*args)  # 执行任务，不再需要token
            finally:
                self.tasks.task_done()

    def run(self):
        # 启动指定数量的线程
        for _ in range(self.max_threads):
            threading.Thread(target=self.worker, daemon=True).start()

        self.tasks.join()  # 等待所有任务完成