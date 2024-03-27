import threading
import queue

class TokenThreadPool:
    def __init__(self, tokens):
        self.tokens = queue.Queue()
        self.tasks = queue.Queue()
        self.active_tokens = set(tokens) 
        self.lock = threading.Lock()  

        for token in tokens:
            self.tokens.put(token)

    def add_task(self, task, args):
        self.tasks.put((task, args))

    def remove_token(self, token):
        with self.lock:  
            if token in self.active_tokens:
                self.active_tokens.remove(token)

    def worker(self):
        while True:
            with self.lock:
                if not self.active_tokens:
                   
                    return

            token = self.tokens.get()
            task, args = self.tasks.get()
            try:
                if token in self.active_tokens:
                    task(token, *args)  
            finally:
                self.tasks.task_done()
                self.tokens.put(token)

    def run(self):
        for _ in range(len(self.active_tokens)):
            threading.Thread(target=self.worker, daemon=True).start()

        self.tasks.join()  

class TokenThreadPool_one:
    def __init__(self, max_threads):
        self.tasks = queue.Queue()
        self.max_threads = max_threads 

    def add_task(self, task, args):
        self.tasks.put((task, args))

    def worker(self):
        while True:
            task, args = self.tasks.get()
            try:
                task(*args)  #
            finally:
                self.tasks.task_done()

    def run(self):
        for _ in range(self.max_threads):
            threading.Thread(target=self.worker, daemon=True).start()

        self.tasks.join() 