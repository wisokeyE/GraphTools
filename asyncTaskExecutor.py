import asyncio
import inspect
import traceback

# 协程任务执行器
class AsyncTaskExecutor:
    def __init__(self, concurrency, task_func = lambda x: x):
        self.tasks = asyncio.Queue(maxsize=concurrency * 10)
        self.semaphore = asyncio.Semaphore(concurrency)
        self.task_func = task_func
        self.stop_sentinel = object()
        self.stopped = False
        self.workers = [asyncio.create_task(self.worker(i + 1)) for i in range(concurrency)]

    async def worker(self, wid):
        while True:
            task = await self.tasks.get()
            if task is self.stop_sentinel:
                self.tasks.task_done()
                break
            async with self.semaphore:
                try:
                    if inspect.iscoroutinefunction(self.task_func):
                        await self.task_func(task)
                    else:
                        self.task_func(task)
                except Exception as e:
                    print(f"工作协程 {wid} 发生错误: {e}")
                    traceback.print_exc()
                finally:
                    self.tasks.task_done()

    async def add_task(self, task):
        if self.stopped:
            raise RuntimeError("任务执行器已停止，无法添加新任务")
        await self.tasks.put(task)

    async def add_tasks(self, tasks):
        if self.stopped:
            raise RuntimeError("任务执行器已停止，无法添加新任务")
        for task in tasks:
            await self.tasks.put(task)

    async def join(self):
        await self.tasks.join()

    async def shutdown(self):
        if self.stopped:
            await asyncio.gather(*self.workers, return_exceptions=True)
            return
        self.stopped = True
        for _ in self.workers:
            await self.tasks.put(self.stop_sentinel)
        await asyncio.gather(*self.workers, return_exceptions=True)
