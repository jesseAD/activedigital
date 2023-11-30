import concurrent.futures
import time

def sum(x, y):
    time.sleep(60)
    return x + y

executor = concurrent.futures.ThreadPoolExecutor(10)

threads = []

for i in range(5):
    threads.append(executor.submit(sum, i, i))

for thread in concurrent.futures.as_completed(threads):
    print(thread.result())
    

