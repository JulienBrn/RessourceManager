import asyncio, time, random


class QuickStorage:
    def __init__(self):
        self.d={}

    def non_async_get(self, key):
        return self.d[key]
    async def async_get(self, key):
        return self.d[key]
    
    def add_key(self, key, val):
        self.d[key] = val

nb_keys = 100
nb_requests = 100000000
loop_bloat = 10000

t = QuickStorage()

for i in range(nb_keys):
    t.add_key(f"key {i}", i*i+2)




keys = [f"key {int(random.random()*nb_keys)}" for _ in range(nb_requests)]

def sync_run():
    r = 0
    for k in keys:
        r+=t.non_async_get(k)
    return r

async def async_run():
    r = 0
    for k in keys:
        r+=await t.async_get(k)
    return r



async def async_main(run_async):
    for i in range(loop_bloat):
        asyncio.create_task(asyncio.sleep(1))
    start = time.time()
    if run_async:
        r =await async_run()
    else:
        r = sync_run()
    end = time.time()
    print(f"runasync={run_async} result={r}, time = {end-start}s")

asyncio.run(async_main(False))
asyncio.run(async_main(True))