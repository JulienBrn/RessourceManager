from RessourceManager.subtask_extractor import make_basic_subtask_extractor
import sys, asyncio
# print(1)
print("start")
d, reconstruct = make_basic_subtask_extractor(depth=2)({"a": 1, "b": "test", "c": {"1": "e", "2":4, "3":"a"}})
print(d, reconstruct({k:f"R{i}" for i,k in enumerate(d.keys())}))

print("done")
asyncio.get_event_loop().run_forever()
# print(d, reconstruct({k:i for i,k in enumerate(d.keys())}))