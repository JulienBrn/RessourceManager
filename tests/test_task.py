from RessourceManager.task import *
import logging, beautifullogger, time
from RessourceManager.storage import result_metadata_storage, readable_human_writer, task_info_metadata_storage

logger=logging.getLogger()
beautifullogger.setup()

def f(a: pd.DataFrame, b: pd.DataFrame):
    time.sleep(5)
    return pd.concat([a+b, a*b])

init_df = pd.DataFrame([[i, 10*i] for i in range(3)], columns=["x", "y"])

param_dict = dict(a= Task.ParamInfo(
    options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
    reconstruct = lambda d: init_df.copy(), embedded_tasks={}),
    b= Task.ParamInfo(
    options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
    reconstruct = lambda d: init_df.copy()+2, embedded_tasks={})
    
    )
t = Task(
    f=f,
    param_dict=param_dict, 
    log = logging.getLogger("mytasklog"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=False, alternative_paths=[]))

param_dict0 = dict(a= Task.ParamInfo(
    options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
    reconstruct = lambda d: init_df.copy()+3, embedded_tasks={}),
    b= Task.ParamInfo(
    options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
    reconstruct = lambda d: init_df.copy()+4, embedded_tasks={})
    
    )
t0 = Task(
    f=f,
    param_dict=param_dict0, 
    log = logging.getLogger("mytasklog0"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=False, alternative_paths=[]))


param_dict1 = dict(a= Task.ParamInfo(
    options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
    reconstruct = lambda d: d.popitem()[1], embedded_tasks={"t0":t0}),
    b= Task.ParamInfo(
    options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
    reconstruct = lambda d: d.popitem()[1], embedded_tasks={"t":t})
    )

t1 = Task(
    f=f,
    param_dict=param_dict1, 
    log = logging.getLogger("mytasklog1"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=False, alternative_paths=[]))







tasks = {"t":t, "t0": t0, "t1": t1}



async def main():
    print(t, t0, t1)
    # print(f"result {await t.result(executor=default_executor, progress=None)}")
    # await t._run(executor=default_executor, progress=None)
    await t1._run(executor="loop_default", progress=None)
    # print(f"result {await t.result(executor=default_executor, progress=None)}")
    print(pd.concat({n:t.get_history() for n,t in tasks.items()}).reset_index(names=["task", "num"]).drop(columns="num").sort_values("date"))
    # print(pickled_disk_storage.has(t), pickled_disk_storage.get_location(t))
    # print(pd.DataFrame(t.list_history))

if __name__ =="__main__":
    asyncio.run(main())
    print("done")