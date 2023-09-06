from RessourceManager.task import *
import logging, beautifullogger, time, concurrent
from RessourceManager.storage import result_metadata_storage, readable_human_writer, task_info_metadata_storage

logger=logging.getLogger()
beautifullogger.setup()

def syracuse(n):
    tot = 17
    for i in range(n):
        tot = tot//2 if tot % 2 ==0 else 3*tot+1
    return tot


def f(a: pd.DataFrame, b: pd.DataFrame, n: int):
    # time.sleep(5)
    return pd.concat([a+b+syracuse(n), a*b])

init_df = pd.DataFrame([[i, 10*i] for i in range(3)], columns=["x", "y"])
n =  11**6

param_dict = dict(
    a= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df.copy(), embedded_tasks={}),
    b= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df.copy()+2, embedded_tasks={}),
    n= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: n, embedded_tasks={})
)
t = Task(
    f=f,
    param_dict=param_dict, 
    log = logging.getLogger("mytasklog"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=False, alternative_paths=[]))

param_dict0 = dict(
    a= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df.copy()+3, embedded_tasks={}),
    b= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df.copy()+4, embedded_tasks={}),
    n= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: n, embedded_tasks={})
)
t0 = Task(
    f=f,
    param_dict=param_dict0, 
    log = logging.getLogger("mytasklog0"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=False, alternative_paths=[]))


param_dict1 = dict(
    a= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: d.popitem()[1], embedded_tasks={"t0":t0}),
    b= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: d.popitem()[1], embedded_tasks={"t":t}),
    n= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: n, embedded_tasks={})
)

t1 = Task(
    f=f,
    param_dict=param_dict1, 
    log = logging.getLogger("mytasklog1"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=False, alternative_paths=[]))

t.used_by.append(t1)
t0.used_by.append(t1)




tasks = {"t":t, "t0": t0, "t1": t1}

processexecutor = concurrent.futures.ProcessPoolExecutor(3)
threadexecutor = concurrent.futures.ThreadPoolExecutor(3)

async def main():
    await t.invalidate()
    print("t Invalidated")
    # hist_df = pd.concat({n:t.get_history() for n,t in tasks.items()}).reset_index(names=["task", "num"]).drop(columns="num").sort_values("date")
    # print(hist_df)
    input()
    await t1.result(executor="sync", progress=None)
    hist_df = pd.concat({n:t.get_history() for n,t in tasks.items()}).reset_index(names=["task", "num"]).drop(columns="num").sort_values("date")
    print(hist_df)
    print(f"Duration: {hist_df['date'].max() - hist_df['date'].min()}")


if __name__ =="__main__":
    asyncio.run(main())
    print("done")