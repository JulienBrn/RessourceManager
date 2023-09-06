from RessourceManager.task import *
import logging, beautifullogger, time, concurrent
from RessourceManager.storage import result_metadata_storage, readable_human_writer, task_info_metadata_storage

logger=logging.getLogger()
beautifullogger.setup()

def syracuse(n, updater: Updater):
    tot = 17
    updater.total = n
    for i in range(n):
        tot = tot//2 if tot % 2 ==0 else 3*tot+1
        updater.update(1)
        # print(updater.n)
    return tot


def f(a: pd.DataFrame, b: pd.DataFrame, n: int, updater: Updater, desc):
    # time.sleep(5)
    # try:
        updater.set_description(desc)
        res = pd.concat([a+b+syracuse(n, updater), a*b])
        return res
    # except asyncio.CancelledError:
    #     print(f"Cancelling {desc} from within task")
    #     raise
    # except BaseException as e:
    #     logger.exception("Within compute exception", exc_info=e)
    #     raise

init_df = pd.DataFrame([[i, 10*i] for i in range(3)], columns=["x", "y"])
n =  11**8

param_dict = dict(
    a= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df.copy(), embedded_tasks={}),
    b= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df.copy()+2, embedded_tasks={}),
    n= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: n, embedded_tasks={}),
    desc= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: "t", embedded_tasks={})
)
t = Task(
    f=f,
    param_dict=param_dict, 
    log = logging.getLogger("mytasklog"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=True, alternative_paths=[]))

param_dict0 = dict(
    a= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df.copy()+3, embedded_tasks={}),
    b= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df.copy()+4, embedded_tasks={}),
    n= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: n, embedded_tasks={}),
    desc= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: "t0", embedded_tasks={})
)
t0 = Task(
    f=f,
    param_dict=param_dict0, 
    log = logging.getLogger("mytasklog0"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=True, alternative_paths=[]))


param_dict1 = dict(
    a= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: d.popitem()[1], embedded_tasks={"t0":t0}),
    b= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: d.popitem()[1], embedded_tasks={"t":t}),
    n= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: n, embedded_tasks={}),
    desc= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: "t1", embedded_tasks={})
)

t1 = Task(
    f=f,
    param_dict=param_dict1, 
    log = logging.getLogger("mytasklog1"), 
    list_history=[], 
    storage_opt=StorageOptions(additional=[result_metadata_storage, readable_human_writer, task_info_metadata_storage]), 
    func_id = "f", 
    compute_options=ComputeOptions(progress=True, alternative_paths=[]))

t.used_by.append(t1)
t0.used_by.append(t1)




tasks = {"t":t, "t0": t0, "t1": t1}

processexecutor = concurrent.futures.ProcessPoolExecutor(3)
threadexecutor = concurrent.futures.ThreadPoolExecutor(3)

async def main():
    await t.invalidate()
    await t0.invalidate()
    print("t Invalidated")
    # hist_df = pd.concat({n:t.get_history() for n,t in tasks.items()}).reset_index(names=["task", "num"]).drop(columns="num").sort_values("date")
    # print(hist_df)
    myexecutor = processexecutor
    try:
        task = asyncio.get_running_loop().create_task(t1.result(executor=myexecutor, progress=None))
        await asyncio.sleep(2)
        task.cancel()
        await task
    except asyncio.CancelledError:
        print("CANCELLED")
        myexecutor.shutdown(wait=True, cancel_futures=False)
        # print("Shutdown")
    hist_df = pd.concat({n:t.get_history() for n,t in tasks.items()}).reset_index(names=["task", "num"]).drop(columns="num").sort_values("date")
    print(hist_df)
    print(f"Duration: {hist_df['date'].max() - hist_df['date'].min()}")


if __name__ =="__main__":
    asyncio.run(main())
    print("done")