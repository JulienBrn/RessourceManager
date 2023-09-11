from RessourceManager.task import *
import logging, beautifullogger, time, concurrent
from RessourceManager.storage import result_metadata_storage, readable_human_writer, task_info_metadata_storage

logger=logging.getLogger()
beautifullogger.setup()
logging.getLogger("RessourceManager.id_makers").setLevel(logging.ERROR)
# import signal
import time
 
def handler(signum, frame):
    raise KeyboardInterrupt("Interupted by user")
 


def syracuse(n, progress):
    tot = 17
    for i in progress(range(n)):
        tot = tot//2 if tot % 2 ==0 else 3*tot+1
        # print(updater.n)
    return tot


def f(a: pd.DataFrame, b: pd.DataFrame, n: int, desc, progress):
    # time.sleep(5)
    try:
        # if updater is None:
        #     updater = tqdm.tqdm()
        # updater.set_description(desc)
        res = a+b+syracuse(n, progress)
        return res
    # except asyncio.CancelledError:
    #     print(f"Cancelling {desc} from within task")
    #     raise
    except KeyboardInterrupt:
        # logger.warning("Within Task KeyBoard Interrupted")
        raise 
    except asyncio.CancelledError:
        # logger.warning("Within Task Cancelled")
        raise 
    except BaseException as e:
        #  logger.exception("Within compute exception", exc_info=e)
         raise

init_df = 1
# pd.DataFrame([[i, 10*i] for i in range(3)], columns=["x", "y"])
n =  int(0.1*10**8)

param_dict = dict(
    a= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df, embedded_tasks={}),
    b= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df+2, embedded_tasks={}),
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
        reconstruct = lambda d: init_df+3, embedded_tasks={}),
    b= Task.ParamInfo(
        options=TaskParamOptions(dependency="graph", pass_as="value", exception="propagate"),
        reconstruct = lambda d: init_df+4, embedded_tasks={}),
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

import progress_executor


pe = progress_executor.ProcessPoolProgressExecutor()
te = progress_executor.ThreadPoolProgressExecutor()
se = progress_executor.SyncProgressExecutor()
# pd.set_option('display.max_rows', None)
async def main():
    await t.invalidate()
    await t0.invalidate()
    print("t, t0 Invalidated")
    # hist_df = pd.concat({n:t.get_history() for n,t in tasks.items()}).reset_index(names=["task", "num"]).drop(columns="num").sort_values("date")
    # print(hist_df)
    myexecutor = pe
    
    with myexecutor:
        task = asyncio.get_running_loop().create_task(t1.result(executor=myexecutor))
        logger.info("total task created and started running")
        # await asyncio.sleep(2)
        # logger.info("triggering cancel")
        # task.cancel()
        try:
            try:
                # logger.info("Awaiting result")
                await task
            except KeyboardInterrupt:
                pass
                # logger.info("Keyboard interuption, cancelling task")
                # task.cancel()
        except asyncio.CancelledError: pass
            # logger.warning("All CANCELLED")
    hist_df = pd.concat({n:t.get_history() for n,t in tasks.items()}).reset_index(names=["task", "num"]).drop(columns="num").sort_values("date")
    print(hist_df)
    print(f"Duration: {hist_df['date'].max() - hist_df['date'].min()}")


if __name__ =="__main__":
    asyncio.run(main())
    print("done")