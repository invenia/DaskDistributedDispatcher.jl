from distributed import Client
client = Client("tcp://127.0.0.1:8786")

client.loop.add_callback(client.scheduler.retire_workers, close_workers=True)
client.loop.add_callback(client.scheduler.terminate)
client.run_on_scheduler(lambda dask_scheduler: dask_scheduler.loop.stop())

# Source:
# https://stackoverflow.com/questions/44021931/stopping-dask-ssh-created-scheduler-from-the-client-interface
