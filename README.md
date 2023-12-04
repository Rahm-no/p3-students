# Overview of Client-ZooKeeper-Worker architecture
## Workflow
- Client sends job to ZooKeeper 
    - by creating a new "job" node.
- Client wait for response
    - by creating watcher for a job_id/result node.
- Workers bid for job
    - by trying to create an ephemeral node under the job
- Worker computes result and create job_id/result node.
- Worker enumerate current jobs and try bidding for each job
    - If not found, worker create watcher for new jobs
- Client receives result for job.
- Client deletes job node (optional, but nice to have for performance reasons since the workers need to enumerate all jobs to find an available one.)

## Implementation: Client
### Sending jobs
Sending jobs require creating a sequential node under /jobs.

### Receiving return values
After the client sends a job, it is possible a quick worker finishes the job before the client moves on to the next line of instruction and start listening for results. 
- Ideally, creating sequential node and installing watcher should be "atomic".
- Alternatively, the client should look for result nodes before installing a watcher.

## Implementation: Worker
Note that the workflow of worker nodes is a bit circular. One way to picture this workflow is to start with the task of looking for new jobs.

### Looking for and Requesting jobs
Workers should look for jobs by enumerating nodes under /jobs. 

For each job, the worker should try bidding for the job. 
- There is no need to check if the job is already taken before submitting a bid, since that check is still going to be required while atomically submitting an bidding. 
- However, note that clients might have already deleted the /job/(job_id) node. The bidding process should ensure that /job/(job_id)/bid isn't created if /job/(job_id) has been deleted already.

If a bid goes through, the worker should jump out of the loop (even that might not be necessary, see discussion below) and launch the computation in a blocking way.
- After finishing a job, the worker doesn't necessarily have to restart from the beginning of the job list.
- The bidding process can continue provided that the bidding process handles the case where bidding should not go through if the job has already been deleted.

If none of the bids goes through, the worker should not busy-wait and check again. Instead, the worker should install a watcher for updates /jobs.
- Ideally, the watcher should capture only node creation event at the /jobs level, so as not to be notified for /jobs/(job_id)/result or job deletion.
- However, it is okay to do the check even when not required.

Since the assignment asks that the watcher method should not be blocking, the logic described above should be wrapped in a separate function- e.g., requestJob- and the watcher should fork/invoke that function in a separate thread.


## Race Condition between Requesting jobs and Setting a new watch

After finishing a job, the worker iterates through the list of jobs to find the next one. If none is found, the worker should create a new watch. However, new jobs that are created in between would be missed.

Possible mitigations include:
- Possible option: Implement a global lock shared between all clients and workers
    - The lock ensures that no new job is added when any of the worker is enumerating the job list but hasn't finished adding a new watch.
- Possible option: Atomically retrieve a new list of jobs and set a watch. 
    - If the list of job contains unhandled jobs, process those jobs first.
    - The watcher might triggers a new run while the aforementioned new jobs are between processed. Use a local synchronization mechanism (within the worker) to prevent more than one jobs from being running at a time, and to prevent new watches from being created until all jobs are finished.
        - Specifically, the new threads created from watcher should exit directly if another thread is currently looping through the list of jobs.
        - The job names obtained from atomically setting the watch needs to be sent back to the `processAndSubmitTasks` method. However, after `processAndSubmitTasks` completes, it might need to be invoked again. The versioning system seems to be required to determine if processAndSubmitTasks needs to be invoked again.
- Possible option: After finish setting a watch, leverage the ZooKeeper versioning system to determine if a new job has been added.
    - Still need to deal with the local concurrency safety requirement mentioned in the previous option. 
    - Get version number of job name list. Keep running `processAndSubmitTasks` until there is no version number updates. Any watch events that arrive while the loop is running can be safely ignored.
- Possible option: launch the worker logic in a loop