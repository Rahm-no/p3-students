/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2023, Bettina Kemme
*/
import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// TODO
// Replace 30 with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your manager process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your manager's logic and worker's logic.
//		This is important as both the manager and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For simplicity, so far all the code in a single class (including the callbacks).
//		You are free to break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! Managers and Workers are also clients of ZK and the ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		In particular, if the process is a worker, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.


public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback, AsyncCallback.StatCallback
{
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isManager=false;
	boolean initalized=false;

	DistProcess(String zkhost)
	{
		zkServer=zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
		
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
	}

	void initalize() {
    
		zk.exists("/dist30/manager", true, this ,null);
		
    }


    void watchWorkers() {
        // Watch for changes in the list of workers
        zk.getChildren("/dist30/workers", true, this, null);

    }

	// Manager fetching task znodes...
	void getTasks()
	{
		zk.getChildren("/dist30/tasks", true, this, null);  
	}

	// Try to become the manager.
	void runForManager() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist30/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}
	private void registerAsWorker() throws KeeperException, InterruptedException {
    // Create a worker znode
    String workerPath = "/dist30/workers/worker-" + pinfo; // Unique identifier for the worker
    zk.create(workerPath, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    
    // Mark the worker as idle initially
    String statusPath = workerPath + "/status";
    zk.create(statusPath, "IDLE".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    System.out.println("DISTAPP : Created worker status znode: " + statusPath);
}  

private void addToIdleWorkersQueue(String worker) {
    String idleWorkerPath = "/dist30/idleWorkers/worker-" + worker;
    try {
        // make an ephemeral node for the idle worker
        zk.create(idleWorkerPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Worker " + worker + " added to idle queue");
    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}

// private void addToTaskQueue(String task) {
//     String taskQueuePath = "/dist30/tasks/task-" + task;
//     try {
//         zk.create(taskQueuePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//         System.out.println("Task " + task + " added to task queue");
//     } catch (KeeperException | InterruptedException e) {
//         e.printStackTrace();
//     }
// }

// if you know a worker is idle 
private void assignTaskToWorker(String workerPath) {
    try {
        List<String> tasks = zk.getChildren("/dist30/tasks", false);

        // get oldest task first
        String task = tasks.get(0); 
        String taskPath = "/dist30/tasks/task-" + task;
        // get the task data
        byte[] taskData = zk.getData(taskPath, false, null);
        // TODO: assign the task to the worker

        

        // remove task from the task queue
        zk.delete(taskPath, -1); 
        // update worker status to busy
        String workerStatusPath = workerPath + "/status";
        zk.setData(workerStatusPath, "BUSY".getBytes(), -1); 
        //set watcher on worker status
        zk.getData(workerStatusPath, true, null);
    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}

// if you know a task is available 
private void assignWorkerToTask(String taskPath) {
    try {

        // pull a worker from the worker idle queue 
        List<String> idleWorkers = zk.getChildren("/dist30/idleWorkers", false);
        String worker = idleWorkers.get(0); 
        // TODO: assign the worker to the task 


        // remove the worker from the idle queue 
        zk.delete("/dist30/idleWorkers/worker-" + worker, -1); 
        // change status from idle to busy
        String statusPath = "/dist30/workers/worker-" + worker + "/status";
        zk.setData(statusPath, "BUSY".getBytes(), -1);
        // set a watcher on the status 
        zk.getData(statusPath, true, null);

    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}


private void handleWorkerChange(Watcher.Event e) {

    try {

        List<String> idleWorkers = zk.getChildren("/dist30/idleWorkers", false);
        String workerPath = e.getPath(); 
        byte[] workerData = zk.getData(workerPath, false, null);
        String workerId = new String(workerData, StandardCharsets.UTF_8);

        List<String> taskQueue = zk.getChildren("/dist30/tasks", false);

        switch (e.getType()) {
            case NodeCreated:
                System.out.println("Node created at path: " + workerPath);
                // if there's currently no tasks to be assigned 
                // add worker to idle workers queue 
                if (taskQueue.isEmpty) {
                    addToIdleWorkersQueue(workerId); 
                } else {
                    assignTaskToWorker(workerId); 
                }
                break; 
            // if a node was deleted, since the idle worker node created is ephemeral
            // node will be automatically deleted
            // TODO: when will the data of a worker node change?  
            case NodeDataChanged:
                System.out.println("Data changed at path: " + workerPath);
                break;
            // this means the status for a worker node changed
            case NodeChildrenChanged:
                System.out.println("Status for Worker changed at path: " + workerPath);

                String workerStatus = String(zk.getData(workerPath + "/status", false, null)); 

                if (workerStatus.equals("IDLE") && !taskQueue.isEmpty) {
                    assignWorkerToTask(workerId); 
                } else {
                    addToIdleWorkersQueue(workerId); 
                }
                break;
            default:
                // handle other types of events
                break;
        }

    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}


private void handleTaskChange(Watcher.Event e) {
    try {

        List<String> tasksQueue = zk.getChildren("/dist30/tasks", false);
        String taskPath = e.getPath(); 
        byte[] taskData = zk.getData(taskPath, false, null);
        String taskId = new String(taskData, StandardCharsets.UTF_8);


        switch (e.getType()) {
            // new task is created 
            // check if there's any idle watchers 
            // if not, add it to the queue 
            case NodeCreated: 
                if (!idleWorkers.isEmpty) {
                    assignTaskToWorker(taskId)
                }
        }

    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}



public void process(WatchedEvent e) 
	{
		//Get watcher notifications.

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		System.out.println("DISTAPP : Event received : " + e);
		System.out.println("Event path" + e.getPath());

		if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
		{
			// Once we are connected, do our intialization stuff.
			if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initalized == false) 
			{
				initalize();
				initalized = true;

			}
		}

		// Manager should be notified if any new znodes are added to tasks.
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist30/tasks"))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getTasks();

            // if a new task has been created
            // check if there's any idle workers to allocate the job to
            // otherwise add the job to the task queue 
            // remove the job from the task queue when it's allocated to a worker
            handleTaskChange(e); 
		}
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist30/workers"))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			watchWorkers();

            // if a new worker node has been created, & there's no tasks to process
            // add it in the idle queue
            // otherwise assign a task to it 
            // if a worker node has been deleted, check if it's part of the idle queue
            // and remove it 
            handleWorkerChange(e);
		}

	}

void initialize() {
    zk.exists("/dist30/manager", this, this, null);
}

public void processResult(int rc, String path, Object ctx, Stat stat) {
    try {
        if (rc == KeeperException.Code.NONODE.intValue()) {
            // The manager znode does not exist, so this instance becomes the manager.
            runForManager();
            isManager = true;
            System.out.println("watchtasks");
            getTasks();
            System.out.println("watchworkers");
            watchWorkers();
        } else if (rc == KeeperException.Code.OK.intValue()) {
            registerAsWorker();
        }

        System.out.println("DISTAPP : Role : I will be functioning as " + (isManager ? "manager" : "worker"));
    } catch (Exception e) {
        System.out.println(e);
    }
}


	//Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		// This logic is for manager !!
		//Every time a new task znode is created by the client, this will be invoked.

		// TODO: Filter out and go over only the newly created task znodes.
		//		Also have a mechanism to assign these tasks to a "Worker" process.
		//		The worker must invoke the "compute" function of the Task send by the client.
		//What to do if you do not have a free worker process?
        if (path.equals("/dist30/workers")) {
            System.out.println("DISTAPP : Workers changed: " + children);
            // assign task to worker 
            checkAndAssignTask(children); 
        }
	    else{
            System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
            for(String c: children)
            {
                System.out.println(c);
                try
                {
                    //TODO There is quite a bit of worker specific activities here,
                    // that should be moved done by a process function as the worker.

                    //TODO!! This is not a good approach, you should get the data using an async version of the API.
                    byte[] taskSerial = zk.getData("/dist30/tasks/" + c, false, null);

                    // Re-construct our task object.
                    ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
                    ObjectInput in = new ObjectInputStream(bis);
                    DistTask dt = (DistTask) in.readObject();

                    //Execute the task.
                    //TODO: Again, time consuming stuff. Should be done by some other thread and not inside a callback!
                    dt.compute();
                    
                    // Serialize our Task object back to a byte array!
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(dt); oos.flush();
                    taskSerial = bos.toByteArray();

                    // Store it inside the result node.
                    zk.create("/dist30/tasks/"+c+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    //zk.create("/distXX/tasks/"+c+"/result", ("Hello from "+pinfo).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                catch(NodeExistsException nee){System.out.println(nee);}
                catch(KeeperException ke){System.out.println(ke);}
                catch(InterruptedException ie){System.out.println(ie);}
                catch(IOException io){System.out.println(io);}
                catch(ClassNotFoundException cne){System.out.println(cne);}
		    }
	    }
	}
	public static void main(String args[]) throws Exception
	{
		//Create a new process
		//Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		//Replace this with an approach that will make sure that the process is up and running forever.
		Thread.sleep(20000); 
	}
}