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
	private boolean watcherSet = false;


	DistProcess(String zkhost)
	{
		zkServer=zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
		
	}
private void cleanupPreviousNodes(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
    try {
        Stat stat = zk.exists(path, false);
        if (stat != null) {
            deleteRecursive(zk, path);
        }
    } catch (KeeperException.NoNodeException e) {
        // Node does not exist, nothing to delete
        System.out.println("Node does not exist: " + path);
    }
}

private void deleteRecursive(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
    try {
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            deleteRecursive(zk, path + "/" + child);
        }
        if (!path.equals("/dist30/idleWorkers")) {
            zk.delete(path, -1);
            System.out.println("Deleted node: " + path);
        }
    } catch (KeeperException.NoNodeException e) {
        // Node might have been deleted by another process, handle as needed
        System.out.println("Node does not exist: " + path);
    }
}





	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
		System.out.println("Doing cleanup");
		cleanupPreviousNodes(zk,"/dist30/idleWorkers");
		//zk.create("/dist30/idleWorkers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); // I may change it to pinfo 

		

	}
	


	void initalize() {
	
    
		zk.exists("/dist30/manager", this, this ,null);
		
    }


    void watchWorkers() {
        // Watch for changes in the list of workers
        zk.getChildren("/dist30/workers",this,this,null);

    }

	void watchIdleWorkers(){
		zk.getChildren("/dist30/idleWorkers",this, this,null);
	}

	// Manager fetching task znodes...
	void getTasks()
	{
		zk.getChildren("/dist30/tasks", this, this, null);  
	}

	// Try to become the manager.
	void runForManager() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist30/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}
	private String registerAsWorker() throws KeeperException, InterruptedException {
    // Create a worker znode
		String workerPath = "/dist30/workers/worker-" + pinfo; // Unique identifier for the worker
		zk.create(workerPath, "IDLE".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL); // I may change it to pinfo 
		addToIdleWorkersQueue(pinfo);
		return pinfo;
	
		
		}  

private void addToIdleWorkersQueue(String worker) {
    String idleWorkerPath = "/dist30/idleWorkers/worker-" + worker;
    try {
        // make an ephemeral node for the idle worker
        zk.create(idleWorkerPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
		System.out.println("Worker " + worker + " added to idle queue");
		zk.create(idleWorkerPath + "/tasks", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		

		
       
        
		

	} catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}
private void handleTasksUnderIdleWorkers(String path) {
 
        // Fetch the list of tasks under idleWorkers
        zk.getChildren(path, this, this ,null);

        // Process the tasks or trigger additional logic
        System.out.println("Tasks changed under " + path + ": " );
 

}

public void process(WatchedEvent e) 
	{
		
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
			System.out.println("Manager has a new task");

            
		}
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist30/workers"))
		{
			watchWorkers();

            
		}
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist30/idleWorkers") )
		{
			watchIdleWorkers();

            
		}
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged &&  e.getPath().endsWith("/tasks") && e.getPath().startsWith("/dist30/idleWorkers/worker")){
			System.out.println("BedoreEvent path" + e.getPath());

          handleTasksUnderIdleWorkers(e.getPath());

		}
		
	
		


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
			watchIdleWorkers();
        } else if (rc == KeeperException.Code.OK.intValue()) {

          pinfo = registerAsWorker();
			handleTasksUnderIdleWorkers("/dist30/idleWorkers/worker-" + pinfo +"/tasks");
        }

        System.out.println("DISTAPP : Role : I will be functioning as " + (isManager ? "manager" : "worker"));
    } catch (Exception e) {
        System.out.println(e);
    }
}

	private void assignTaskToIdleWorker(List<String> tasks) {
    try {
		System.out.println("Assign job to the idleworker");
        // Get the list of idle workers
        List<String> idleWorkers = zk.getChildren("/dist30/idleWorkers", false);

        if (!idleWorkers.isEmpty()) {
            // Choose an idle worker (for simplicity, choose the first one)
            String chosenWorker = idleWorkers.get(0);

            // Assign the task to the chosen idle worker
            String taskNodePath = "/dist30/idleWorkers" + "/" + chosenWorker + "/tasks";

            // Check if the task node exists before setting data
            Stat taskNodeStat = zk.exists(taskNodePath, false);
            if (taskNodeStat != null) {
                if (!tasks.isEmpty()) {
                    zk.setData(taskNodePath, tasks.get(0).getBytes(), -1);
                    System.out.println("Task assigned to idle worker: " + chosenWorker);
                } else {
                    System.out.println("No tasks available to assign to the worker.");
                }
            
            }

            
        } else {
            System.out.println("No idle workers available to assign the task.");
        }
    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}


	//Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{
		

		
		 if (path.equals("/dist30/workers")) {
            System.out.println("DISTAPP : Workers list Idle: " + children);
         
        }
		else if (path.equals("/dist30/idleWorkers")) {
            System.out.println("DISTAPP : Workers list Idle: " + children);
         
        }
		else if(path.startsWith("/dist30/idleWorkers") && path.endsWith("/tasks")){
			System.out.println("Tasks added to the idle  worker for processing in a single thread");
			
		//	 try {
          //  zk.delete(path, -1);
        //} catch (KeeperException | InterruptedException e) {
          //  e.printStackTrace();
       // }

		}

	    else{
//here manager will get the task notification so it should add it to the task under " dist30/idleWorkers/workers/tasks( if there is a idle worker )"
       
            System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
			System.out.println("tasks" + children);
			assignTaskToIdleWorker(children);
			
		
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



