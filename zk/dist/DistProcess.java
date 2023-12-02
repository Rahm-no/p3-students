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
import java.nio.charset.StandardCharsets;

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


public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback, AsyncCallback.StatCallback, AsyncCallback.DataCallback
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
private void clearIdleWorkers(String path) {
    try {
        // Get the list of children nodes at the given path
        List<String> children = zk.getChildren(path, false);

        // Recursively delete children nodes and their descendants
        for (String child : children) {
            String childPath = path + "/" + child;

            // Recursive call to delete the child node and its descendants
            clearIdleWorkers(zk,childPath);

            // Delete the current child node
            zk.delete(childPath, -1);
            System.out.println("Deleted node: " + childPath);
        }

        System.out.println("All children nodes under " + path + " deleted successfully.");
    } catch (KeeperException.NoNodeException e) {
        // The node doesn't exist, no further action needed
    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}




	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
		System.out.println("Doing cleanup");
		clearIdleWorkers("/dist30/idleWorkers");
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
		
		if (e.getType() == 	Watcher.Event.EventType.NodeDataChanged && e.getPath().startsWith("/dist30/idleWorkers") && e.getPath().endsWith("/tasks")){
						System.out.println("Watch for data changes under dist30/idleWorkers/tasks");


			zk.getData(e.getPath(), this ,this,null);

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
			zk.getData("/dist30/idleWorkers/worker-" + pinfo +"/tasks", this ,this,null);

        }

        System.out.println("DISTAPP : Role : I will be functioning as " + (isManager ? "manager" : "worker"));
    } catch (Exception e) {
        System.out.println(e);
    }
}

private void assignTaskToIdleWorker(String c) {
    try {
        System.out.println("Assign job to the idle worker");
        System.out.println("c: " + c);

        // Get the list of idle workers
        List<String> idleWorkers = zk.getChildren("/dist30/idleWorkers", true);

        for (String chosenWorker : idleWorkers) {
            // Assign the task to the chosen idle worker
            String taskNodePath = "/dist30/idleWorkers/" + chosenWorker + "/tasks";

            // Check if the task node exists before getting data
            Stat taskNodeStat = zk.exists(taskNodePath, true);
            if (taskNodeStat != null) {
                // Check if the worker already has a task
                List<String> workerTasks = zk.getChildren(taskNodePath, true);

                if (workerTasks.isEmpty()) {
                    zk.setData(taskNodePath, c.getBytes(), -1);
                    System.out.println("Task assigned to idle worker: " + chosenWorker);

                    // Optionally, you may want to remove the assigned task from /dist30/tasks
                    // zk.delete("/dist30/tasks/" + c, -1);

                    // Break out of the loop after assigning one task to one worker
                    break;
                } else {
                    System.out.println("No tasks available to assign to the worker.");
                }
            } else {
                System.out.println("Worker " + chosenWorker + " already has a task.");
            }
        }

        // Handle the case when no idle workers are available
        if (idleWorkers.isEmpty()) {
            System.out.println("No idle workers available to assign the task.");
        }
    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
}



	public void processResult(int rc, String path, Object ctx, List<String> children) {
    try {
        if (path.equals("/dist30/workers")) {
            System.out.println("DISTAPP : Workers list Idle: " + children);
        } 
		
		
		else if (path.equals("/dist30/idleWorkers")) {

            
			System.out.println("DISTAPP : Workers list Idle: " + children);
            
			List<String> waitingTasks = zk.getChildren("/dist30/tasks", false);
            
		if (!waitingTasks.isEmpty()) {
    List<String> idleWorkers = zk.getChildren("/dist30/idleWorkers", true);

    for (String worker : idleWorkers) {
        String taskNodePath = "/dist30/idleWorkers/" + worker + "/tasks";

        // Ensure waitingTasks is not empty before accessing the first element
        if (!waitingTasks.isEmpty()) {
            // Assign waiting tasks to the idle worker
            zk.setData(taskNodePath, waitingTasks.get(0).getBytes(), -1);

            System.out.println("Waiting task assigned to idle worker: " + worker);

            // Remove the assigned task from /dist30/tasks
           // zk.delete("/dist30/tasks/" + waitingTasks.get(0), -1);

            // Break out of the loop to assign only one task to one worker
            break;
        } else {
            System.out.println("No waiting tasks available.");
        }
    }
} else {
    System.out.println("No waiting tasks available.");
}

		
	} else {
            System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
            System.out.println("tasks" + children);
            for (String c : children) {
                assignTaskToIdleWorker(c);
            }
        }
    } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
    }
	
	}


 public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        // Implementation of the processResult method for DataCallback
        if (rc == KeeperException.Code.OK.intValue()) {
            // Data retrieval successful, do something with the data
            System.out.println("Data retrieved successfully: " + new String(data));
			 System.out.println("Data " + new String(data));
			

			  
try{

	String data1 = new String(data, StandardCharsets.UTF_8);
// Remove trailing '/' if present
System.out.println("data1" + data1);
if ( !data1.isEmpty()){
byte[] taskSerial = zk.getData("/dist30/tasks/" + data1, false, null);


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
				zk.create("/dist30/tasks/"+data1+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
}
                } catch (KeeperException | InterruptedException | IOException | ClassNotFoundException e) {
                    System.out.println(e);
                }
        } else {
            // Handle error
            System.err.println("Error retrieving data: " + KeeperException.Code.get(rc));
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



