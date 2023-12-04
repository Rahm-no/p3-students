/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2023, Bettina Kemme
*/
import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

// threading 
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// random imports
import java.nio.charset.StandardCharsets;

// TODO
// Replace XX with your group number.
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
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback
{
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isManager=false;
	boolean initalized=false;
    int highestSequenceNumber = -1;
    private ExecutorService executor = Executors.newCachedThreadPool();

	DistProcess(String zkhost)
	{
		zkServer=zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

    // ------------------------ overriding async callbacks ------------------------------------

    AsyncCallback.StringCallback createCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            // This method gets called once create operation completes
            // rc is the result code of the operation
            // path is the path for which this request was issued
            // ctx is the context object passed when the method was called
            // name is the actual path that was created
            String context = (String) ctx; 
            if (context.equals("createManager")) {
                System.out.println("create() processResult: Manager");
            } else if (context.equals("createWorker")) {
                System.out.println("create() processResult: Workers");
            } else if (context.equals("createInitialAssignments")) {
                System.out.println("create() processResult: Assignments");
            } else if (context.equals("createAssignment")) {
                System.out.println("create() processResult: Assignments");
            }
            System.out.println("Create znode status: " + KeeperException.Code.get(rc) + ", path: " + path);
        }
    };

    AsyncCallback.Children2Callback getChildren2Callback = new AsyncCallback.Children2Callback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            // This method gets called once the operation completes
            // rc is the result code of the operation
            // path is the path for which this request was issued
            // ctx is the context object passed when the method was called
            // children is the list of children of the znode
            // stat is the Stat object containing metadata about the znode
            System.out.println("context"); 
    
            if (path.equals("/dist30/tasks")) {
                System.out.println("getChildren processResult: tasks"); 

                // check if new node got added 
                int currentHighestSequenceNumber = children.stream()
                    .mapToInt(child -> {
                        // Assuming the child nodes follow the pattern 'node-name0000000001'
                        String sequencePart = child.substring(child.lastIndexOf('-') + 1);
                        return Integer.parseInt(sequencePart);
                    })
                    .max()
                    .orElse(-1);

                if (currentHighestSequenceNumber > highestSequenceNumber) {
                    System.out.println("New child(ren) added.");
                    highestSequenceNumber = currentHighestSequenceNumber;
                    String formattedSequenceNumber = String.format("%010d", highestSequenceNumber);
                    String taskId = "/dist30/tasks/task-" + formattedSequenceNumber; 
                    
                    try {
                        List<String> assignmentsChildren = zk.getChildren("/dist30/assignments", true); 

                        System.out.println("If idle worker is available in /dist30/assignments, then assign task");
                        if (!assignmentsChildren.isEmpty()) {

                            String firstChildPath = "/dist30/assignments/" + assignmentsChildren.get(0);
                            zk.setData(firstChildPath, taskId.getBytes(), -1, setDataCallback, "setTaskToWorker"); 
                        }
                    } catch (Exception ex) {
                        System.out.println("Exception occured");
                    }
                } else {
                    System.out.println("Child(ren) deleted.");
                }
                
            } else if (path.equals("/dist30/workers")) {
                System.out.println("getChildren processResult: workers"); 
                

            } else if (path.equals("/dist30/assignments")) {
                System.out.println("getChildren processResult: assignments"); 
                
                // TODO: Handle situation where there's excess of tasks but not enough 
                // idle workers

            //     // check if there are tasks that haven't been processed 
            //     try {
            //         List<String> unassignedWorkers = zk.getChildren("/dist30/assignments", false);
            //         if (unassignedWorkers.isEmpty()) {
            //             return; 
            //         }
            //         List<String> tasks = zk.getChildren("/dist30/tasks", false);
            //         System.out.println(tasks.size());  
                    
            //         System.out.println("Iterate through list of tasks and check for unfulfilled tasks"); 
            //         for (String task : tasks) {

                        
            //             String taskId = "/dist30/tasks/" + task; 
            //             String resultPath = "/dist30/tasks/" + task + "/result"; 
            //             System.out.println("Check if child node exists");
            //             Stat resultNode = zk.exists(resultPath, false); 
                        
            //             if (resultNode == null) {
                            
            //                 System.out.println("Found unassigned task");
            //                 unassignedWorkers = zk.getChildren("/dist30/assignments", false);
            //                 String unassignedWorkerPath = "/dist30/assignments/" + unassignedWorkers.get(0); 
            //                 zk.setData(unassignedWorkerPath, taskId.getBytes(), -1, setDataCallback, "setTaskToWorker"); 
                            
            //             }
            //         }
            //     } catch (Exception ex) {
            //         System.out.println("Exception occured");
            //     }
            }
    
            // Optionally, you can use the Stat object to get additional information about the znode
            // For example:
            // System.out.println("Znode version: " + stat.getVersion());
        }
    };
    AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            System.out.println("DISTAPP : processResult for exist()");
            try
            {
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    runForManager();	// See if you can become the manager (i.e, no other manager exists)
                    isManager=true;
                    getTasks(); // Install monitoring on any new tasks that will be created.
                    getWorkers();
                    createInitialAssignments(); 
                } else if (rc == KeeperException.Code.OK.intValue()) {
                    isManager=false; 
                    createWorker(); 
                }
                System.out.println("DISTAPP : Role : I will be functioning as " + (isManager ? "manager" : "worker"));
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }; 
    AsyncCallback.StatCallback setDataCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            System.out.println("DISTAPP : processResult for setData()");
        }

    };

    AsyncCallback.DataCallback getDataStatCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {

            if (ctx.equals("getDataAssignmentWorker")) {
                return; 
            }

            System.out.println("DISTAPP : processResult for getData()");
            String taskPath = new String(data, StandardCharsets.UTF_8);
            System.out.println(taskPath);
            executor.submit(() -> {
            try {
                System.out.println("Remove assignment worker node");
                String assignmentsWorkerNode = "/dist30/assignments/worker-" + new String(pinfo.getBytes()); 
                zk.delete(assignmentsWorkerNode, -1);
                byte[] task = zk.getData(taskPath, null, stat); 
                System.out.println("Got the task");

                // Re-construct our task object.
                System.out.println("Deserialize");
				ByteArrayInputStream bis = new ByteArrayInputStream(task);
				ObjectInput in = new ObjectInputStream(bis);
				DistTask dt = (DistTask) in.readObject();


				//Execute the task.
				//TODO: Again, time consuming stuff. Should be done by some other thread and not inside a callback!
                System.out.println("Starting computing");
				dt.compute();
                System.out.println("Finished computing");
				
				// Serialize our Task object back to a byte array!
                System.out.println("Reserialize");
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(dt); oos.flush();
				task = bos.toByteArray();

				// Store it inside the result node.
                System.out.println("Create result node");
				zk.create(taskPath + "/result", task, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                createAssignments();

            } catch (Exception ex) {
                System.out.println("Error occured");
            }
        });
        }

    };
    
    // ------------------------ initializing ------------------------

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
	}

	void initalize() throws UnknownHostException, KeeperException, InterruptedException
	{
        System.out.println("DISTAPP : initialize()");
        zk.exists("/dist30/manager", this, existsCallback, "isManager");
	}

    // ------------------------ helper funcs ------------------------
	// Manager fetching task znodes...
	void getTasks()
	{
        System.out.println("DISTAPP : getTasks()");
		zk.getChildren("/dist30/tasks", this, getChildren2Callback, "getTasks()");  
	}
    
    void getWorkers()
	{
        System.out.println("DISTAPP : getWorkers()");
		zk.getChildren("/dist30/workers", this, getChildren2Callback, "getWorkers()");  
	}

    void getAssignments() {
        System.out.println("DISTAPP : getAssignments()");
		zk.getChildren("/dist30/assignments", this, getChildren2Callback, "getAssignments()");  
    }

	// Try to become the manager.
	void runForManager() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        System.out.println("DISTAPP : runForManager()");
		zk.create("/dist30/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createCallback, "createManager");
	}

    // become the worker.
	void createWorker() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be a worker, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        System.out.println("DISTAPP : becomeWorker()");
        String workerPath = "/dist30/workers/worker-" + new String(pinfo.getBytes()); 
		zk.create(workerPath, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createCallback, "createWorker");
        // zk.setData(workerPath, "IDLE".getBytes(), -1, setDataCallback, "setWorkerData");
        createAssignments(); 
    }

    void createInitialAssignments() throws UnknownHostException, KeeperException, InterruptedException {
        System.out.println("DISTAPP : createInitialAssignments()");
        String assignmentsPath = "/dist30/assignments"; 
		zk.create(assignmentsPath, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createCallback, "createInitialAssignments");
        zk.getChildren(assignmentsPath, this, getChildren2Callback, "getAssignmentsChildren()");
    }

    void createAssignments() throws UnknownHostException, KeeperException, InterruptedException {
        System.out.println("DISTAPP : createAssignments()");
        String workerAssignmentPath = "/dist30/assignments/worker-" + new String(pinfo.getBytes()); 
        zk.create(workerAssignmentPath, "EMPTY".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createCallback, "createAssignment"); 
        zk.getData(workerAssignmentPath, true, getDataStatCallback, "getDataAssignmentWorker"); 
    }

    // ------------------------ other funcs ------------------------
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

		if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
		{
			// Once we are connected, do our intialization stuff.
            System.out.println("DISTAPP : Event received : if statement 1 :" + e);
			if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initalized == false) 
			{
                try {
                    initalize();
                    initalized = true;
                } catch (UnknownHostException exception) {
                } catch (KeeperException exception) {
                } catch (InterruptedException exception) {
                }
			}
		}

		// Manager should be notified if any new znodes are added to tasks.
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist30/tasks"))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
            System.out.println("DISTAPP : Event received : if statement 2 :" + e);
			getTasks();

		} else if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist30/workers")) {
            System.out.println("DISTAPP : Event received : if statement 3 :" + e);
            getWorkers();

        } else if (e.getType() == Watcher.Event.EventType.NodeDataChanged && e.getPath().equals("/dist30/assignments/worker-" + new String(pinfo.getBytes()))) {
            System.out.println("DISTAPP : Event received : if statement 4 :" + e);
            String assignmentsTaskPath = "/dist30/assignments/worker-" + new String(pinfo.getBytes()); 
            zk.getData(assignmentsTaskPath, this, getDataStatCallback, "getTaskFromAssignment");
        }
	}

    // DEAD FUNCTION currently not used 
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
		//		The worker must invoke the "compute" function of the Task sent by the client.
		//      What to do if you do not have a free worker process?
		System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
		for(String c: children)
		{
			System.out.println(c);
			try
			{
				//TODO There is quite a bit of worker specific activities here,
				// that should be moved done by a process function as the worker.

				//TODO!! This is not a good approach, you should get the data using an async version of the API.
				byte[] taskSerial = zk.getData("/dist30/tasks/"+c, false, null);

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

    public static void main(String args[]) throws Exception {
        // Create a new process
        // Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();
    
        // Loop to keep the main thread running indefinitely
        while (true) {
            try {
                // Sleep for a specified amount of time (e.g., 5 seconds)
                Thread.sleep(5000);
    
                // You can add any necessary checks or operations inside this loop
                // For example, checking if the process should still be running, or performing periodic tasks
    
            } catch (InterruptedException e) {
                // Optional: Handle the InterruptedException
                System.out.println("Main thread interrupted, exiting.");
                break;
            }
        }
    }
    
}
