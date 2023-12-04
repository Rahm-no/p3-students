
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

// Be sure to include zookeeper-jute JAR
// for IDE linting to work properly
// See https://www.mail-archive.com/user@zookeeper.apache.org/msg09953.html
// "For 3.5, it was moved out of it, to zookeeper-jute-3.5.5.jar (with all
// the other Jute-generated classes)"

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;

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
public class DistProcess implements Watcher {
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isManager = false;
	boolean initalized = false;

	DistProcess(String zkhost) {
		zkServer = zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
		zk = new ZooKeeper(zkServer, 10000, this); // connect to ZK.
	}

	void initalize() {
		// No Manager in this MVP.
		isManager = false;
		System.out.println("DISTAPP : Role : " + " I will be functioning as " + (isManager ? "manager" : "worker"));

		try {
			startWorking();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// Try to become the manager.
	void runForManager() throws UnknownHostException, KeeperException, InterruptedException {
		// Try to create an ephemeral node to be the manager, put the hostname and pid
		// of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for
		// the execution and no callback is involved..
		zk.create("/distXX/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	private String findAndClaimJob() {
		/*
		 * Try bidding for each jobs. This method does not create any callback or watch.
		 * 
		 * Design note: ZooKeeper prevents /distXX/task_id/lock
		 * from being created if /distXX/task_id has been deleted.
		 * 
		 * Params:
		 * - List<String> jobNodes: list of job nodes.
		 * 
		 * Returns:
		 * - job string if job bid went through.
		 * - otherwise, return null.
		 */
		List<String> jobNames;

		try {
			jobNames = zk.getChildren("/distXX/tasks", false);
		} catch (Exception e) {
			return null;
		}

		for (String jobName : jobNames) {
			String jobPath = String.format("/distXX/tasks/%s", jobName);
			String lockPath = String.format("/distXX/tasks/%s/lock", jobName);

			try {
				zk.create(lockPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				return jobPath;
			} catch (KeeperException e) {
				// Job has already been claimed.
			} catch (InterruptedException e) {
				return null;
			}
		}

		return null;
	}

	private byte[] processTask(String jobPath) {
		/*
		 * Given the path to a job node, returns the serialized result for the job.
		 * 
		 * Params:
		 * - String jobPath: full, absolute path to the job node.
		 * 
		 * Returns:
		 * - byte[], serialized job result ready to be added to /results.
		 */
		byte[] data;
		DistTask task;

		try {
			data = zk.getData(jobPath, false, null);
		} catch (KeeperException e) {
			return null;
		} catch (InterruptedException e) {
			return null;
		}

		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInput ois = new ObjectInputStream(bis);
			task = (DistTask) ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		task.compute();

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(task);
			oos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		byte[] dataUpdated = bos.toByteArray();
		return dataUpdated;
	}

	private void processAndSubmitTask(String jobPath) {
		/*
		 * Process a given task, submit result to ZooKeeper, and atomically create a new
		 * watch.
		 * 
		 * Params:
		 * - String jobPath: path to job.
		 * 
		 */
		byte[] taskDataUpdated = processTask(jobPath);
		String resultPath = String.format("%s/result", jobPath);

		try {
			zk.create(resultPath, taskDataUpdated, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setWatch() {
		/*
		 * Set a watch for new jobs.
		 */
		try {
			zk.getChildren("/distXX/tasks", true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void startWorking() throws InterruptedException {
		// This loop does not busy-wait, but gets blocked until a watch event arrives.
		while (true) {
			// Keep trying until no more job is available.
			String jobPath = findAndClaimJob();
			while (jobPath != null) {
				// Set watch before retrieving the job list, so that updates will not be missed.
				// While the same updates might be processed multiple times, each job will be
				// processed only once. Besides, notice that there is only one thread that
				// actually does the computation,
				setWatch();
				processAndSubmitTask(jobPath);
				jobPath = findAndClaimJob();
			}

			// Block until a new job arrives (a watch event) and unblocks this loop.
			synchronized (this) {
				// We can set as many watch as needed- duplicated ones are okay, too.
				setWatch();
				wait();
			}
		}
	}

	public void process(WatchedEvent event) {
		/*
		 * Design constraints:
		 * - each worker should process only one job at a time.
		 * - job list should be created while atomically setting a new watcher.
		 */

		System.out.println("DISTAPP : Event received : " + event);

		// Manager should be notified if any new znodes are added to tasks.
		if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged && event.getPath().equals("/distXX/tasks")) {
			synchronized (this) {
				// Unblock the worker thread, or not if the worker thread is currently busy,
				// which won't trigger another computation anyway.
				notifyAll();
			}
		}
	}

	public static void main(String args[]) throws Exception {
		// Create a new process
		// Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();
		dt.initalize();

		// Replace this with an approach that will make sure that the process is up and
		// running forever.
		while (true) {

		}
	}
}
