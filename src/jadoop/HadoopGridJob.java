// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is part of Jadoop
// Copyright (c) 2016 Grant Braught. All rights reserved.
// 
// Jadoop is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published
// by the Free Software Foundation, either version 3 of the License,
// or (at your option) any later version.
// 
// Jadoop is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public
// License along with Jadoop.
// If not, see <http://www.gnu.org/licenses/>.
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

package jadoop;

import jadoop.util.SingleRecordSplitSequenceFileInputFormat;
import jadoop.util.TextArrayWritable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import javax.management.openmbean.KeyAlreadyExistsException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * A a collection of HadoopGridTasks to be run on a Hadoop Cluster.
 * 
 * A HadoopGridJob has several flags indicating its current state:
 * <UL>
 * <LI>started: true if the task in the job have been started on the cluster.
 * <LI>running: true if the tasks in the job are currently running on the
 * cluster.
 * <LI>finished: true if all of the tasks in the job were started but are no
 * longer running.
 * <LI>terminated: true if one or more tasks in the job not finish on its own
 * (i.e. it timed out or was killed because the job it was a part of was
 * terminated or timed out).
 * <LI>timedout: true if the job was killed because it exceeded the time limit.
 * <LI>successful: true if all of the tasks in the job finished with an exit
 * code of 0.
 * </UL>
 * 
 * @see HadoopGridTask
 * 
 * @author Grant Braught
 * @author Xiang Wei
 * @author Dickinson College
 * @version Jun 9, 2015
 */
public class HadoopGridJob {

	private String jobName;
	private long timeout;

	private Job job;

	private HashMap<String, HadoopGridTask> taskMap;
	private ArrayList<File> files;
	private ArrayList<File> archives;

	private boolean started;
	private boolean finished;
	private volatile boolean terminated;
	private boolean timedout;
	private boolean successful;

	/*
	 * Some flags used for testing. These control whether particular parts of the
	 * job processing occur or not.
	 */
	private boolean testing;
	private boolean makeTempDir;
	private boolean copyFiles;
	private boolean copyArchives;
	private boolean makeInputDir;
	private boolean writeTasksFile;
	private boolean configureJob;
	private boolean submitJob;

	/*
	 * Wait 3 seconds between each poll of the cluster to determine if the job has
	 * completed.
	 */
	private static final int JOB_MONITOR_DELAY = 3000;

	private Thread jobMonitorThread;

	/**
	 * Construct a new HadoopGridJob with the specified name. A default
	 * org.apache.hadoop.conf.Configuration object will be created and used for the
	 * job by passing it to the 2-arg constructor. The default timeout for a new
	 * HadoopGridJob is 10 minutes.
	 * 
	 * @param name
	 *            the name for the HadoopGridJob. This name will also be the name of
	 *            the job submitted to Hadoop and thus may be useful if observing
	 *            the Hadoop cluster with other tools. This is also the name of the
	 *            temporary working directory on the HDFS, and thus must be a valid
	 *            directory name.
	 * @throws IOException
	 *             propagated from the org.apache.hadoop.conf.Configuration
	 *             constructor.
	 */
	public HadoopGridJob(String name) throws IOException {
		this(name, new Configuration());
	}

	/**
	 * Construct a new HadoopGridJob with the specified name and the provided
	 * org.apache.hadoop.conf.Configuration. The default timeout for a HadoopGridJob
	 * is 10 minutes.
	 * 
	 * The following properties in the Configuration object will be overwritten,
	 * thus any values set in the provided object will be ignored:
	 * <UL>
	 * <LI>number of reduce tasks
	 * <LI>Mapper class
	 * <LI>InputFormat class
	 * <LI>OutputKey class
	 * <LI>OutputValue class
	 * <LI>OutputFormat class
	 * <LI>InputPaths
	 * <LI>OutputPath
	 * </UL>
	 * 
	 * @param name
	 *            the name for the HadoopGridJob. This name will also be the name of
	 *            the job submitted to Hadoop and thus may be useful if observing
	 *            the Hadoop cluster with other tools. This is also the name of the
	 *            temporary working directory on the HDFS, and thus must be a valid
	 *            directory name.
	 * @param config
	 *            a Hadoop Configuration object to be used for submission of the
	 *            job. The provided Configuration object is cloned for use in the
	 *            job. Thus, subsequent changes to the provided Configuration will
	 *            not affect the Configuration being used for the job.
	 * @throws IOException
	 *             propagated from the org.apache.hadoop.conf.Configuration
	 *             constructor used to clone the one provided.
	 */
	public HadoopGridJob(String name, Configuration config) throws IOException {
		jobName = name;

		/*
		 * NOTE: Job makes its own copy of config, so we use the one from job anytime we
		 * need it rather than keeping a reference to config.
		 */
		job = Job.getInstance(config, jobName);
		job.setNumReduceTasks(0); // only map tasks

		taskMap = new HashMap<String, HadoopGridTask>();
		files = new ArrayList<File>();
		archives = new ArrayList<File>();

		timeout = Long.MAX_VALUE; // effectively no timeout.

		started = false;
		finished = false;
		successful = false;
		terminated = false;
		timedout = false;

		/*
		 * Testing is false so that things run as normal unless this is changed by a
		 * call to one of the private helpers at the bottom of the file. Those are used
		 * by the tests to set the appropriate values to true for what is being tested
		 * by the test.
		 */
		testing = false;
		makeTempDir = false;
		copyFiles = false;
		copyArchives = false;
		makeInputDir = false;
		writeTasksFile = false;
		configureJob = false;
		submitJob = false;

		jobMonitorThread = null;
	}

	/*
	 * NOTE: All of the following accessors could compute their results but instead
	 * rely on fields set during the processing of the results by the JobMonitor
	 * and/or processResults method. That way they are only computed once rather
	 * than on each call.
	 */

	/**
	 * Check to see if this job has been started. A job is started when it is
	 * submitted to the cluster.
	 * 
	 * @return true if this job has been started.
	 */
	public boolean wasStarted() {
		return started;
	}

	/**
	 * Check to see if this job has finished. A job has finished if all of the tasks
	 * contained in the job have finished (completed, failed, terminated or timed
	 * out).
	 * 
	 * @return true if this job has finished.
	 */
	public boolean hasFinished() {
		return finished;
	}

	/**
	 * Check to see if this job is currently running. A job is considered running if
	 * it has been started but has not yet finished.
	 * 
	 * @return true if this job is running, false if not
	 */
	public boolean isRunning() {
		return started && !finished;
	}

	/**
	 * Check to see if this job was terminated (via a call to the terminate method).
	 * 
	 * @return true if this job was terminated.
	 */
	public boolean wasTerminated() {
		return terminated;
	}

	/**
	 * Check to see if this job timed out. A job that has timed out will also be
	 * marked as terminated.
	 * 
	 * @return true if this job timed out.
	 */
	public boolean hasTimedout() {
		return timedout;
	}

	/**
	 * Check if all of the tasks in this job have completed successfully (i.e. they
	 * are finished and the task command gave an exit value of 0.)
	 * 
	 * @return true if all tasks have completed successfully, false if not.
	 */
	public boolean wasSuccessful() {
		return successful;
	}

	/**
	 * Get a clone of the configuration being used for this HadoopGridJob.
	 * 
	 * @return a clone of this job's org.apache.hadoop.conf.Configuration object.
	 */
	public Configuration getConfiguration() {
		Configuration cloneConf = new Configuration(job.getConfiguration());
		return cloneConf;
	}

	/**
	 * Get the org.apache.hadoop.mapreduce.Job that this HadoopGridJob is using to
	 * interact with hadoop.
	 * 
	 * @return the Job.
	 */

	public Job getJob() {
		return job;
	}

	/**
	 * Add a HadoopGridTask to this HadoopGridJob. The task will be retrievable by
	 * the key specified in the task. This method throws an exception if another
	 * task has already been added with the same key. To replace a task with the
	 * same key the existing task must be removed first and then the new task can be
	 * added.
	 * 
	 * @param task
	 *            the HadoopGridTask to be added to this HadoopGridJob.
	 * 
	 * @throws KeyAlreadyExistsException
	 *             if this HadoopGridJob already contains a task with the same key
	 *             as specified in the given task.
	 * @throws IllegalStateException
	 *             if this HadoopGridJob has already been started.
	 * 
	 * @see #removeTask(String)
	 */
	public void addTask(HadoopGridTask task) {
		if (started) {
			throw new IllegalStateException("Cannot add a task after a job is started.");
		}

		if (taskMap.get(task.getKey()) != null) {
			throw new KeyAlreadyExistsException("There is already a task with the same key (" + task.getKey()
					+ ") as the one you are trying to add to the job.");
		} else {
			taskMap.put(task.getKey(), task);
		}
	}

	/**
	 * Get the number of tasks in the job.
	 * 
	 * @return the number of tasks in the job.
	 */
	public int size() {
		return taskMap.size();
	}

	/**
	 * Get the HadoopGridTask associated with the given key.
	 * 
	 * @param key
	 *            the key of the task to be retrieved.
	 * @return the HadoopGridTask with the given key or null if no such task exists.
	 */
	public HadoopGridTask getTask(String key) {
		return taskMap.get(key);
	}

	/**
	 * Get a list of all of the HadoopGridTasks contained in this HadoopGridJob. If
	 * this HadoopGridJob is complete then each task will contain the results (e.g.
	 * exit value, standard output, standard error) generated by the task.
	 * 
	 * @return a list of the HaddopGridTasks in this HadoopGridJob.
	 */
	public List<HadoopGridTask> getAllTasks() {
		List<HadoopGridTask> lHGT = new ArrayList<HadoopGridTask>();
		for (HadoopGridTask task : taskMap.values()) {
			lHGT.add(task);
		}
		return lHGT;
	}

	/**
	 * Add all of the HadoopGridTasks contained in the List to this HadoopGridJob.
	 * Each task will be retrievable by the key specified in the task object. This
	 * method operates by invoking addTask on each of the individual tasks.
	 * 
	 * @param tasks
	 *            a list of HadoopGridTasks to be added to this HadoopGridJob.
	 * 
	 * @throws KeyAlreadyExistsException
	 *             if this HadoopGridJob already contains a task with the same key
	 *             as that specified in any of the given tasks.
	 * @throws IllegalStateException
	 *             if this HadoopGridJob is currently running or has already been
	 *             completed.
	 * 
	 * @see #addTask(HadoopGridTask)
	 */
	public void addAllTasks(List<HadoopGridTask> tasks) {
		for (HadoopGridTask task : tasks) {
			addTask(task);
		}
	}

	/**
	 * Remove the HadoopGridTask with the specified key from this HadoopGridJob.
	 * 
	 * @param key
	 *            the key of the HadoopGridTask to be removed.
	 * 
	 * @throws IllegalStateException
	 *             if this HadoopGridJob is currently running or has already been
	 *             completed.
	 */
	public void removeTask(String key) {
		if (started) {
			throw new IllegalStateException("Cannot remove a task after a job is started.");
		}

		taskMap.remove(key);
	}

	/**
	 * Set the timeout period for the job. If the job takes longer than this amount
	 * of wall clock time it will be terminated. Terminating the job will terminate
	 * all tasks and no results from any task will be available. Generally it is
	 * preferable to rely on timeouts for the individuals HadoopGridTasks instead so
	 * that results from completed tasks are available. If no timeout is desired,
	 * set this to Long.MAX_VALUE, which is the default value.
	 * 
	 * @param ms
	 *            the task timeout in milliseconds.
	 * 
	 * @throws IllegalStateException
	 *             if this job has already been started.
	 */
	public void setJobTimeout(long ms) {
		if (started) {
			throw new IllegalStateException("Cannot set timeout after the job has been started");
		}

		timeout = ms;
	}

	/**
	 * Get the current timeout period for the job.
	 * 
	 * @return the timeout period for the job in milliseconds.
	 */
	public long getJobTimeout() {
		return timeout;
	}

	/**
	 * Add a file to the list of files that will be available (read-only) to the
	 * HadoopGridTasks in their working directory.
	 * 
	 * When the job is run (i.e. runJob is invoked), the specified file will be
	 * copied to a temporary working directory on the hadoop HDFS and then made
	 * available in the task's working directory via Hadoop's distributed cache.
	 * When the job is complete, the temporary working directory, along with this
	 * file will be deleted from the HDFS.
	 * 
	 * @param dataFile
	 *            a File object referring to the file on the local filesystem.
	 */
	public void addFile(File dataFile) {
		if (started) {
			throw new IllegalStateException("Cannot add a file after the job has been started");
		}

		/*
		 * Just hold the file for now. Later it will be placed into the HDFS so that it
		 * is available to the map task.
		 */
		files.add(dataFile);
	}

	/**
	 * Add an archive (jar file) to the list of archives that will be available
	 * (read-only) to the HadoopGridTasks in their working directory.
	 * 
	 * When the job is run (i.e. when runJob is invoked), a sub-directory with the
	 * same name as the archive will be created within the temporary working
	 * directory on the Hadoop HDFS. The archive will be expanded within that
	 * sub-directory. The sub-directory will be made available in the tasks' working
	 * directory via Hadoop's distributed cache.
	 * 
	 * When the job is complete, the temporary working directory, the sub-directory
	 * for the archive and any files and directories created by expanding the
	 * archive will be deleted.
	 * 
	 * @param archiveFile
	 *            a File object referring to the archive on the local filesystem.
	 * 
	 * @throws IllegalStateException
	 *             if this job is currently running or has already been completed.
	 */
	public void addArchive(File archiveFile) {
		if (started) {
			throw new IllegalStateException("Cannot add an archive after the job has been started");
		}

		/*
		 * Just hold the archive file for now. Later it will be placed into the HDFS so
		 * that it is available to the map task.
		 */
		archives.add(archiveFile);
	}

	/**
	 * Run all of the HadoopGridTasks in this job on the Hadoop cluster. When all of
	 * the tasks have finished execution (completed, failed, terminated or timed
	 * out) the results of the tasks are parsed and placed into the HadoopGridTask
	 * objects. Once all of the results have been processed the job will be marked
	 * as finished (and timedout and/or terminated as appropriate)
	 * 
	 * @param wait
	 *            true to cause this method to wait until all of the tasks have
	 *            completed, failed, terminated or timed out, and all of the results
	 *            have been processed, before this method returns. False to return
	 *            immediately.
	 * 
	 * @throws IllegalStateException
	 *             if this job has already been started
	 * @throws IOException
	 *             if there is a problem running the HadoopGridTasks. This is
	 *             propagated from the org.apache.hadoop.mapreduce.Job.submit
	 *             method.
	 * @throws InterruptedException
	 *             if interrupted while waiting for the HadoopGridTasks to complete.
	 *             This is propagated from the
	 *             org.apache.hadoop.mapreduce.Job.submit method.
	 * @throws ClassNotFoundException
	 *             if a class needed to execute the HadoopGridTasks cannot be found.
	 *             This is propagated from the
	 *             org.apache.hadoop.mapreduce.Job.submit method.
	 * @throws URISyntaxException
	 *             if there is a problem generating the URIs used to add the files
	 *             and archives to the hadoop distributed cache.
	 */
	public void runJob(boolean wait) throws IllegalStateException, IOException, InterruptedException,
			ClassNotFoundException, URISyntaxException {

		if (started) {
			throw new IllegalStateException("Cannot run a job that is already started.");
		}

		/*
		 * When a HadoopGridJob is started, a temporary working directory is created on
		 * the Hadoop HDFS. This directory will be prefaced with the job's name and will
		 * include additional digits to make it unique so that it does not conflict with
		 * any other directory on the HDFS. The file listing all of the tasks to be run
		 * will be written to this directory. Any files or archives that have been added
		 * to the job used by are also moved to this directory. From there files and
		 * archives being used are added to the job's hadoop distribued cache. Upon
		 * completion of the HadoopGridJob, this working directory and all of its
		 * contents are removed from the HDFS.
		 */

		FileSystem fs = FileSystem.get(job.getConfiguration());

		/*
		 * Mark the job and all of the tasks as started. Their completion status is
		 * taken care of in the processResults method.
		 */
		started = true;
		for (HadoopGridTask hgt : taskMap.values()) {
			hgt.markAsStarted();
		}

		/*
		 * NOTE: Flag checks below are to facilitate incremental focused testing of each
		 * part of the run process. Flags are set in the test class as appropriate to
		 * each test.
		 */

		Path tempHDFSWorkingDir = null;
		if (!testing || (testing && makeTempDir)) {
			tempHDFSWorkingDir = createTemporaryDirectory(fs);
		}

		if (!testing || (testing && copyFiles)) {
			if (!files.isEmpty()) {
				copyLocalFileToHDFS(fs, tempHDFSWorkingDir);
			}
		}

		if (!testing || (testing && copyArchives)) {
			if (!archives.isEmpty()) {
				copyLocalArchiveToHDFS(fs, tempHDFSWorkingDir);
			}
		}

		Path inputDir = null;
		if (!testing || (testing && makeInputDir)) {
			inputDir = createInputDirectory(fs, tempHDFSWorkingDir);
			FileInputFormat.addInputPath(job, inputDir);
		}

		if (!testing || (testing && writeTasksFile)) {
			writeTasksSequenceFiles(inputDir);
		}

		Path outputDir = null;
		if (!testing || (testing && configureJob)) {
			job.setJarByClass(jadoop.HadoopGridTaskRunner.class);
			job.setMapperClass(jadoop.HadoopGridTaskRunner.class);
			job.setInputFormatClass(SingleRecordSplitSequenceFileInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MapWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			outputDir = new Path(tempHDFSWorkingDir.toString() + "/output");
			FileOutputFormat.setOutputPath(job, outputDir);
		}

		if (!testing || (testing && submitJob)) {
			job.submit();

			jobMonitorThread = (new Thread(new JobMonitor(fs, outputDir, tempHDFSWorkingDir)));
			jobMonitorThread.start();

			if (wait) {
				jobMonitorThread.join();
			}
		}
	}

	/**
	 * Terminates any currently running tasks in this job. This method will block
	 * until all of the tasks in the job have been terminated. If the job is not
	 * currently running this method has no effect.
	 */
	public void terminate() {
		if (isRunning()) {
			/*
			 * NOTE: We don't do the termination here. Just set a flag and let the
			 * JobMonitor thread detect it and then do the termination there.
			 */
			terminated = true;
			try {
				jobMonitorThread.join();
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
	}

	/**
	 * Get the status information from the underlying Hadoop Job that is running the
	 * HadoopGridTasks.
	 * 
	 * @return the JobStatus for the Haddop Job.
	 * 
	 * @throws IOException
	 *             if there is a problem getting the status of the Hadoop Job. This
	 *             is propagated from the org.apache.hadoop.mapreduce.Job.getStatus
	 *             method.
	 * @throws InterruptedException
	 *             if interrupted while waiting for the HadoopGridTasks to complete.
	 *             This is propagated from the
	 *             org.apache.hadoop.mapreduce.Job.getStatus method.
	 */
	public JobStatus getStatus() throws IOException, InterruptedException {
		return job.getStatus();
	}

	/**
	 * Print periodic messages regarding the status of the running Hadoop job. This
	 * method will monitor the jobs and print information about completed, failed
	 * and killed tasks. It returns when all tasks have been completed, failed or
	 * killed.
	 * 
	 * @throws IOException
	 *             if there is a problem getting the status of the Hadoop Job. This
	 *             is propagated from the
	 *             org.apache.hadoop.mapreduce.Job.monitorAndPrintJob method.
	 * @throws InterruptedException
	 *             if interrupted while waiting for the HadoopGridTasks to complete.
	 *             This is propagated from the
	 *             org.apache.hadoop.mapreduce.Job.monitorAndPrintJob method.
	 */
	public void monitorAndPrintJob() throws IOException, InterruptedException {
		job.monitorAndPrintJob();
	}

	/**
	 * Creates a temporary working directory on the hadoop HDFS for the job that
	 * will be running. The name of this temporary directory will be the name given
	 * to the job. If there is an existing directory with the same name as the job's
	 * name, this method generate a new name that will be used so that the temporary
	 * directory does not share a name with another directory on the HDFS.
	 * 
	 * @return the path of the new temporary working directory on the HDFS
	 * 
	 * @throws IOException
	 *             if there is a problem creating the temporary working directory.
	 */
	private Path createTemporaryDirectory(FileSystem fs) throws IOException {
		// path to the HDFS system
		Path hdfsHome = fs.getHomeDirectory();

		// base name of the temporary working directory.
		Path newHDFSDir = new Path("/" + jobName);

		// full path to the temporary working directory on the HDFS.
		Path tempHDFSWorkingDir = Path.mergePaths(hdfsHome, newHDFSDir);

		// append numbers to the job name until there is no conflict...
		int number = 1;
		while (fs.exists(tempHDFSWorkingDir)) {
			Path jobNum = new Path("/" + jobName + number);
			tempHDFSWorkingDir = Path.mergePaths(hdfsHome, jobNum);
			number++;
		}

		// make the directory on the HDFS and return the path to it.
		fs.mkdirs(tempHDFSWorkingDir);
		return tempHDFSWorkingDir;
	}

	/**
	 * Copies the file(s) on the local machine onto the temporary HDFS working
	 * directory and make them available in the hadoop distributed cache so that
	 * they appear in the working directory of the HadoopGridTask(s) when they are
	 * running
	 * 
	 * @param fs
	 *            the hadoop HDFS filesystem
	 * @param hdfsDirectory
	 *            the path to the temporary working directory on the HDFS to which
	 *            the files are to be copied.
	 * @throws IOException
	 *             if there is a problem copying the files to the HDFS or adding
	 *             them to the hadoop distributed cache.
	 * @throws URISyntaxException
	 *             if there is a problem generating the URI used to add the file to
	 *             the hadoop distributed cache.
	 */
	private void copyLocalFileToHDFS(FileSystem fs, Path hdfsDirectory) throws IOException, URISyntaxException {

		for (File localFile : files) {
			// get the path to the file on the local file system.
			Path fileRelativePath = new Path(localFile.getPath());

			/*
			 * copy the file from the local file system to the temporary working directory
			 * on the HDFS.
			 */
			fs.copyFromLocalFile(fileRelativePath, hdfsDirectory);

			/*
			 * Build a URI to the file on the HDFS so we can add it to the working cache.
			 * 
			 * The value before the # gives the name of the file on the HDFS, the value
			 * after the # gives the name that the file will have in the cache (i.e. the
			 * working directory of the tasks).
			 */
			URI uri = new URI(hdfsDirectory + "/" + localFile.getName() + "#" + localFile.getName());

			job.addCacheFile(uri);
		}
	}

	/**
	 * Copies the archive file(s) on the local machine into the temporary working
	 * directory on the hadoop HDFS. And also be makes them available in the
	 * distributed working cache so the HadoopGridTask(s) can access them in their
	 * working directory. Once the archive file(s) have been copied onto the HDFS, a
	 * directory with the archive file(s)'s name will be created and the contents of
	 * archive file(s) will unpacked into that directory
	 * 
	 * @param fs
	 *            the hadoop HDFS file system
	 * @param hdfsDirectory
	 *            path to the temporary working directory on the HDFS to which the
	 *            archives are to be copied.
	 * @throws IOException
	 *             if there is a problem copying the archives to the HDFS or adding
	 *             them to the hadoop distributed cache.
	 * @throws URISyntaxException
	 *             if there is a problem generating the URI used to add the archive
	 *             to the hadoop distributed cache.
	 */
	private void copyLocalArchiveToHDFS(FileSystem fs, Path hdfsDirectory) throws IOException, URISyntaxException {
		for (File localArchive : archives) {
			Path archiveRelativePath = new Path(localArchive.getPath());

			fs.copyFromLocalFile(archiveRelativePath, hdfsDirectory);

			URI uri = new URI(hdfsDirectory + "/" + localArchive.getName() + "#" + localArchive.getName());
			job.addCacheArchive(uri);
		}
	}

	/**
	 * Creates a directory named "input" in the temporary working directory on the
	 * hadoop HDFS.
	 * 
	 * @param fs
	 *            the hadoop HDFS file system
	 * @param hdfsDirectory
	 *            path to the temporary working directory on the HDFS in which the
	 *            input directory is to be created.
	 * 
	 * @return a path to the input directory that was created.
	 * 
	 * @throws IOException
	 *             if there is a problem creating the input directory.
	 */
	private Path createInputDirectory(FileSystem fs, Path hdfsDirectory) throws IOException {
		String IN_DIR = hdfsDirectory.toString() + "/input";
		Path inDir = new Path(IN_DIR);
		fs.mkdirs(inDir);
		return inDir;
	}

	/**
	 * Create a tasks.seq sequence file in the input directory for each task. This
	 * file contains the key and command that defines the map task. The key is the
	 * key that was associated with the task when it was added to the job. The value
	 * is an TextArrayWritable object with the following contents:
	 * <UL>
	 * <LI>true/false - indicating if standard output is to be captured.
	 * <LI>true/false - indicating if standard error is to be captured.
	 * <LI>cmd - the command to be run in the mapper task.
	 * <LI>... - any successive elements contains an argument to the cmd.
	 * </UL>
	 * 
	 * @see HadoopGridTaskRunner
	 * 
	 * @param hdfsInputDir
	 *            the input directory on the HDFS where the tasks.seq file is to be
	 *            created.
	 * 
	 * @throws IOException
	 *             if there is a problem creating the tasks.seq file.
	 */
	private void writeTasksSequenceFiles(Path hdfsInputDir) throws IOException {
		/*
		 * Seems as if we should be able to just write one task file with multiple
		 * key/value pairs in it. However, hadoop did not seem to want to send each
		 * entry to a different node. Rather one node processed many of the tasks. It
		 * seems as if this could be fixed by defining how hadoop is to split up the
		 * sequence file, but we were unable to get that to work. Writing a different
		 * task file for each task is a bit of a hack solution, but it works. Each task
		 * is then run on a different node, as desired.
		 */

		Text mapperKey = new Text();
		TextArrayWritable mapperVal = new TextArrayWritable();

		// for each task in the job...
		int index = 0;
		for (HadoopGridTask hgt : taskMap.values()) {

			Path seqFileInDirPath = new Path(hdfsInputDir.toString() + "/tasks" + index + ".seq");

			SequenceFile.Writer writer = SequenceFile.createWriter(job.getConfiguration(),
					Writer.file(seqFileInDirPath), Writer.keyClass(Text.class),
					Writer.valueClass(TextArrayWritable.class));

			String taskKey = hgt.getKey();
			String[] taskVal = hgt.getCommand();

			// set the key for sequence file entry for this task
			mapperKey.set(taskKey);

			/*
			 * Build an array of Writeable holding the flags that indicate if standard
			 * output/error are to be captured, the timeout and the command and its
			 * arguments.
			 */
			Writable[] vals = new Writable[taskVal.length + 3];

			// put the flags in the array.
			vals[0] = new Text(String.valueOf(hgt.captureStandardOutput()));
			vals[1] = new Text(String.valueOf(hgt.captureStandardError()));
			vals[2] = new Text(String.valueOf(hgt.getTimeout()));

			// put the command and its arguments into the array.
			for (int i = 3; i < taskVal.length + 3; i++) {
				vals[i] = new Text(taskVal[i - 3]);
			}

			/*
			 * Set the value for the sequence file entry for this task to be the array.
			 */
			mapperVal.set(vals);

			writer.append(mapperKey, mapperVal);

			writer.close();

			index++;
		}
	}

	/**
	 * Process the results that were returned by the Hadoop job. Each result will be
	 * a key value pair with the format specified in the HadoopGridTaskRunner class.
	 * The results for each key should be parsed and placed into the HadoopGridTask
	 * object with the same key.
	 * 
	 * @see HadoopGridTaskRunner
	 * 
	 * @throws IOException
	 *             if there is a problem reading the results.
	 */
	private void processResults(FileSystem fs, Path outDir) throws IOException {

		FileStatus[] fileStatus = fs.listStatus(outDir);

		/*
		 * Process the results for all of the tasks that have completed. Any task that
		 * did not complete will be included in any file.
		 */
		for (FileStatus file : fileStatus) {
			String fileName = file.getPath().getName();

			if (fileName.contains("part-m-")) {

				Path filePath = new Path(outDir + "/" + fileName);
				SequenceFile.Reader reader = new SequenceFile.Reader(job.getConfiguration(),
						SequenceFile.Reader.file(filePath));

				Text mapperOutputKey = new Text();
				MapWritable mapperOutputVal = new MapWritable();

				/*
				 * If multiple tasks are sent to the same node then the response file will
				 * contain multiple entries. Be sure to process each one of them.
				 */
				while (reader.next(mapperOutputKey, mapperOutputVal)) {
					// Get the value returned from the HadoopGridTaskRunner.
					byte exitValue = ((ByteWritable) mapperOutputVal.get(new Text("EV"))).get();

					boolean taskTO = ((BooleanWritable) mapperOutputVal.get(new Text("TO"))).get();

					String stdOut = ((Text) mapperOutputVal.get(new Text("SO"))).toString();
					String stdErr = ((Text) mapperOutputVal.get(new Text("SE"))).toString();

					HadoopGridTask task = getTask(mapperOutputKey.toString());

					if (taskTO) {
						task.markAsTimedout();
					} else {
						// change the task's exit value.
						task.markAsFinished(exitValue);
					}

					if (task.captureStandardOutput()) {
						task.setStandardOutput(stdOut);
					}

					if (task.captureStandardError()) {
						task.setStandardError(stdErr);
					}
				}

				reader.close();
			}
		}
	}

	/**
	 * Runnable that will be launched in a thread to monitor the progress of the
	 * tasks and process the results.
	 */
	private class JobMonitor implements Runnable {

		private FileSystem fs;
		private Path outputDir;
		private Path tempWorkingDir;

		public JobMonitor(FileSystem fileSystem, Path outputDirectory, Path tempWorkingDirectory) {
			this.fs = fileSystem;
			outputDir = outputDirectory;
			tempWorkingDir = tempWorkingDirectory;
		}

		/**
		 * This method periodically checks to see if all of the tasks in the job have
		 * completed or have been killed. When all of the tasks are complete, failed or
		 * killed it calls a method that processes the returned key,value pairs and
		 * fills in the fields in the associated HadoopGridTask objects. When all
		 * results have been processed the finished and correct flags are set.
		 */
		@Override
		public void run() {
			long startTime = System.currentTimeMillis();
			long curTime = System.currentTimeMillis();
			long runTime = curTime - startTime;

			try {
				while (!job.isComplete() && !terminated) {
					Thread.sleep(JOB_MONITOR_DELAY);
					curTime = System.currentTimeMillis();
					runTime = curTime - startTime;

					if (runTime >= timeout) {
						timedout = true;
						terminated = true;
					}

				}

				if (terminated) {
					job.killJob(); // blocks until tasks are killed.
				}

				/*
				 * If a job is terminated then we cannot get any results from hadoop because
				 * they are not available on the HDFS, only a _temprorary file exits in the
				 * working directory.
				 */
				if (!terminated) {
					processResults(fs, outputDir);
				}

				// remove the temporary working directory.
				fs.delete(tempWorkingDir, true);
			} catch (Exception e) {
				/*
				 * Don't really want to kill everything... and this cannot be easily caught...
				 * so print it out mark the unfinished tasks as appropriate (in finally) and get
				 * on with it.
				 */
				e.printStackTrace();
			} finally {
				/*
				 * Any tasks not already marked as finished by the processResults method should
				 * be marked as terminated and timedout (if appropriate)
				 */
				for (HadoopGridTask task : taskMap.values()) {
					if (!task.hasFinished()) {
						if (timedout) {
							task.markAsTimedout();
						} else {
							task.markAsTerminated();
						}
					}
				}

				/*
				 * Mark the job as successful if all of the tasks have an exit code of 0. Could
				 * be combined with above loop, but seemed more clear this way.
				 */
				successful = true;
				for (HadoopGridTask hgt : taskMap.values()) {
					successful = successful && (hgt.wasSuccessful());
				}

				/*
				 * Do this only at the end so that we can be sure that all of the results have
				 * been processed before a call to hasFinished() will return true - i.e. if
				 * runJob was not asked to wait and the user code is polling.
				 */
				finished = true;
			}
		}
	}

	/*
	 * There are a number of private methods below here that are used by the tests.
	 * The tests invoke them via reflection so that they can remain private.
	 */

	/**
	 * Gets all of the files that are made available to the HadoopGridTasks. For
	 * testing purposes.
	 * 
	 * @return a list of files that were made available to the HadoopGridTasks.
	 */
	@SuppressWarnings("unused")
	private List<File> getFiles() {
		return files;
	}

	/**
	 * Retrieves all of the archive files that are made available for the
	 * HadoopGridTasks. For testing purposes.
	 * 
	 * @return a list of archive files that were made available for the
	 *         HadoopGridTasks.
	 */
	@SuppressWarnings("unused")
	private List<File> getArchives() {
		return archives;
	}

	/**
	 * Marks the job as started. For testing purposes.
	 */
	@SuppressWarnings("unused")
	private void markAsStarted() {
		testing = true;
		started = true;
	}

	/**
	 * Marks the job the job as finished. For testing purposes.
	 */
	@SuppressWarnings("unused")
	private void markAsFinished() {
		testing = true;
		finished = true;
	}

	/**
	 * Marks the job the job as terminated. For testing purposes.
	 */
	@SuppressWarnings("unused")
	private void markAsTerminated() {
		testing = true;
		terminated = true;
	}

	/**
	 * Marks the job as timedout. For testing purposes.
	 */
	@SuppressWarnings("unused")
	private void markAsTimedout() {
		testing = true;
		timedout = true;
	}

	/**
	 * Marks the job as successfully completed. For testing purposes.
	 */
	@SuppressWarnings("unused")
	private void markAsSuccessful() {
		testing = true;
		successful = true;
	}

	/**
	 * Mark the makeTempDir variable so that the temporary directory is created. For
	 * testing purposes.
	 */
	@SuppressWarnings("unused")
	private void makeTempDir() {
		testing = true;
		makeTempDir = true;
	}

	/**
	 * Mark the copyFiles variable so that the files are copied to the HDFS. For
	 * testing purposes.
	 */
	@SuppressWarnings("unused")
	private void copyFiles() {
		testing = true;
		makeTempDir = true;
		copyFiles = true;
	}

	/**
	 * Mark the copyFiles variable so that the archives are copied to the HDFS. For
	 * testing purposes.
	 */
	@SuppressWarnings("unused")
	private void copyArchives() {
		testing = true;
		makeTempDir = true;
		copyArchives = true;
	}

	/**
	 * Mark the makeInputDir variable so that the input directory is created on the
	 * HDFS. For testing purposes.
	 */
	@SuppressWarnings("unused")
	private void makeInputDir() {
		testing = true;
		makeTempDir = true;
		makeInputDir = true;
	}

	/**
	 * Mark the writeTasksFile variable so that the tasks sequence file is created
	 * in the input directory. For testing purposes.
	 */
	@SuppressWarnings("unused")
	private void writeTasksFile() {
		testing = true;
		makeTempDir = true;
		makeInputDir = true;
		writeTasksFile = true;
	}

	/**
	 * Mark the configureJob variable so that the job is configured. For testing
	 * purposes.
	 */
	@SuppressWarnings("unused")
	private void configureJob() {
		testing = true;
		makeTempDir = true;
		configureJob = true;
	}

	/**
	 * Mark the submitJob variable so that the job is submitted to the cluster. For
	 * testing purposes.
	 */
	@SuppressWarnings("unused")
	private void submitJob() {
		testing = true;
		makeTempDir = true;
		makeInputDir = true;
		writeTasksFile = true;
		copyFiles = true;
		copyArchives = true;
		configureJob = true;
		submitJob = true;
	}
}
