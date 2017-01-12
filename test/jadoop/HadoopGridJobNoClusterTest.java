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

import jadoop.HadoopGridJob;
import jadoop.HadoopGridTask;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.management.openmbean.KeyAlreadyExistsException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class HadoopGridJobNoClusterTest extends TestCase {

	private HadoopGridTask hgt;
	private HadoopGridTask hgt2;
	private HadoopGridTask hgt3;
	private HadoopGridJob hgj;
	private Configuration conf;
	private HadoopGridJob hgj2;
	private File file1;
	private File file2;
	private File file3;

	@Before
	protected void setUp() throws Exception {
		hgt = new HadoopGridTask("task1", "java ProgramSample", 1000);
		hgt2 = new HadoopGridTask("task2", "java ProgramExtra", 1000);
		hgt3 = new HadoopGridTask("task3", "cal", 1000);
		
		hgj = new HadoopGridJob("job1");
		conf = new Configuration();
		hgj2 = new HadoopGridJob("job2", conf);
		
		file1 = new File("/a/b/c/file1.txt");
		file2 = new File("/a/b/c/file2.dat");
		file3 = new File("/a/b/c/file3.txt");
	}

	@Test
	public void testFirstConstructor() throws IOException,
			NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		assertNotNull("the configuration should NOT be null",
				hgj.getConfiguration());
		assertEquals("job name was NOT set correctly", "job1", hgj.getJob()
				.getJobName());
		assertEquals("there should be NO reduce tasks", 0, hgj.getJob()
				.getNumReduceTasks());

		assertFalse("the job should NOT be started", hgj.wasStarted());
		assertFalse("the job should NOT be finished", hgj.hasFinished());
		assertFalse("the job should NOT be running", hgj.isRunning());
		assertFalse("the job should NOT be terminated", hgj.wasTerminated());
		assertFalse("the job should NOT be timed out", hgj.hasTimedout());
		assertFalse("the job should NOT be successfully complete",
				hgj.wasSuccessful());

		assertEquals("the job should have the default timeout time", Long.MAX_VALUE,
				hgj.getJobTimeout());
	}

	@Test
	public void testSecondConstructor() throws IOException,
			NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		assertNotNull("the configuration should NOT be null",
				hgj2.getConfiguration());
		assertEquals("job name was NOT set correctly", "job2", hgj2.getJob()
				.getJobName());
		assertEquals("there should be NO reduce tasks", 0, hgj2.getJob()
				.getNumReduceTasks());

		assertFalse("the job should NOT be started", hgj.wasStarted());
		assertFalse("the job should NOT be finished", hgj.hasFinished());
		assertFalse("the job should NOT be running", hgj.isRunning());
		assertFalse("the job should NOT be terminated", hgj.wasTerminated());
		assertFalse("the job should NOT be timed out", hgj.hasTimedout());
		assertFalse("the job should NOT be successfully complete",
				hgj.wasSuccessful());

		assertEquals("the job should have the default timeout time", Long.MAX_VALUE,
				hgj2.getJobTimeout());
	}
	
	@Test
	public void testRunning() throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		markStarted(hgj);
		assertTrue("Should be running after being started.", hgj.isRunning());
		markFinished(hgj);
		assertFalse("Should NOT be running after finished.", hgj.isRunning());
	}

	@Test
	public void testGetConfiguration() {
		assertNotSame("the configurations should NOT be the same", conf,
				hgj2.getConfiguration());
	}

	@Test
	public void testAddTaskAfterStarted() throws IOException,
			NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		assertFalse("the job should NOT be started", hgj.wasStarted());
		markStarted(this.hgj);
		assertTrue("the job should be started", hgj.wasStarted());

		try {
			hgj.addTask(hgt);
			fail("Should not be able to add a task to a started job.");
		} catch (IllegalStateException ise) {
			// passed
		} catch (Exception e) {
			fail("wrong type of exception was thrown");
		}
	}

	@Test
	public void testAddTaskDuplicateKey() throws IOException {

		HadoopGridTask hgt4 = new HadoopGridTask("task1", "java ProgramExtra", 1000);
		assertSame("the two tasks should have the same key", hgt.getKey(),
				hgt4.getKey());
		hgj.addTask(hgt);

		try {
			hgj.addTask(hgt4);
			fail("Should not be able to add two tasks with the same key");
		} catch (KeyAlreadyExistsException kaee) {
			// passed
		} catch (Exception e) {
			fail("wrong type of exception was thrown");
		}
	}

	@Test
	public void testAddTasks() throws IOException {
		hgj.addTask(hgt);
		hgj.addTask(hgt2);

		assertEquals("should be 2 jobs in the task", 2, hgj.size());
		assertSame("should be getting back hgt", hgt, hgj.getTask("task1"));
		assertSame("should be getting back hgt2", hgt2, hgj.getTask("task2"));
	}

	@Test
	public void testGetAllTasks() {
		hgj.addTask(hgt);
		hgj.addTask(hgt2);

		List<HadoopGridTask> lHGT = hgj.getAllTasks();
		assertEquals("Incorrect number of tasks in job", 2, hgj.size());
		assertTrue("tasks does not contain hgt", lHGT.contains(hgt));
		assertTrue("tasks does not contain hgt2", lHGT.contains(hgt2));
	}

	@Test
	public void testAddAllTasks() throws IOException {
		List<HadoopGridTask> lHGT = new ArrayList<HadoopGridTask>();
		lHGT.add(hgt);
		lHGT.add(hgt2);
		lHGT.add(hgt3);

		hgj.addAllTasks(lHGT);

		assertEquals(
				"the job should have the same number of elements as the list",
				3, hgj.size());
	}

	@Test
	public void testAddAllTasksAfterStarted() throws NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		markStarted(this.hgj);

		List<HadoopGridTask> lHGT = new ArrayList<HadoopGridTask>();
		lHGT.add(hgt);
		lHGT.add(hgt2);
		lHGT.add(hgt3);

		try {
			hgj.addAllTasks(lHGT);
			fail("Should not be able to add a task to a started job.");
		} catch (IllegalStateException ise) {
			// passed
		} catch (Exception e) {
			fail("wrong type of exception was thrown");
		}
	}

	@Test
	public void testRemoveTaskAfterStarted() throws IOException,
			NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		hgj.addTask(hgt);
		markStarted(this.hgj);

		try {
			hgj.removeTask(hgt.getKey());
			fail("IllegalStateException was not thrown");
		} catch (IllegalStateException ise) {
			// passed
		} catch (Exception e) {
			fail("wrong type of exception was thrown");
		}
	}

	@Test
	public void testRemoveTaskExists() throws IOException {

		hgj.addTask(hgt);
		hgj.addTask(hgt2);
		hgj.addTask(hgt3);
		hgj.removeTask(hgt.getKey());

		assertNull("task1 should NO LONGER be part of the job",
				hgj.getTask(hgt.getKey()));
		assertEquals("Should be 2 tasks left", 2, hgj.size());
	}

	@Test
	public void testRemoveTaskDoesntExist() throws IOException {

		hgj.addTask(hgt);
		hgj.addTask(hgt2);
		hgj.addTask(hgt3);

		hgj.removeTask("notme");
		assertEquals("Should be 3 tasks left", 3, hgj.size());
	}

	@Test
	public void testSetTimeOutAfterStarted() throws NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		markStarted(hgj);

		try {
			hgj.setJobTimeout(60000);
			fail("Should not be able to set timeout after job has started.");
		} catch (IllegalStateException ise) {
			// passed
		} catch (Exception e) {
			fail("wrong type of exception was thrown");
		}
	}

	@Test
	public void testSetTimeOut() {
		hgj.setJobTimeout(120000);
		assertEquals(
				"the timeout time should be the number of milliseconds set",
				120000, hgj.getJobTimeout());
	}

	@Test
	public void testAddFileAlreadyStarted() throws NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		markStarted(this.hgj);

		try {
			hgj.addFile(file1);
			fail("Should not be able to add a file to a job that is already started.");
		} catch (IllegalStateException ise) {
			// passed
		} catch (Exception e) {
			fail("wrong type of exception was thrown");
		}
	}

	@Test
	public void testAddFile() throws IOException, NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		hgj.addFile(file1);
		hgj.addFile(file2);
		hgj.addFile(file3);

		List<File> files = getFiles(hgj);

		assertEquals(
				"the list should have the same number of elements as the number of files added",
				3, files.size());
		assertTrue("file1 should be in the list", files.contains(file1));
		assertTrue("file2 should be in the list", files.contains(file2));
		assertTrue("file3 should be in the list", files.contains(file3));
	}

	@Test
	public void testAddArchiveAlreadyStarted() throws NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		markStarted(this.hgj);

		assertTrue("the job should be running", hgj.isRunning());

		try {
			hgj.addArchive(file1);
			fail("IllegalStateException was NOT thrown");
		} catch (IllegalStateException ise) {
			// passed
		} catch (Exception e) {
			fail("wrong type of exception was thrown");
		}
	}

	@Test
	public void testAddArchive() throws IOException, NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		hgj.addArchive(file1);
		hgj.addArchive(file2);
		hgj.addArchive(file3);

		List<File> arcFiles = getArchives(hgj);
		
		assertEquals(
				"the list should have the same number of elements as the number of files added",
				3, arcFiles.size());
		assertTrue("myFile should be in the list", arcFiles.contains(file1));
		assertTrue("screenShot should be in the list", arcFiles.contains(file2));
		assertTrue("dataFile should be in the list", arcFiles.contains(file3));
	}
	
	@Test
	public void testRunJobAfterStarted() throws NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		HadoopGridJobNoClusterTest.markStarted(hgj);

		try {
			hgj.runJob(false);
			fail("Should not be able to run job that is already started.");
		} catch (IllegalStateException e) {
			// pass
		} catch (Exception e) {
			fail("Incorrect exception type.");
		}
	}

	/*
	 * Helper methods that invoke private methods in the HadoopGridJob to
	 * facilitate the testing process.  Package access so they can be used
	 * in the cluster tests as well.
	 */
	static List<File> getFiles(HadoopGridJob hgj)
			throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		Method m = hgj.getClass().getDeclaredMethod("getFiles");
		m.setAccessible(true);
		
		@SuppressWarnings("unchecked")
		List<File> files = (ArrayList<File>) m.invoke(hgj);
		
		return files;
	}

	static List<File> getArchives(HadoopGridJob hgj)
			throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		Method m = hgj.getClass().getDeclaredMethod("getArchives");
		m.setAccessible(true);
		
		@SuppressWarnings("unchecked")
		List<File> arcFiles = (ArrayList<File>) m.invoke(hgj);
		
		return arcFiles;
	}

	static void markStarted(HadoopGridJob hgj) throws NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		Method m = hgj.getClass().getDeclaredMethod("markAsStarted");
		m.setAccessible(true);
		m.invoke(hgj);
	}

	static void markFinished(HadoopGridJob hgj) throws NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		Method m = hgj.getClass().getDeclaredMethod("markAsFinished");
		m.setAccessible(true);
		m.invoke(hgj);
	}
}
