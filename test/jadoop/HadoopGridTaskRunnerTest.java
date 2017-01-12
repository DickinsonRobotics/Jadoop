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

import jadoop.HadoopGridTaskRunner;
import jadoop.util.TextArrayWritable;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class HadoopGridTaskRunnerTest extends TestCase {

	private HadoopGridTaskRunner hgtr;
	private MockContext mc;

	// keys of mapper
	private Text evKey;
	private Text toKey;
	private Text soKey;
	private Text seKey;

	@Before
	protected void setUp() throws Exception {
		hgtr = new HadoopGridTaskRunner();
		mc = new MockContext();
		evKey = new Text("EV");
		toKey = new Text("TO");
		soKey = new Text("SO");
		seKey = new Text("SE");
	}

	@Test
	public void testMapTrueTrueStandardOutputStandardError()
			throws IOException, InterruptedException {

		Text key = new Text("key1");
		String[] value = new String[] { "true", "true", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "0", "output", "error" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		assertEquals("Key of result not set correctly.", "key1",
				mc.keyFromMapper.toString());

		assertNotNull("map value should not be null", mc.mapFromMapper);

		byte exitVal = ((ByteWritable) (mc.mapFromMapper.get(evKey))).get();
		assertEquals("Incorrect exit value.", 0, exitVal);

		boolean toVal = ((BooleanWritable) (mc.mapFromMapper.get(toKey))).get();
		assertFalse("Incorrect timeout value.", toVal);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("standard output was not set correctly", "output",
				stdOutput);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("standard error was not set correctly", "error", stdErr);
	}

	@Test
	public void testMapTrueFalseStandardOutputStandardError()
			throws IOException, InterruptedException {

		Text key = new Text("key2");
		String[] value = new String[] { "true", "false", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "-1", "output2", "error2" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		assertEquals("key from mapper is not set correctly", "key2",
				mc.keyFromMapper.toString());
		assertNotNull("map value shoud not be null", mc.mapFromMapper);

		byte exitVal = ((ByteWritable) (mc.mapFromMapper.get(evKey))).get();
		assertEquals("Incorrect exit value", -1, exitVal);

		boolean toVal = ((BooleanWritable) (mc.mapFromMapper.get(toKey))).get();
		assertFalse("Incorrect timeout value.", toVal);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("standard output was not set correctly", "output2",
				stdOutput);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("SE element should be empty", "", stdErr);
	}

	@Test
	public void testMapFalseTrueStandardOutputStandardError()
			throws IOException, InterruptedException {
		Text key = new Text("key3");
		String[] value = new String[] { "false", "true", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "0", "output3", "error3" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		assertEquals("key was NOT set correctly", "key3",
				mc.keyFromMapper.toString());
		assertNotNull("map value should NOT be null", mc.mapFromMapper);

		byte exitVal = ((ByteWritable) (mc.mapFromMapper.get(evKey))).get();
		assertEquals("Incorrect exit value", 0, exitVal);

		boolean toVal = ((BooleanWritable) (mc.mapFromMapper.get(toKey))).get();
		assertFalse("Incorrect timeout value.", toVal);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("SO element should be empty", "", stdOutput);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("SE element was not set correctly", "error3", stdErr);
	}

	@Test
	public void testMapFalseFalseStandardOutputStandardError()
			throws IOException, InterruptedException {
		Text key = new Text("key4");
		String[] value = new String[] { "false", "false", "1000", "java",
				"-cp", "./bin", "GridTaskRunnerSampleTask", "0", "output3",
				"error3" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		assertEquals("key was NOT set correctly", "key4",
				mc.keyFromMapper.toString());
		assertNotNull("map value should NOT be null", mc.mapFromMapper);

		byte exitVal = ((ByteWritable) (mc.mapFromMapper.get(evKey))).get();
		assertEquals("Incorrect exit value", 0, exitVal);

		boolean toVal = ((BooleanWritable) (mc.mapFromMapper.get(toKey))).get();
		assertFalse("Incorrect timeout value.", toVal);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("SO element should be empty", "", stdOutput);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("SE element should be empty", "", stdErr);
	}

	@Test
	public void testMapLongerStandardOutput() throws IOException,
			InterruptedException {
		Text key = new Text("key5");
		String[] value = new String[] { "true", "false", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "0", "longer output here",
				"error is not captured" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("standard output was not set correctly",
				"longer output here", stdOutput);
	}

	@Test
	public void testMapLongerStandardError() throws IOException,
			InterruptedException {
		Text key = new Text("key6");
		String[] value = new String[] { "false", "true", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "0",
				"output not captured", "longer error here" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("standard output was not set correctly",
				"longer error here", stdErr);
	}

	@Test
	public void testMapQuotedStandardOutputStandardError() throws IOException,
			InterruptedException {
		Text key = new Text("key7");
		String[] value = new String[] { "true", "true", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "0",
				"\"quoted output\"", "\"quoted error\"" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("standard output was not set correctly",
				"\"quoted output\"", stdOutput);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("standard error was not set correctly",
				"\"quoted error\"", stdErr);
	}

	@Test
	public void testNewLinesInOutputs() throws IOException,
			InterruptedException {
		Text key = new Text("key7");
		String[] value = new String[] { "true", "true", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "0",
				"Standard out\nwith\nmultiple\nlines",
				"Standard error\nwith\nmultiple\nlines" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("standard output was not set correctly",
				"Standard out\nwith\nmultiple\nlines", stdOutput);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("standard error was not set correctly",
				"Standard error\nwith\nmultiple\nlines", stdErr);
	}

	@Test
	public void testMapException() {
		Text key = new Text("key8");
		String[] value = new String[] { "true", "true", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "0", "output8", "error8" };
		TextArrayWritable awVal = new TextArrayWritable(value);

		Writable[] wrt = new Writable[value.length];
		for (int i = 0; i < value.length; i++) {
			wrt[i] = new Text(value[i].toString());
		}

		wrt[2] = null;
		awVal.set(wrt);

		try {
			hgtr.map(key, awVal, mc);
			fail("Exception was not caught");
		} catch (NullPointerException npe) {
			// passed
		} catch (Exception e) {
			fail("wrong type of exception was thrown");
		}
	}

	@Test
	public void testTaskTimesOut() throws IOException, InterruptedException {
		Text key = new Text("key1");
		String[] value = new String[] { "true", "true", "1000", "sleep", "10" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		assertEquals("key was NOT set correctly", "key1",
				mc.keyFromMapper.toString());

		assertNotNull("map value should NOT be null", mc.mapFromMapper);

		byte exitVal = ((ByteWritable) (mc.mapFromMapper.get(evKey))).get();
		assertEquals("Incorrect exit value", -1, exitVal);

		boolean toVal = ((BooleanWritable) (mc.mapFromMapper.get(toKey))).get();
		assertTrue("Task should have timed out.", toVal);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("SO element should be empty", "", stdOutput);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("SE element should be empty", "", stdErr);
	}

	@Test
	public void testTaskGrabsStdOutErrBeforeTimeout() throws IOException,
			InterruptedException {

		Text key = new Text("key1");
		String[] value = new String[] { "true", "true", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerTimeoutTask", "0", "output", "error" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		assertEquals("key was NOT set correctly", "key1",
				mc.keyFromMapper.toString());

		assertNotNull("map value should NOT be null", mc.mapFromMapper);

		byte exitVal = ((ByteWritable) (mc.mapFromMapper.get(evKey))).get();
		assertEquals("Incorrect exit value", -1, exitVal);

		boolean toVal = ((BooleanWritable) (mc.mapFromMapper.get(toKey))).get();
		assertTrue("Task should have timed out.", toVal);

		String stdOutput = ((Text) (mc.mapFromMapper.get(soKey))).toString();
		assertEquals("SO element should have output before timeout", "output",
				stdOutput);

		String stdErr = ((Text) (mc.mapFromMapper.get(seKey))).toString();
		assertEquals("SE element should have output before timeout", "error",
				stdErr);
	}

	@Test
	public void testTaskSetsStatus() throws IOException, InterruptedException {
		assertNull("no status yet", mc.getStatus());

		Text key = new Text("key1");
		String[] value = new String[] { "true", "true", "1000", "java", "-cp",
				"./bin", "GridTaskRunnerSampleTask", "0", "output", "error" };
		TextArrayWritable awVal = new TextArrayWritable(value);
		hgtr.map(key, awVal, mc);

		assertTrue("Status was not set.",
				mc.getStatus().contains("Running for:"));
	}
}
