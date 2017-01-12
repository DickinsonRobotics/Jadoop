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

import java.io.IOException;
import java.net.URISyntaxException;

import jadoop.HadoopGridJob;
import jadoop.HadoopGridTask;

/**
 * A sample program that when run will compile a list of the hostnames on which
 * a set of HadoopGridTasks (contained in a HadoopGridJob) are executed.
 * 
 * @author Grant Braught
 * @author Dickinson College
 * @version Jun 10, 2015
 */
public class Hostnames {

	private static int NUM_HOSTS = 10;

	/**
	 * Run a HadoopGridJob that produces a list of the hostnames used as
	 * execution nodes. This is accomplished by running a collection of
	 * HadoopGridTasks that each execute the hostname command on their execution
	 * node.
	 */
	public static void main(String[] args) throws IOException,
			IllegalStateException, ClassNotFoundException,
			InterruptedException, URISyntaxException {

		// Build the job by adding one command for each hostname to be printed.
		HadoopGridJob hgj = new HadoopGridJob("HostnameExample");
		for (int i = 0; i < NUM_HOSTS; i++) {
			/*
			 * Build the task, capture standard output but not standard error
			 * and use a timeout of 1 second for the task. Note: This assumes
			 * that the nodes in the cluster have the hostname command on the
			 * default path.
			 */
			HadoopGridTask hgt = new HadoopGridTask("Task" + i, "hostname",
					true, false, 1000);
			hgj.addTask(hgt);
		}

		// Run the job and wait for it to complete.
		hgj.runJob(true);

		/*
		 * Print the results of all of the tasks. Note: the results do not
		 * appear in the order they were added to the job because they are being
		 * retrieved from a HashMap. If that order is required, the keys can be
		 * generated in order and used to get the associated task.
		 */
		for (HadoopGridTask hgt : hgj.getAllTasks()) {
			System.out.println(hgt.getStandardOutput());
		}
	}
}
