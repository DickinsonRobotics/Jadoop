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
 * A sample program that when run will print the date of Easter for a range of
 * years.
 * 
 * @author Grant Braught
 * @author Dickinson College
 * @version May 30, 2016
 */
public class Easter {

	private static int START_YEAR = 1990;
	private static int END_YEAR = 2020;

	/**
	 * Run a HadoopGridJob that produces the date of Easter in the range of
	 * years specified by the class constants above.
	 */
	public static void main(String[] args) throws IOException,
			IllegalStateException, ClassNotFoundException,
			InterruptedException, URISyntaxException {

		// Build the job by adding one command for each Easter date.
		HadoopGridJob hgj = new HadoopGridJob("EasterExample");
		for (int y = START_YEAR; y <= END_YEAR; y++) {
			/*
			 * Build the task, capture standard output but not standard error
			 * and use a timeout of 1 second for the task. NOTE: This assumes
			 * that the nodes in the cluster have the ncal command on the
			 * default path.
			 */
			HadoopGridTask hgt = new HadoopGridTask("Year" + y, "ncal -e " + y,
					true, false, 1000);
			hgj.addTask(hgt);
		}

		// Run the job and wait for it to complete.
		hgj.runJob(true);

		// Print the results of all of the tasks.
		for (int y = START_YEAR; y <= END_YEAR; y++) {
			String key = "Year" + y;
			HadoopGridTask hgt = hgj.getTask(key);
			System.out.println(hgt.getStandardOutput());
		}
	}
}
