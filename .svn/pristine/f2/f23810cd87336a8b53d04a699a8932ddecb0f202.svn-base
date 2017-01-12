//package jadoop;
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

/**
 * Sample task that just prints its arguments to standard output and standard
 * error. This is used by a number of the tests.
 * 
 * NOTE: This class must be compiled into the default package so that the class
 * file can be placed in and run from the working directory on the cluster.
 */
public class GridTaskRunnerSampleTask {

	public static void main(String[] args) {
		int ev = Integer.parseInt(args[0]);
		String stdOut = args[1];
		String stdErr = args[2];

		System.out.println(stdOut);
		System.err.println(stdErr);
		System.exit(ev);
	}
}
