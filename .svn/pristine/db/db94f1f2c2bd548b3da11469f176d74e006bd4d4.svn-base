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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import jadoop.HadoopGridJob;
import jadoop.HadoopGridTask;

/**
 * Example of running multiple trials of an experiment in parallel using Jadoop.
 * The CoinFlipTask is uploaded to the HDFS and then run multiple times
 * (potentially in parallel on a cluster).
 */
public class CoinFlipExperiment {

	private static final int TRIALS = 10;
	private static final int FLIPS = 1000;

	public static void main(String[] args) throws IOException,
			IllegalStateException, ClassNotFoundException,
			InterruptedException, URISyntaxException {

		// Create the job.
		HadoopGridJob hgj = new HadoopGridJob("CoinflipExample");

		// Make the CoinFlipTask class file available on the cluster.
		hgj.addFile(new File("examples/CoinFlipTask.class"));

		/*
		 * Add one command for each trial. Each command executes the
		 * CoinFlipTask with an argument indicating the number of flips to
		 * perform.
		 */
		for (int t = 0; t < TRIALS; t++) {
			HadoopGridTask hgt = new HadoopGridTask("Trial" + t,
					"java CoinFlipTask " + FLIPS, true, false, 1000);
			hgj.addTask(hgt);
		}

		// Run the job and wait for it to complete.
		hgj.runJob(true);

		/*
		 * Display each of the trial results and compute the average number of
		 * heads across all of the trials.
		 */
		int total = 0;
		for (int t = 0; t < TRIALS; t++) {
			String key = "Trial" + t;
			HadoopGridTask hgt = hgj.getTask(key);

			int cur = Integer.parseInt(hgt.getStandardOutput());
			total = total + cur;

			System.out.println(hgt.getKey() + " : " + hgt.getStandardOutput());
		}
		System.out.println("Average : " + ((double) total) / TRIALS);
	}
}
