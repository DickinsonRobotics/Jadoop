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

import java.util.Random;

/**
 * This task reads a number of coin flips in args[0], flips a coin that many
 * times, and prints the number of heads.
 * 
 * If this program represents an experiment, then the JadoopCoinFlipExperiment
 * class illustrates how Jadoop can be used to run many trials of an experiment
 * in parallel.
 */
public class CoinFlipTask {

	private static Random rnd = new Random();

	public static void main(String[] args) {
		/*
		 * Get the number of flips from args[0], where it was provided by the
		 * HadoopGridTask.
		 */
		int flips = Integer.parseInt(args[0]);

		// Do the flips and count the number of heads.
		int heads = 0;
		for (int f = 0; f < flips; f++) {
			if (rnd.nextBoolean()) {
				heads++;
			}
		}

		/*
		 * Print the result to standard output, which will be captured and made
		 * available through the HadoopGridTask.
		 */
		System.out.println(heads);
	}
}
