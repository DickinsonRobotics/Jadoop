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

package jadoop.util;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * A SequenceFileInputFormat that produces splits with one key,value pair per
 * split. 
 * 
 * In hadoop each InputSplit is assigned to an individual Mapper for processing.  
 * Thus, if each key,value pair specifies a compute-bound task (as is the case with
 * grid computing applications) then this is an ideal split.  This is the case with
 * Jadoop where each HadoopGridTask in a HadoopGridJob becomes one key,value pair in
 * the input to the cluster.
 * 
 * @author Grant Braught
 * @author Dickinson College
 * @version Jun 9, 2015
 */
public class SingleRecordSplitSequenceFileInputFormat extends
		SequenceFileInputFormat<Text, TextArrayWritable> {

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);

		for (FileStatus file : files) {
			Path seqFilePath = file.getPath();

			SequenceFile.Reader reader = new SequenceFile.Reader(
					job.getConfiguration(), Reader.file(seqFilePath));

			Text key = new Text();
			TextArrayWritable val = new TextArrayWritable();

			long prevPos = 0;
			while (reader.next(key, val)) {
				long curPos = reader.getPosition();
				FileSplit split = new FileSplit(seqFilePath, prevPos, curPos
						- prevPos, null);
				splits.add(split);
				prevPos = curPos;
			}

			reader.close();
		}

		return splits;
	}
}
