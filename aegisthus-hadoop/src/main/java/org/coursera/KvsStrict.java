package org.coursera;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.netflix.aegisthus.input.AegisthusInputFormat;
import com.netflix.aegisthus.mapred.reduce.CassReducer;
import com.netflix.aegisthus.tools.DirectoryWalker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class KvsStrict extends Configured implements Tool {
	public static class Map extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	private static final String OPT_INPUT = "input";
	private static final String OPT_INPUTDIR = "inputDir";
	private static final String OPT_OUTPUT = "output";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KvsStrict(), args);

		System.exit(res);
	}

	protected List<Path> getDataFiles(Configuration conf, String dir) throws IOException {
		Set<String> globs = Sets.newHashSet();
		List<Path> output = Lists.newArrayList();
		Path dirPath = new Path(dir);
		FileSystem fs = dirPath.getFileSystem(conf);
		List<FileStatus> input = Lists.newArrayList(fs.listStatus(dirPath));
		for (String path : DirectoryWalker.with(conf).threaded().addAllStatuses(input).pathsString()) {
			if (path.endsWith("-Data.db")) {
				globs.add(path.replaceAll("[^/]+-Data.db", "*-Data.db"));
			}
		}
		for (String path : globs) {
			output.add(new Path(path));
		}
		return output;
	}


	@SuppressWarnings("static-access")
	public CommandLine getOptions(String[] args) {
		Options opts = new Options();
		opts.addOption(OptionBuilder
				.withArgName(OPT_INPUT)
				.withDescription("Each input location")
				.hasArgs()
				.create(OPT_INPUT));
		opts.addOption(OptionBuilder
				.withArgName(OPT_OUTPUT)
				.isRequired()
				.withDescription("output location")
				.hasArg()
				.create(OPT_OUTPUT));
		opts.addOption(OptionBuilder
				.withArgName(OPT_INPUTDIR)
				.withDescription("a directory from which we will recursively pull sstables")
				.hasArgs()
				.create(OPT_INPUTDIR));
		CommandLineParser parser = new GnuParser();

		try {
			CommandLine cl = parser.parse(opts, args, true);
			if (!(cl.hasOption(OPT_INPUT) || cl.hasOption(OPT_INPUTDIR))) {
				System.out.println("Must have either an input or inputDir option");
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(String.format("hadoop jar aegsithus.jar %s", KvsStrict.class.getName()), opts);
				return null;
			}
			return cl;
		} catch (ParseException e) {
			System.out.println("Unexpected exception:" + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(String.format("hadoop jar aegisthus.jar %s", KvsStrict.class.getName()), opts);
			return null;
		}

	}

	@Override
	public int run(String[] args) throws Exception {
                CommandLine cl = getOptions(args);
                if (cl == null) {
                        return 1;
                }

		Job job1 = new Job(getConf());
		job1.setJarByClass(KvsStrict.class);
		job1.setInputFormatClass(AegisthusInputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setMapperClass(KvsStrictMapper.class);
		AegisthusInputFormat.setInputPaths(job1, new Path(cl.getOptionValue(OPT_INPUT)));
		TextOutputFormat.setOutputPath(job1, new Path(cl.getOptionValue(OPT_OUTPUT)));

		job1.submit();
		job1.waitForCompletion(true);
		boolean success = job1.isSuccessful();
		return success ? 0 : 1;
	}
}
