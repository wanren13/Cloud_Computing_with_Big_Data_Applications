package stubs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

//@SuppressWarnings("unused")
public class SequenceFileWriter extends Configured implements Tool{

//	static private enum exitCode {
		static private final int 
		Success = 0,
		InputFileNotExists = 1,
		OutputFileExists = 2,
		WrongArgumentsNumber = 3; 
//	};

		private static FileSystem fs; 
		private static Configuration conf;
		
		@Override
		public int run(String[] args) throws Exception {
			
			int exitCode = Success;
			
			if (args.length != 2) {
				System.err.printf("Usage: SequenceFileWriter <input dir> <output dir>\n");
				exitCode = WrongArgumentsNumber;
				return exitCode;
			}
			
			Path inPath = new Path(args[0]);
			Path outPath = new Path(args[1]);
			conf = getConf();
			fs = FileSystem.get(conf);

			if (fs.exists(outPath)) {
				System.err.println("Output file exists");
				exitCode = OutputFileExists;
				return exitCode;
			}

			if (fs.exists(inPath)) {
				ArrayList<FileStatus> listOfFiles = null;
				if (fs.isDirectory(inPath)) {
					listOfFiles = new ArrayList<FileStatus>(Arrays.asList(fs.listStatus(inPath)));
				}
				if (fs.isFile(inPath)) {
					listOfFiles = new ArrayList<FileStatus>();
					listOfFiles.add(fs.getFileStatus(inPath));
				}
				if (listOfFiles != null) {
					writeSequenceFile(listOfFiles, args[1]);
				}
			} else {
				System.err.println(args[0] + " does not exist");
				exitCode = InputFileNotExists;
				return exitCode;
			}
			
			System.out.println("Job Done! Check out result.seq in " + args[1] + "/");
					
			return exitCode;
		}

	private static void writeSequenceFile(ArrayList<FileStatus> listOfFiles, String outPath) throws IOException {
		Text key = new Text();
		Text value = new Text();

		Pattern pattern = Pattern.compile("(\\d{1,3}(?:\\.\\d{1,3}){3})");

		Option compressOption = Writer.compression(CompressionType.NONE);//, new DefaultCodec());
		SequenceFile.Writer writer = null;

		try {
			writer = SequenceFile.createWriter(
					conf, 
					SequenceFile.Writer.file(new Path(outPath + "//result.seq")),
					SequenceFile.Writer.keyClass(key.getClass()),
					SequenceFile.Writer.valueClass(value.getClass()),
					compressOption
					);

			for (FileStatus f : listOfFiles) {
				if (f.isFile()) {			
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
					String line;

					line = br.readLine();

					while (line != null) {
						Matcher matcher = pattern.matcher(line);
						if (matcher.find()) {
							key.set(matcher.group(0));
							value.set(line);
							writer.append(key, value);
						}
						line = br.readLine();
					}
				}					
			}
		}
		finally {
			IOUtils.closeStream(writer);
		}
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SequenceFileWriter(), args);
	    System.exit(exitCode);		
	}	
}
