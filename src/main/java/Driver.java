import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {

        String inputPath = args[0];
        String nGramLibPath = args[1];
        String numOfGram = args[2];
        String threshold = args[3];
        String topk = args[4];

        Configuration conf1 = new Configuration();

        //Define the job to read data sentence by sentence
        conf1.set("textinputformat.record.delimiter", "\\pP|\\pS");
        conf1.set("noGram", numOfGram);

        Job job1 = Job.getInstance(conf1);
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        // set only once because output class for mapper and reducer are the same
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(nGramLibPath));
        job1.waitForCompletion(true);



        // Second Job
        Configuration conf2 = new Configuration();

        conf2.set("threshold", threshold);
        conf2.set("k", topk);

        // configure DB url(ip address + db name), username, password
        DBConfiguration.configureDB(conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://143.215.62.109:3306/test",
                "root",
                "FriApr14");

        Job job2 = Job.getInstance(conf2);
        job2.setJobName("WriteToDB");
        job2.setJarByClass(Driver.class);

        // upload mysql driver to HDFS, because hadoop can only use driver from hdfs
        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));

        job2.setMapperClass(LanguageModel.LanguageMapper.class);
        job2.setReducerClass(LanguageModel.LanguageReducer.class);

        // set output class respectively since mapper's output is not the same as reducer
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        //use dbOutputformat to define the table name and columns
        DBOutputFormat.setOutput(job2, "output",
                new String[] {"starting_phrase", "following_word", "count"});

        TextInputFormat.setInputPaths(job2, nGramLibPath);
        job2.waitForCompletion(true);
    }
}
