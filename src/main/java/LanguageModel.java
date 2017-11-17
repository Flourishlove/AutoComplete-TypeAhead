import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {
    public static class LanguageMapper extends Mapper<LongWritable, Text, Text, Text>{
        int threshold;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.threshold = conf.getInt("threshold", 15);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if((value == null) || (value.toString().trim()).length() == 0) {
                return;
            }
            //this is cool\t20
            String line = value.toString().trim();

            String[] wordsPlusCount = line.split("\t");
            if(wordsPlusCount.length < 2) {
                return;
            }

            // ignore whose count is less than threshold
            int count = Integer.valueOf(wordsPlusCount[1]);
            if(count < threshold) {
                return;
            }

            int index = wordsPlusCount[0].lastIndexOf(" ");
            String outputKey = wordsPlusCount[0].substring(0, index).trim();
            String outputValue = wordsPlusCount[0].substring(index).trim();

            if(!((outputKey == null) || (outputKey.length() <1))) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }
        }
    }

    public static class LanguageReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable>{
        int topk;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            topk = conf.getInt("topk", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //this is, <girl = 50, boy = 60>
            PriorityQueue<DBOutputWritable> pq = new PriorityQueue<DBOutputWritable>(topk, new Comparator<DBOutputWritable>() {
                public int compare(DBOutputWritable o1, DBOutputWritable o2) {
                    return o1.getCount() - o2.getCount();
                }
            });

            for(Text val : values){
                String curValue = val.toString().trim();
                String word = curValue.split("=")[0].trim();
                int count = Integer.parseInt(curValue.split("=")[1].trim());

                DBOutputWritable temp = new DBOutputWritable(key.toString(), word ,count);
                if(pq.isEmpty() || pq.size() < topk) pq.add(temp);
                else {
                    if(count > pq.peek().getCount()){
                        pq.poll();
                        pq.add(temp);
                    }
                }
            }

            while(!pq.isEmpty()){
                context.write(pq.poll(), NullWritable.get());
            }

        }
    }
}
