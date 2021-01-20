import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyJoin2 {
    public static class Tablebean implements WritableComparable<Tablebean> {
        private String order_id;
        private String order_date;
        private int product_id;
        private String name;
        private int amount;
        private int price;
        private int flag;
        public Tablebean() {
            super();
        }
        public void set(String order_id, String order_date, int product_id, String name, int amount, int price, int flag) {
            this.order_id = order_id;
            this.order_date = order_date;
            this.product_id = product_id;
            this.name = name;
            this.amount = amount;
            this.price = price;
            this.flag = flag;
        }

        public String getOrder_id() {
            return this.order_id;
        }
        public void setOrder_id(String order_id) {
            this.order_id = order_id;
        }

        public String getOrder_date() {
            return this.order_date;
        }
        public void setOrder_date(String order_date) {
            this.order_date = order_date;
        }

        public int getProduct_id() {
            return this.product_id;
        }
        public void setProduct_id(int product_id) {
            this.product_id = product_id;
        }

        public String getName() {
            return this.name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public int getAmount() {
            return this.amount;
        }
        public void setAmount(int amount) {
            this.amount = amount;
        }

        public int getPrice() {
            return this.price;
        }
        public void setPrice(int price) {
            this.price = price;
        }
        public int getFlag() {
            return this.flag;
        }
        public void setFlag(int flag) {
            this.flag = flag;
        }
        @Override
        public void write(DataOutput output) throws IOException {
            output.writeUTF(this.order_id);
            output.writeUTF(this.order_date);
            output.writeInt(this.product_id);
            output.writeUTF(this.name);
            output.writeInt(this.amount);
            output.writeInt(this.price);
            output.writeInt(this.flag);
        }
        @Override
        public void readFields(DataInput input) throws IOException {
            this.order_id = input.readUTF();
            this.order_date = input.readUTF();
            this.product_id = input.readInt();
            this.name = input.readUTF();
            this.amount = input.readInt();
            this.price = input.readInt();
            this.flag = input.readInt();
        }
        @Override
        public String toString() {
            return this.order_id + " " + this.order_date + " " + this.product_id + " " + this.name + " " + this.price + " " + this.amount;
        }

        public int compareTo(Tablebean o) {
            return this.product_id - o.product_id;
        }
    }

    public static class DistributedMapper extends Mapper<LongWritable, Text, Text, Tablebean> {
        private Tablebean v = new Tablebean();
        private Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            FileSplit fs = (FileSplit) context.getInputSplit();
            String filename = fs.getPath().getName();
            if(filename.startsWith("order")) {
                String[] fileds = line.split(" ");
                v.setOrder_id(fileds[0]);
                v.setOrder_date(fileds[1]);
                v.setProduct_id(Integer.valueOf(fileds[2]));
                v.setAmount(Integer.valueOf(fileds[3]));
                v.setName("");
                v.setPrice(0);
                v.setFlag(0);

                k.set(fileds[2]);
            }
            else {
                String[] fileds = line.split(" ");
                v.setProduct_id(Integer.valueOf(fileds[0]));
                v.setName(fileds[1]);
                v.setPrice(Integer.valueOf(fileds[2]));
                v.setOrder_id("");
                v.setOrder_date("");
                v.setFlag(1);
                v.setAmount(0);

                k.set(fileds[0]);
            }
            context.write(k, v);
        }
    }
    public static class MyReducer extends Reducer<Text, Tablebean, Tablebean, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Tablebean> values, Context context)
                throws IOException, InterruptedException {
            Tablebean pt = new Tablebean();
            ArrayList<Tablebean> arrayList = new ArrayList<Tablebean>();

            for(Tablebean value : values) {
                if(value.getFlag() == 1) {
                    try {
                        BeanUtils.copyProperties(pt, value);
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }else {
                   Tablebean other = new Tablebean();
                   try {
                       BeanUtils.copyProperties(other, value);
                       arrayList.add(other);
                   }catch (Exception e) {
                       e.printStackTrace();
                   }
                }
            }
            for(Tablebean tablebean : arrayList) {
                tablebean.setName(pt.getName());
                tablebean.setPrice(pt.getPrice());
                context.write(tablebean, NullWritable.get());
            }

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherargs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherargs.length != 2) {
            System.err.println("Usage:hadoop jar  input output");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MyJion2");
        job.setJarByClass(MyJoin2.class);
        job.setMapperClass(DistributedMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tablebean.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Tablebean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(otherargs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

