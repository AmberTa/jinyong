import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class PageRank {
    /*PageRank排序类*/
    public static class PageRankViewer {
        public static void pageRankViewer(String inputPath, String outputPath) throws InterruptedException, IOException, ClassNotFoundException {
            Configuration conf=new Configuration();
            Job job = new Job(conf,"Part4.3 - PageRank Sort");
            job.setJarByClass(PageRankViewer.class);
            job.setMapperClass(PageRankViewerMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setSortComparatorClass(DoubleWritableDecressingComparator.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.waitForCompletion(true);
        }

        public static class PageRankViewerMapper extends Mapper<Object,Text,DoubleWritable,Text>
        {

            public void map(Object key,Text value,Context context) throws IOException,InterruptedException {
                String[] line = value.toString().split("\t");
                String page = line[0];
                double pr = Double.parseDouble(line[1]);
                context.write(new DoubleWritable(pr),new Text(page));
            }
        }

        private static class DoubleWritableDecressingComparator extends DoubleWritable.Comparator {
            public int compare(WritableComparable a,WritableComparable b) {
                return -super.compare(a,b);
            }
            /*二进制数据的字典顺序*/
            public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2) {
                return -super.compare(b1,s1,l1,b2,s2,l2);
            }
        }
    }

    /*PageRank值迭代计算类*/
    public static class PageRankIter{
        public static void pageRankIter(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf=new Configuration();
	/*创建任务来迭代更新PageRank值*/
            Job job = new Job(conf,"Part4.2 - PageRank Calculate");
            job.setJarByClass(PageRankIter.class);
	/*Mapper操作用来产生<人物名，关系链表>和<人物名，pagerank值>*/
            job.setMapperClass(PRIterMapper.class);
	/*reduce将同一人物名的<key,value>汇聚在一起
	如果value是PR值，则累加到sum变量；
	如果value是关系链表则保存为List。
	遍历完迭代器里所有的元素后输出键值对<人物名，sum#List>
	即最初输入的格式*/
            job.setReducerClass(PRIterReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.waitForCompletion(true);
        }

        public static class PRIterMapper extends Mapper<LongWritable,Text,Text,Text> {
            public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
                String line =value.toString();
                String[] tokens = line.split("\t");
	/*人物名字*/
                String pageKey=tokens[0];
	/*人物当前PageRank值*/
                double pr_init = Double.parseDouble(tokens[1]);
                String links;
                if(tokens.length>=3) {
                    links = tokens[2];
                    /*name1:a|name2:b|name3:c*/
                    /*   |  具体特殊含义！！！！得转义*/
                    String[] linkPages = links.split("\\|");
	    /*对每一对姓名<i,j>计算j的PageRank值pr_init*w[i,j]*/
                    for (String stri : linkPages) {
                         /*name1:a*/
                        String[] tmp = stri.split(":");
                        if(tmp.length < 2)
                            continue;
	        /*获取i到j人物的权值*/
                        double proportion = Double.parseDouble(tmp[1]);
	        /*输出人物j在i部分的pagerank，<namej,namei\tpagerank>*/
                        String pr_value = pageKey + "\t" + String.valueOf(pr_init * proportion);
                        context.write(new Text(tmp[0]), new Text(pr_value));
                    }
                    /*在迭代过程中，必须保留原来的链出信息，以维护图的结构添加 "," 供区分
	    输出为<namei,","name1:w1;name2:w2...>*/
                    context.write(new Text(pageKey), new Text("," + links));
                }
            }
        }

        public static class PRIterReducer extends Reducer<Text,Text,Text,Text>
        {
            public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
                String links = "";
                double pr = 0;
	/*对于每一个<人物名，pagerank值>,<人物名，关系链表>*/
                for( Text value:values)
                {
                    String tmp = value.toString();
	    /*如果value是关系链表则保存为List。*/
                    if(tmp.charAt(0)==','){
	        /*将人物i和对应的关系链表写入新的链表*/
                        links  = "\t" ;
                        for(int j=1;j<tmp.length();j++)
                            links+=tmp.charAt(j);
                        continue;
                    }
	    /*否则将PageRank值累加*/
                    pr += Double.parseDouble(tmp.split("\t")[1]);
                }
	/*写出格式<name,pagerank值\t关系链表>*/
                context.write(new Text(key),new Text(String.valueOf(pr) + links ));
            }
        }
    }
    /*创建邻接图类*/
    public static class CreateGraph {
        public static class CreateGraphMapper extends Mapper<Object, Text, Text, Text> {
            public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {
	/*初始化每个人物的pagerank值为1.0*/
                String pagerank = "1.0\t";
                String str=value.toString();
                Text name = new Text(str.split("\t")[0]);
                pagerank += str.split("\t")[1];
                /*生成人物名字   1.0 关系人物1:权重1|关系人物2:权重2|关系人物3:权重3*/
                context.write(name, new Text(pagerank));
            }
        }

        public static void createGraph(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
	/*创建一个任务，用来初始化pagerank*/
            Job job = new Job(conf, "Task4.1 - Create Graph");
            job.setJarByClass(CreateGraph.class);
	/*只进行一次mapper*/
            job.setMapperClass(CreateGraphMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.waitForCompletion(true);
        }
    }

    /*进行pagerank操作的入口*/
    public static final int loopTimes = 20;
    public static void main(String[] args)throws Exception {
        /*if(args != null && args.length > 0){
            if(args[0].equals("pre"))
                CreateGraph.createGraph("/usr/gcx/JinYongNovelAnalysis-master/result/ReadNovelOutput", "/usr/gcx/JinYongNovelAnalysis-master/result/PRData0");
            else if(args[0].equals("final"))
                PageRankViewer.pageRankViewer("/usr/gcx/JinYongNovelAnalysis-master/result/PRData" + loopTimes, "/usr/gcx/JinYongNovelAnalysis-master/result/FinalRank");
            else {
                for (int i = Integer.valueOf(args[0]); i < loopTimes; i++)
                    PageRankIter.pageRankIter("/usr/gcx/JinYongNovelAnalysis-master/result/PRData" + i, "/usr/gcx/JinYongNovelAnalysis-master/result/PRData" + String.valueOf(i + 1));
            }
        }*/
        //else Default
        //else {
	/*根据上一个任务的输出作为mapper输入，只包含一个mapper，初始化pagerank值
	输入为{nameA,nameB:w1;nameC:w2;...nameN:wN}
	输出为{nameA,1.0#nameB:w1;nameC:w2;...nameN:wN}*/
            CreateGraph.createGraph("/home/kexin/IdeaProjects/jinyong/output/readOutput", "/home/kexin/IdeaProjects/jinyong/output/PRData0");
	/*进行迭代计算排名，次数这里设为20次*/
            for (int i = 0; i < loopTimes; i++)
                PageRankIter.pageRankIter("/home/kexin/IdeaProjects/jinyong/output/PRData" + i, "/home/kexin/IdeaProjects/jinyong/output/PRData" + String.valueOf(i + 1));
	/*对所有人物按照pagerank值排序，
	Map过程只提取迭代输出结果中的人物名以及对应的PageRank值，
	并以PageRank值作为key，人物名作为value输出*/
            PageRankViewer.pageRankViewer("/home/kexin/IdeaProjects/jinyong/output/PRData" + loopTimes, "/home/kexin/IdeaProjects/jinyong/output/FinalRank");
        //}
    }
}
