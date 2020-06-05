import java.io.FileReader;
import java.util.*;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.IOException;

import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.domain.Term;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReadNovel {
   /*数据预处理，得到人物同现概率*/
    public static class ReadNovelMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*对每一段进行分词，提取每一段的人名*/
            List<Term> PPLResult = DicAnalysis.parse(value.toString()).getTerms();
            HashMap<String, Integer> nameMap = new HashMap<String, Integer>();
            for (Term term : PPLResult)
                if (term.getNatureStr().equals(new String("WuxiaNames")))
                    nameMap.put(term.getName(), 1);

            /*进行人物同现统计，找出所有匹配对*/
            Set<Entry<String, Integer>> nameSet = nameMap.entrySet();
            Entry<String, Integer> entryA, entryB;
            for(Iterator<Entry<String, Integer>> iteratorA = nameSet.iterator(); iteratorA.hasNext();){
                entryA = iteratorA.next();
                for(Iterator<Entry<String, Integer>> iteratorB = nameSet.iterator(); iteratorB.hasNext();){
                    entryB = iteratorB.next();
                    if(entryA.getKey().equals(entryB.getKey()) == false)
                        context.write(new Text(entryA.getKey()), new Text(entryB.getKey()));
                }
            }
        }
    }

    public static class ReadNovelReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            /*将匹配对进行整合*/
            HashMap<String, Integer> namePairMap = new HashMap<String, Integer>();
            for(Text index : value){
                String str = index.toString();
                if(namePairMap.containsKey(str))
                    namePairMap.put(str, namePairMap.get(str).intValue() + 1);
                else
                    namePairMap.put(str, 1);
            }

            /*获得总权重*/
            Set<Entry<String, Integer>> namePairSet = namePairMap.entrySet();
            int weightValue = 0;
            Iterator<Entry<String, Integer>> namePairSetIteratorForWei = namePairSet.iterator();
            while(namePairSetIteratorForWei.hasNext())
                weightValue += namePairSetIteratorForWei.next().getValue();

            /*计算概率值*/
            Iterator<Entry<String, Integer>> namePairSetIteratorForCalc = namePairSet.iterator();
            StringBuilder namePairString = new StringBuilder();
            Boolean isFirstLoop = true;
            while(namePairSetIteratorForCalc.hasNext()){
                Entry<String,Integer> entry = namePairSetIteratorForCalc.next();
                String tName = entry.getKey();
                double tValue = ((double)entry.getValue()) / (double)weightValue;
                if(isFirstLoop) {
                    namePairString.append(tName + ":" + tValue);
                    isFirstLoop = false;
                }
                else
                    namePairString.append("|" + tName + ":" + tValue);
            }
            context.write(key, new Text(namePairString.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf,"Read Novel");
        job.setJarByClass(ReadNovel.class);
        job.setMapperClass(ReadNovelMapper.class);
        job.setReducerClass(ReadNovelReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/home/kexin/IdeaProjects/jinyong/input/novel"));
        FileOutputFormat.setOutputPath(job, new Path("/home/kexin/IdeaProjects/jinyong/output/readOutput"));
        job.waitForCompletion(true);
    }
}