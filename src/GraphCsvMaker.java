import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;


public class GraphCsvMaker {
    static HashMap<String, Integer> nodes = new HashMap<String,Integer>();  //节点
    static HashMap<String, Double> edges = new HashMap<String,Double>();   //边
    static HashMap<String, Integer> tags = new HashMap<String,Integer>();   //小说类别
    static HashMap<String, Double> prs = new HashMap<String,Double>();     //

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        //读取FinalRank，即角色比重
        String line;
        Scanner scPr = new Scanner(new File("/home/kexin/IdeaProjects/jinyong/output/FinalRank/part-r-00000"),"UTF-8");
        while(scPr.hasNextLine()){
            line = scPr.nextLine();
            StringTokenizer st0=new StringTokenizer(line);
            double eValue = Double.parseDouble(st0.nextToken());
            String word = st0.nextToken();

            prs.put(word, eValue);
        }
        scPr.close();

        //读取RawTag，即角色类别
        Scanner scTag = new Scanner(new File("/home/kexin/IdeaProjects/jinyong/input/RawTagInput/RawTag.txt"),"UTF-8");
        int nodecount =0;
        while(scTag.hasNextLine()){
            line = scTag.nextLine();
            StringTokenizer stWord=new StringTokenizer(line);
            String word = stWord.nextToken();
            int tag = Integer.parseInt(stWord.nextToken());
            tags.put(word, tag);
            nodes.put(word, nodecount);
            nodecount++;
        }
        scTag.close();

        //读取readoutput，即人物关系比重
        Scanner scEdge = new Scanner(new File("/home/kexin/IdeaProjects/jinyong/output/readOutput/part-r-00000"),"UTF-8");
        while(scEdge.hasNext()){
            line = scEdge.nextLine();
            StringTokenizer stWord = new StringTokenizer(line);
            String word = stWord.nextToken();
            StringTokenizer stValue = new StringTokenizer(stWord.nextToken(), "|");
            String tline;
            int wordNo = 0;
            try {
                wordNo = nodes.get(word);
            }catch (Exception e){
                System.out.println(1);
                continue;
            }
            while(stValue.hasMoreTokens()){
                tline = stValue.nextToken();
                StringTokenizer st2 = new StringTokenizer(tline, ":");
                String tar = st2.nextToken();
                double eValue = Double.parseDouble(st2.nextToken());
                int tarNo = 0;
                try{
                    tarNo = nodes.get(tar);
                }catch (Exception e){
                    System.out.println(1);
                    continue;
                }
                if(wordNo > tarNo){
                    int t = wordNo;
                    wordNo=tarNo;
                    tarNo = t;
                }
                String edge = new String(wordNo+","+tarNo);
                edges.put(edge, eValue);
            }
        }
        scEdge.close();
        //将tags，nodes，prs写node.csv
        PrintWriter pr2 = new PrintWriter(new File("/home/kexin/IdeaProjects/jinyong/output/node.csv"),"UTF-8");
        Set<Entry<String,Integer>> set = tags.entrySet();
        Iterator<Entry<String,Integer>> it0= set.iterator();
        //write file
        pr2.println("id,label,tag,pr");
        while(it0.hasNext()){
            Entry<String,Integer> entry = it0.next();
            String word = entry.getKey();
            int tag = entry.getValue();
            int nodeId = nodes.get(word);
            double value_pr = prs.get(word);
            pr2.println(nodeId+","+word+","+tag+","+value_pr);
        }
        pr2.close();

        //将edges写edges.csv
        PrintWriter pr3 = new PrintWriter(new File("/home/kexin/IdeaProjects/jinyong/output/edge.csv"));
        Set<Entry<String, Double>> set0 = edges.entrySet();
        Iterator<Entry<String, Double>> it1 = set0.iterator();
        int edgeCount =0;
        
        //write file
        pr3.println("Source,Target,id,weight");
        while(it1.hasNext()){
            Entry<String, Double> entry=it1.next();
            String edge = entry.getKey();
            double weight = entry.getValue();
            pr3.println(edge+","+edgeCount+","+weight);
            ++edgeCount;
        }
        pr3.close();
        System.out.println("Processing sucess!");
    }
}
