package example;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

public class FirstExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Tuple2<String,Integer>> ds =env.addSource(new DataSource());

        ds.keyBy(0).sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }
        }).fold(new HashMap<String,Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> fold(HashMap<String, Integer> stringIntegerHashMap, Tuple2<String, Integer> o) throws Exception {
                stringIntegerHashMap.put(o.f0,o.f1);
                return stringIntegerHashMap;
            }
        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
            @Override
            public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                System.out.printf("Group By category sum: %s",value.toString());
                System.out.println();
            }
        });

        ds.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }
        }).sum(1).addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                System.out.printf("All category sum: %d",value.f1);
                System.out.println();
            }
        });

        env.execute("FirstExample");

    }
    /**
     * Parallel data source that serves a list of key-value pairs.
     */
    private static class DataSource extends RichParallelSourceFunction<Tuple2<String,Integer>> {

        private volatile boolean running = true;
        private Random rand = new Random();

        @Override
        public void run(SourceContext<Tuple2<String,Integer>> ctx) throws Exception {


            while (running) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask()+1)*1000+500);
                String key="Category"+(char)('A'+rand.nextInt(3));
                int value = rand.nextInt(10)+1;
                System.out.printf("Emit:\t(%s,%d)",key,value);
                System.out.println();
                ctx.collect(new Tuple2<String,Integer>(key,value));
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
