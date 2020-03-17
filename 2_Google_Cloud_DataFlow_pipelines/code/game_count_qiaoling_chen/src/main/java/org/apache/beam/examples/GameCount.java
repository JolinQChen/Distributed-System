package org.apache.beam.examples;

import org.apache.beam.examples.common.ExamplePubsubTopicAndSubscriptionOptions;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.*;

public class GameCount {

    @DefaultCoder(AvroCoder.class)
    static class GameInfo {
        @Nullable String game_id;
        @Nullable Double game_revenue;
        @Nullable String game_genre;
        @Nullable String game_purchase_time;
        @Nullable String game_purchaser_id;
        public GameInfo(){}
        public GameInfo(String id, Double rev){
            this.game_id = id;
            this.game_revenue = rev;
            this.game_genre = null;
        }
        public GameInfo(String genre, String id, Double rev){
            this.game_genre = genre;
            this.game_id = id;
            this.game_revenue = rev;
        }
        public GameInfo(String id, String time, String purchaser){
            this.game_id = id;
            this.game_purchase_time = time;
            this.game_purchaser_id = purchaser;
        }

        public GameInfo(String genre, String id){
            this.game_genre = genre;
            this.game_id = id;
        }
        public String getGameId(){
            return this.game_id;
        }
        public Double getGame_revenue(){
            return this.game_revenue;
        }
        public String getGameGenre(){
            return this.game_genre;
        }
    }


    public interface GameOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        @Required
        String getInputFile();

        void setInputFile(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);

        /** Set this required option to specify the operation. */
        @Description("Path of the file to write to")
        @Required
        String getOperation();

        void setOperation(String value);
    }
    // ============================ part I ===================================
    // ============================ part I ===================================
    // ============================ part I ===================================
    // ============================ part I ===================================
    // 1
    public static void games_purchased(GameOptions options){

        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));

        // get the game id of each line
        PCollection<String> gameId = lines.apply(MapElements.via(new SimpleFunction<String, String>() {
            @Override
            public String apply(String input){
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return tokens[0];
            }
        }));
        // count the times for each game id
        PCollection<KV<String, Long>> gameNumber = gameId.apply(Count.perElement());
        gameNumber
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input){
                        return String.format("%s\t%s", input.getKey(), input.getValue());
                    }
                }))
        .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
    // 3
    public static void developer_purchased(GameOptions options){

        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));

        // get the game id of each line
        PCollection<String> developer_id = lines.apply(MapElements.via(new SimpleFunction<String, String>() {
            @Override
            public String apply(String input){
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return tokens[6];
            }
        }));
        // count the times for each game id
        PCollection<KV<String, Long>> copyNumber = developer_id.apply(Count.perElement());
        copyNumber
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input){
                        return String.format("%s\t%s", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
    // 2
    public static void game_revenue(GameOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));

        // get the game id of each line
        PCollection<GameInfo> game_rev = lines.apply(MapElements.via(new SimpleFunction<String, GameInfo>() {
            @Override
            public GameInfo apply(String input) {
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return new GameInfo(tokens[0], Double.parseDouble(tokens[10]));
            }
        }));
        PCollection<KV<String, Double>> game_rev_count = game_rev
                .apply(
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                                .via((GameInfo ginfo) -> KV.of(ginfo.getGameId(), ginfo.getGame_revenue())))
                .apply(Sum.doublesPerKey());
        // count the revenue for each game id
        //PCollection<KV<String, Long>> gameNumber = gameId.apply(Count.perElement());
        game_rev_count
                .apply(MapElements.via(new SimpleFunction<KV<String, Double>, String>() {
                    @Override
                    public String apply(KV<String, Double> input){
                        return String.format("%s\t%.2f", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
    // 4
    public static void developer_revenue(GameOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));

        // get the game id of each line
        PCollection<GameInfo> game_rev = lines.apply(MapElements.via(new SimpleFunction<String, GameInfo>() {
            @Override
            public GameInfo apply(String input) {
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return new GameInfo(tokens[6], Double.parseDouble(tokens[10]));
            }
        }));
        PCollection<KV<String, Double>> game_rev_count = game_rev
                .apply(
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                                .via((GameInfo ginfo) -> KV.of(ginfo.getGameId(), ginfo.getGame_revenue())))
                .apply(Sum.doublesPerKey());
        // count the revenue for each game id
        //PCollection<KV<String, Long>> gameNumber = gameId.apply(Count.perElement());
        game_rev_count
                .apply(MapElements.via(new SimpleFunction<KV<String, Double>, String>() {
                    @Override
                    public String apply(KV<String, Double> input){
                        return String.format("%s\t%.2f", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }


    //-----------------------part1-5----------------------------
    // filter function
    public static PCollection<GameInfo> filterByGenre(PCollection<GameInfo> input, final String genre){
        return input.apply("FilterByGenre", Filter.by(new SerializableFunction<GameInfo, Boolean>() {
            @Override
            public Boolean apply(GameInfo input) {
                return input.getGameGenre().equals(genre);
            }
        }));
    }


    public static void adventure_game_purchased(GameOptions options){

        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));

        // get the game id of each line
        PCollection<GameInfo> gameId = lines.apply(MapElements.via(new SimpleFunction<String, GameInfo>() {
            @Override
            public GameInfo apply(String input){
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return new GameInfo(tokens[3],tokens[0]); // genre, game_id
            }
        }));
        // filter out adventure games
        PCollection<GameInfo> adventure_game_id_with_genre = filterByGenre(gameId, "Adventure");
        // change to Strings
        PCollection<String> adventure_game_id = adventure_game_id_with_genre
                .apply(MapElements.via(new SimpleFunction<GameInfo, String>() {
                    @Override
                    public String apply(GameInfo input) {
                        return input.getGameId();
                    }
                }));
        // count the times for each game id
        PCollection<KV<String, Long>> adventure_gameNumber = adventure_game_id.apply(Count.perElement());
        adventure_gameNumber
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input){
                        return String.format("%s\t%s", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
    // 3
    public static void adventure_developer_purchased(GameOptions options){

        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));

        // get the game id of each line
        PCollection<GameInfo> developer_id = lines.apply(MapElements.via(new SimpleFunction<String, GameInfo>() {
            @Override
            public GameInfo apply(String input){
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return new GameInfo(tokens[3], tokens[6]);
            }
        }));
        // filter by game genre
        PCollection<GameInfo> adventure_developer_id_with_genre = filterByGenre(developer_id, "Adventure");
        // change to Strings
        PCollection<String> adventure_developer_id = adventure_developer_id_with_genre
                .apply(MapElements.via(new SimpleFunction<GameInfo, String>() {
                    @Override
                    public String apply(GameInfo input) {
                        return input.getGameId();
                    }
                }));

        // count the times for each game id
        PCollection<KV<String, Long>> adventure_copyNumber = adventure_developer_id.apply(Count.perElement());
        adventure_copyNumber
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input){
                        return String.format("%s\t%s", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
    // 2
    public static void adventure_game_revenue(GameOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));

        // get the game id of each line
        PCollection<GameInfo> game_rev = lines.apply(MapElements.via(new SimpleFunction<String, GameInfo>() {
            @Override
            public GameInfo apply(String input) {
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return new GameInfo(tokens[3], tokens[0], Double.parseDouble(tokens[10]));
            }
        }));
        // filter out adventure games "Adventure"
        PCollection<GameInfo> adventure_game_rev = filterByGenre(game_rev,"Adventure");

        PCollection<KV<String, Double>> adventure_game_rev_count = adventure_game_rev
                .apply(
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                                .via((GameInfo ginfo) -> KV.of(ginfo.getGameId(), ginfo.getGame_revenue())))
                .apply(Sum.doublesPerKey());

        adventure_game_rev_count
                .apply(MapElements.via(new SimpleFunction<KV<String, Double>, String>() {
                    @Override
                    public String apply(KV<String, Double> input){
                        return String.format("%s\t%.2f", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
    // 4
    public static void adventure_developer_revenue(GameOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));

        // get the game id of each line
        PCollection<GameInfo> game_rev = lines.apply(MapElements.via(new SimpleFunction<String, GameInfo>() {
            @Override
            public GameInfo apply(String input) {
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return new GameInfo(tokens[3], tokens[6], Double.parseDouble(tokens[10]));
            }
        }));
        // filter out adventure games "Adventure"
        PCollection<GameInfo> adventure_game_rev = filterByGenre(game_rev,"Adventure");

        PCollection<KV<String, Double>> game_rev_count = adventure_game_rev
                .apply(
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                                .via((GameInfo ginfo) -> KV.of(ginfo.getGameId(), ginfo.getGame_revenue())))
                .apply(Sum.doublesPerKey());
        // count the revenue for each game id
        //PCollection<KV<String, Long>> gameNumber = gameId.apply(Count.perElement());
        game_rev_count
                .apply(MapElements.via(new SimpleFunction<KV<String, Double>, String>() {
                    @Override
                    public String apply(KV<String, Double> input){
                        return String.format("%s\t%.2f", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }

    // ============================ part II ===================================
    // ============================ part II ===================================
    // ============================ part II ===================================
    // ============================ part II ===================================
    /*
    static class PurchasePair{
        @Nullable String game_from;
        @Nullable String game_to;
        public PurchasePair(){}
        public PurchasePair(String game_from, String game_to){
            this.game_from = game_from;
            this.game_to = game_to;
        }
        public PurchasePair(String game_from){
            this.game_from = game_from;
            this.game_to = null;
        }
        public String getGame_from(){
            return this.game_from;
        }
        public String getGame_to(){
            return this.game_to;
        }
        public boolean equals(PurchasePair another){
            if (this.game_from == another.game_from && this.game_to==another.game_to) return true;
            return false;
        }

    }

     */
    @DefaultCoder(AvroCoder.class)
    static class purchaseRecord{
        @Nullable String purchase_key;
        @Nullable String purchase_game;
        public purchaseRecord(){}
        public purchaseRecord(String key, String game) {
            this.purchase_game = game;
            this.purchase_key = key;
        }

        public String getPurchase_game() {
            return purchase_game;
        }
        public String getPurchase_key(){
            return purchase_key;
        }
    }


    public static void countPair(GameOptions options){
        Pipeline pipeline = Pipeline.create(options);
        // read from inputFile
        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from(options.getInputFile()));
        // group games in the same purchase
        PCollection<purchaseRecord> purchase = lines.apply(MapElements.via(new SimpleFunction<String, purchaseRecord>() {
            @Override
            public purchaseRecord apply(String input) {
                String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                return new purchaseRecord(tokens[8]+tokens[11], tokens[0]);
            }
        }));
        PCollection<KV<String, Iterable<String>>> group_by_purchase = purchase
                .apply(
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((purchaseRecord input) -> KV.of(input.getPurchase_key(), input.getPurchase_game())))
                .apply(GroupByKey.create());
        // transform games in one purchase to lists of pairs
        PCollection<List<List<String>>> record_by_pair = group_by_purchase.apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, List<List<String>>>() {
            @Override
            public List<List<String>> apply(KV<String, Iterable<String>> input) {
                Iterable<String> games_iter = input.getValue();
                List<List<String>> result = new ArrayList<>();
                List<String> games =
                        StreamSupport.stream(games_iter.spliterator(), false)
                                .collect(Collectors.toList());
                int len = games.size();
                if(len == 1) result.add(new ArrayList<>(Arrays.asList(games.get(0),"")));
                else{
                    for(int i=0; i<len; i++){
                        String curr_game = games.get(i);

                        for(int j=0; j<len; j++) {
                            String next_game = games.get(j);
                            if(j!=i) result.add(new ArrayList<>(Arrays.asList(games.get(i), games.get(j))));
                        }
                    }
                }
                return result;
            }
        }));
        // count pairs
        PCollection<List<String>> final_pairs = record_by_pair.apply(Flatten.iterables());
        PCollection<KV<List<String>, Long>> pair_count = final_pairs.apply(Count.perElement());
        // change the form of pair_count, make game1 to be the key and group by game_from
        PCollection<KV<String, Iterable<KV<String, Long>>>> pair_grouped = pair_count.apply(MapElements.via(new SimpleFunction<KV<List<String>, Long>, KV<String, KV<String, Long>>>() {
            @Override
            public KV<String, KV<String, Long>> apply(KV<List<String>, Long> input) {
                return KV.of(input.getKey().get(0), KV.of(input.getKey().get(1), input.getValue()));
            }
        })).apply(GroupByKey.create());

        // get the most frequent pair for a game
        PCollection<KV<KV<String, Long>, List<String>>> pair_most_frequent = pair_grouped.apply(MapElements.via(new SimpleFunction<KV<String, Iterable<KV<String, Long>>>, KV<KV<String, Long>, List<String>>>() {
            @Override
            public KV<KV<String, Long>, List<String>> apply(KV<String, Iterable<KV<String, Long>>> input) {
                Long max = new Long(0);

                List<String> res = new ArrayList<>();
                for(KV<String, Long> target_number : input.getValue()) {
                    Long cur = target_number.getValue();
                    if(!target_number.getKey().equals("") && cur>max) {
                        res.clear();
                        max = cur;
                        res.add(target_number.getKey());
                    }
                    else if(cur == max && !target_number.getKey().equals("")) {
                        res.add(target_number.getKey());
                    }
                }
                return KV.of(KV.of(input.getKey(), max), res);
            }
        }));
        pair_most_frequent
                .apply(MapElements.via(new SimpleFunction<KV<KV<String, Long>, List<String>>, String>() {
                    @Override
                    public String apply(KV<KV<String, Long>, List<String>> input){
                        String game1 = input.getKey().getKey();
                        Long num = input.getKey().getValue();
                        if(num == 0) {
                            return String.format("%s\tNone\t0", game1);
                        }
                        String[] arr = input.getValue().stream().toArray(String[] ::new);
                        Arrays.sort(arr, new Comparator<String>() {
                            @Override
                            public int compare(String o1, String o2) {
                                String o11 = o1.substring(2, o1.length());
                                String o22 = o2.substring(2,o2.length());
                                Long o1_1 = Long.parseLong(o11);
                                Long o2_2 = Long.parseLong(o22);
                                Long res = o1_1 - o2_2;
                                if(res>Long.valueOf(0)) return 1;
                                else if(res == Long.valueOf(0)) return 0;
                                else return -1;
                                
                            }
                        });
                        String game2 = StringUtils.join(arr, "\t");
                        return String.format("%s\t%s\t%s", game1, game2, num);
                    }
                }))
                .apply(TextIO.write().withoutSharding().to(options.getOutput()).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();

    }









    //------------------------ main function ----------------------------------


    public static void main(String[] args){
        GameOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GameOptions.class);
        String operation = options.getOperation();
        if(operation.equals("games_purchased")) games_purchased(options);
        else if(operation.equals("game_revenue")) game_revenue(options);
        else if(operation.equals("developer_purchased")) developer_purchased(options);
        else if(operation.equals("developer_revenue")) developer_revenue(options);
        else if(operation.equals("adventure_games_purchased")) adventure_game_purchased(options);
        else if(operation.equals("adventure_developer_purchased")) adventure_developer_purchased(options);
        else if(operation.equals("adventure_game_revenue")) adventure_game_revenue(options);
        else if(operation.equals("adventure_developer_revenue")) adventure_developer_revenue(options);
        else if(operation.equals("countPair")) countPair(options);
    }
}
