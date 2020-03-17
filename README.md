# 1

# 2 Analyze Purchasing Data with Google Cloud Dataflow pipelines (Java)
## 2.1 Intro
This code uses Apache Beam the SDK, it contains the functions of: 
1. Count and output the total number of purchased copies for each game that appears in the data set. 
2. Calculate and output the total revenue for each game. 
3. Count and output the total number of purchased copies for each game developer that appears in the data set. 
4. Calculate and output the total revenue for each developer.  
5. Repeat Items 1-4 for games in Adventure genre (or any other one by changing the parameter). 
6. For each game that was purchased at least once, find the other game that was most often purchased in the same transaction and count how many times the two games were purchased together.
 The output is in the form of: <game_index_1> \t <game_index_2> \t <times_purchased_together>

## 2.2 Run
You will need to install apache maven first to establish vitual environment. The pom.xml file is already set in the given project file, so please excute this code from the project directory or establish another project using this code and template. The command examples are:


on cloud:
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.GameCount      -Dexec.args="--runner=DataflowRunner --project=<PROJECT_ID>(e.g., qiaoling-chen-dist-sys-hw2) --inputFile=<INPUT_FILE>(e.g.,gs://storing_bucket/video_games.csv) --output=<OUTPUT_PATH>(e.g.,gs://qiaolingoutput/games_numbers) --operation=<OPERATION>(e.g.,games_purchased)" -Pdataflow-runner

local:
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.GameCount      -Dexec.args="--runner=DirectRunner --inputFile=<INPUT_FILE>(video_games_short.csv )--output=<OUTPUT_FILE>(gameCountTest) --operation=<OPERATION>(e.g.,games_purchased)" 

Operations:
1. game_numbers: games_purchased
2. game_revenue: game_revenue
3. developer_numbers: developer_purchased
4. developer_revenue: developer_revenue
5. adventure_numbers: adventure_games_purchased
6. adventure_revenue: adventure_game_revenue
7. adventure_developer_numbers: adventure_developer_purchased
8. adventure_developer_revenue: adventure_developer_revenue
9. most_purchased_together: countPair
Page to download input data set: https://drive.google.com/drive/folders/1GXBymo9hVo6h3D8pH4r9xhRvkYDsO3wq
