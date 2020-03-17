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





