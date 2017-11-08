# DataAnalyser

This project use Scala (with sbt), Spark and MLlib to predict customers clicks on advertising.

## Files structure

- `artifacts`: contains the compiled jars
    1. `oracle.jar`: the jar to predict new customers clicks
    2. `model.jar`: the jar to build the model
- `common`: contains the common code such as data cleaning stuff
- `model`: contains the code to build the model
- `oracle`: contains the code to predict new customers clicks

## How to use it

This repo comes with a prebuilt model. 
You should have: 
- Java 1.8;
- Scala 2.11.11;
- sbt 1.0.3;
- Spark 2.2.0.

1. Download or clone this repo  
    `git clone https://github.com/bern-ads/DataAnalyser`
2. Go into the repo  
    `cd DataAnalyser`
3. Go to the `artifacts` directory
    `cd artifacts`
4. Submit the spark job
    `spark-submit --class "oracle.Main" --master local oracle.jar "<path_to_the_data_file>" "<path_to_selector_directory>" "<path_to_model_directory>"` 

You can find `<path_to_selector_directory>` at the root the repository.
You can find `<path_to_model_directory>` at the root the repository.
So the command should look like this:
    `spark-submit --class "oracle.Main" --master local oracle.jar "<path_to_the_data_file>" "../bernads.spark.selector" "../bernads.spark.model"` 
  
