# DataAnalyser

This project use Scala (with sbt), Spark and MLib to predict customers clicks on advertising.

## Files structure

- `artefact`: contains the compiled jars
    1. `oracle.jar`: the jar to predict new customers clicks
    2. `model.jar`: the jar to build the model
- `common`: contains the common code such as data cleaning stuff
- `model`: contains the code to build the model
- `oracle`: contains the code to predict new customers clicks

## How to use it

This repo comes with a prebuilt model. 
You should have at least Java 1.8.

1. Download or clone this repo  
    `git clone https://github.com/bern-ads/DataAnalyser`
2. Go into the repo  
    `cd DataAnalyser`
3. Run the `oracle.jar` in the artifacts directory  
    `java -jar out/artifacts/oracle_jar/oracle.jar /home/yves/Téléchargements/data-students.json /home/yves/Bureau/DataAnalyser/bernads.spark.selector /home/yves/Bureau/DataAnalyser/bernads.spark.model
 [result_file_name.csv]`
  
Note: if you want to generate a new model you should run Create the model  
    `java -jar model.jar <labeled_source_data_path.json> [model_file_name]`