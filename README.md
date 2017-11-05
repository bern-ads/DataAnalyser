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

1. Create the model  
    `java -jar model.jar <labeled_source_data_path.json> [model_file_name]`
2. Execute the model  
    `java -jar oracle.jar <unlabeled_new_data_path.json> <path_to_model_file> [result_file_name.csv]`