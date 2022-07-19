## Building the project

1. Unpack the archive provided
2. In a console window go to the project folder you have created in the previous step
3. Run the `sbt 'set assembly/assemblyOutputPath := new File("app.jar")' assembly` command
4. Review the output and make sure the project was built successfully.
5. A file `app.jar` should be generated in the project folder.

## Running the application
To run the application, use the following command in a console:

`java -jar app.jar --out outfile.json`

To view all options available invoke

`java -jar app.jar`



