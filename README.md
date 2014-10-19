Skyline
=======

Instructions on how to reproduce the results-

1)	First create an input directory folder in your Hadoop file system.
Hadoop fs  –mkdir <input-dir>
 
2)	Copy the input data file which I have attached “gsod_aggregated.out” and copy it to your input directory folder.

3)	I have attached one executable jar file named “Skyline-1.0-SNAPSHOT.jar”

4)	Command line parameters to be passed are number of reducers, input directory and output directory.

hadoop  jar Skyline-1.0-SNAPSHOT.jar   edu.cs236.skyline.Skyline  <no_reducers>  <Input-dir>  <Output-Dir>

number of reducers can be 1, 2 or 4.
edu.cs236.skyline.Skyline is the class name.

Eg- hadoop  jar  Skyline-1.0-SNAPSHOT.jar   edu.cs236.skyline.Skyline   4  Input   Output

5)	I have also attached all the source files and the result file file which contains all the skyline points created in the final result round by removing all the dominated points at all the levels of the tree.

6)	This project was done on maven and I have also attached the pom.xml file.

7)	In case we have 4 levels of the tree our final reducer result would be stored in Output directory appended by a variable x .We will have 4 Output directories Output0, Output1, Output2, Output3. Final skyline result would be stored in Output3/part-r-00000

8)	All the remaining folders Output0, Output1 and Output2 would contain intermediate results of map reduce rounds.
