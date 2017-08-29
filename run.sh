mvn package install;
$SPARK_HOME/bin/spark-submit --class "com.shyam.sparkimagevisualizer.ImageVisualizer" --master local[2] /home/shyam/workspace/sparkimagevisualizer/target/sparkimagevisualizer-1.0.jar
