# spark app demo with maven
## build
	mvn clean package
## submit spark job
	spark-submit --class "SimpleApp" --master spark://hdpmaster:7077 target/scala-demo-1.0-SNAPSHOT.jar
