inventory_checker_us:
	mvn compile exec:java@inventory_checker_us

inventory_checker_china:
	mvn compile exec:java@inventory_checker_china

cluster_admin:
	mvn compile exec:java@cluster_admin -DdeleteSchema=false

deleteSchema:
	mvn compile exec:java@cluster_admin -DdeleteSchema=true


orderProducer:
	mvn compile exec:java@order_producer