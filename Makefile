MVN=mvn
CLEAN_INSTALL_COMPILE=clean install compile
EXEC_PROD=exec:java -Dexec.mainClass="com.kafka.ProdMain"
EXEC_CONS=exec:java -Dexec.mainClass="com.kafka.ConsMain"
EXEC_CONS_MOY=exec:java -Dexec.mainClass="com.kafka.ConsMoyenne"
STREAM=exec:java -Dexec.mainClass="com.kafka.TraitementTemperature"

build:
	$(MVN) $(CLEAN_INSTALL_COMPILE)

prod:
	$(MVN) $(EXEC_PROD)


stream: 
	$(MVN) $(STREAM)

cons:
	$(MVN) $(EXEC_CONS)

consMoy:
	$(MVN) $(EXEC_CONS_MOY)

clean:
	$(MVN) clean


