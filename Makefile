MVN=mvn
CLEAN_INSTALL_COMPILE=clean install compile
EXEC_PROD=exec:java -Dexec.mainClass="com.kafka.ProdMain"
EXEC_CONS=exec:java -Dexec.mainClass="com.kafka.ConsMain"

all: build

build:
	$(MVN) $(CLEAN_INSTALL_COMPILE)

prod:
	$(MVN) $(EXEC_PROD)

cons:
	$(MVN) $(EXEC_CONS)

clean:
	$(MVN) clean


