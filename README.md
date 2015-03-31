This project is used to install a schema within titan so that indexes based on the ontology can be built.  Therefore before any content is added the titan index this process will need to be run.  Else the user will need to perform reindxing of the titan and elastic search indexes.

# Setup for Testing with Titan in-memory instance

First, make sure that Titan is installed, and that the Titan path is correctly specified at the beginning of the `start-titan-test-server.sh` and `stop-titan-test-server.sh` scripts. Typically titan is installed at /usr/local/titan<version>.


The test server can be launched with `sudo ./start-titan-test-server.sh > /dev/null` and stopped with `sudo ./stop-titan-test-server.sh` .

# Running the tests

You must first run the schema-build.sh script before you run the tests as there are files that will be brought into this environment.

Once the test server is up, the tests can be run with:
