#!/bin/sh

echo "Checking out modules..."
mvn -q --non-recursive scm:checkout -Dmodule.name=rexster-client-java -Dbranch.name=master
git clone -b forIndex https://github.com/stucco/ontology.git

echo "Building graph-init..."
mvn clean install -Dmaven.test.skip=true
cd schema
mvn clean package -Dmaven.test.skip=true
cd ..
