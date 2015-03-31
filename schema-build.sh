#!/bin/sh

echo "Checking out modules..."
mvn -q --non-recursive scm:checkout -Dmodule.name=rexster-client-java -Dbranch.name=master
git clone -b forIndexing https://github.com/stucco/ontology.git
cp ontology/stucco_schema.json .

echo "Building graph-init..."
mvn clean install -Dmaven.test.skip=true
cd schema
mvn clean package -Dmaven.test.skip=true
cd ..

echo "Cleaning up...."
rm -rf rexster-client-java
rm -rf ontology

