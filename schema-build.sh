#!/bin/sh

echo "Checking out modules..."
mvn -q --non-recursive scm:checkout -Dmodule.name=rexster-client-java -Dbranch.name=master
git clone -b master https://github.com/stucco/ontology.git
cp ontology/stucco_schema.json .

echo "Building graph-init..."
mvn -X -e clean install -Dmaven.test.skip=true
cd schema
mvn -X -e clean package -Dmaven.test.skip=true
cd ..

echo "Cleaning up...."
rm -rf rexster-client-java
rm -rf ontology

