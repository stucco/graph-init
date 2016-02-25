#!/bin/sh

echo "Checking out modules..."
mvn -q --non-recursive scm:checkout -Dmodule.name=graph-db-connection -Dbranch.name=master
git clone -b master https://github.com/stucco/ontology.git
cp ontology/stucco_schema.json .
cp graph-db-connection/*.yml .

echo "Building graph-init..."
mvn clean install -Dmaven.test.skip=true
cd schema
mvn clean package -Dmaven.test.skip=true
cd ..

echo "Cleaning up...."
rm -rf graph-db-connection
rm -rf ontology

