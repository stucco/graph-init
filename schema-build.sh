#!/bin/sh

echo "Checking out modules..."
mvn -q --non-recursive scm:checkout -Dmodule.name=graph-db-connection -DscmVersion=1.0.0 -DscmVersionType=tag

echo "Building graph-init..."
mvn clean install -Dmaven.test.skip=true
cd schema
mvn clean package -Dmaven.test.skip=true
cd ..

echo "Cleaning up...."
rm -rf graph-db-connection

