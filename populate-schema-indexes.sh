#!/bin/sh

echo "executing schema load and indexing..."
java -jar schema/target/populate_schema.jar -schema .
