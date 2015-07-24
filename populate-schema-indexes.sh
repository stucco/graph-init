#!/bin/sh

echo "executing specification load and indexing..."
java -jar schema/target/populate_specification.jar $1
