#!/usr/bin/env bash

java -cp target/hyperlink-tools-1.0-SNAPSHOT.jar org.hyperlink.Import "$@"

#java -cp target/hyperlink-tools-1.0-SNAPSHOT.jar org.hyperlink.Import -D -c src/test/resources/import.properties --nodes=person=data/v_person.csv --relationships=data/edges.csv --edgeLabels=KNOWS,MET


#./bin/bulk-loading.sh -D --threads=4 -c src/test/resources/import.properties \
#--nodes=actor=data/movie/actor.csv \
#--nodes=director=data/movie/director.csv \
#--nodes=category=data/movie/category.csv \
#--nodes=movie=data/movie/movie.csv \
#--edgeLabels=BELONGS,ACT,DIRECT \
#--relationships=data/movie/edge_belongs.csv \
#--relationships=data/movie/edge_acted.csv \
#--relationships=data/movie/edge_directed.csv

