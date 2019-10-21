#!/usr/bin/env bash

java -cp target/bulk-loading-1.0-SNAPSHOT.jar org.beyond.graph.bulk.loading.Import "$@"


#./bin/bulk-loading.sh -D \
# --threads=4 \
# --config=conf/import.properties \
# --remote=conf/remote.yaml \
# --nodes=actor=data/movie/actor.csv \
# --nodes=director=data/movie/director.csv \
# --nodes=category=data/movie/category.csv \
# --nodes=movie=data/movie/movie.csv \
# --edgeLabels=BELONGS,ACT,DIRECT \
# --relationships=data/movie/edge_belongs.csv \
# --relationships=data/movie/edge_acted.csv \
# --relationships=data/movie/edge_directed.csv

