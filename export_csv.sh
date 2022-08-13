#!/bin/bash

mongoexport --db=test --collection=insert_results --type=csv --out=insert_results.csv --fields=collectionName,vectorSize,vectorCount,bulkSize,randomVectorCount,startTime,endTime,durationMs,storageSize

mongoexport --db=test --collection=query_results --type=csv --out=query_results.csv --fields=collectionName,vectorSize,vectorCount,bulkSize,randomVectorCount,calculation,sorting,top,resultCount,startTime,endTime,durationMs,storageSize