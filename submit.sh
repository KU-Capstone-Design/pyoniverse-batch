#!/bin/bash
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 "$@"
