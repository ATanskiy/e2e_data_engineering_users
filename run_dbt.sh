#!/bin/bash
docker exec dbt sh -c "cd /usr/app/dbt && dbt run --debug"