## 1. Run Docker inside `topic-builder-dev` project

    If tables does not exist, run `topic-builder-dev` project and create tables

    * docker-compose -p up topic-builder-dev -d

## 2. Initialize DB

    * python -m scripts.create_propbank_table
