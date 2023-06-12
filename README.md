# SQL and NoSQL Database Benchmarking for Image Payload

This project aims to benchmark several relational and non-relational databases when directly reading and storing binary data in the form of images. All tests were run in AWS machines, using official docker images when available.

This also serves as our graduation's final project.

## Authors

- Lucas Monteiro Miranda
- Caio Alessandri Albuquerque

## Usage

Inside the database's corresponding folder, there is a python script used inside each database's docker.

Inside the images folder is the payload used in tests, as well as each of the three sorting patterns used.

## Databases used

- SQL
    - MariaDB
    - Microsoft SQL Server
    - MySQL
    - PostgreSQL

- NoSQL
    - Amazon S3
    - Cassandra
    - CouchDB
    - DynamoDB
    - KuzuDB
    - MongoDB
    - OrientDB
    - Redis
    - Solr

## License

[GNU GPL-3.0](https://www.gnu.org/licenses/gpl-3.0.html)
