What's this?
============

[elasticsearch-SQL](https://github.com/NLPchina/elasticsearch-sql/) is a great tool but it provides only Web frontend. `sql-on-es-cli.py` is a simple tool written in Python. It just reads SQL from user and requests the SQL to `elasticsearch-SQL` and parse Elasticsearch output and print output 2 dimentional table format like this:

```
SQL> SELECT zipcode, area_depth1 FROM jsheo LIMIT 5
| _id                  | _type   | area_depth1 | zipcode |
|----------------------|---------|-------------|---------|
| AVELB3nkuplgpa0tYzi6 | zipcode |  Gangwon-do |  210821 |
| AVELB3npuplgpa0tYzi8 | zipcode |  Gangwon-do |  210823 |
| AVELB3nruplgpa0tYzi9 | zipcode |  Gangwon-do |  210823 |
| AVELB3nzuplgpa0tYzjB | zipcode |  Gangwon-do |  210822 |
| AVELB3n2uplgpa0tYzjD | zipcode |  Gangwon-do |  210824 |

5 rows printed
2044 docs hitted (0.007 sec)
```

Limitations
===========

- Only tested with Elasticseach 1.7
- Only tested with primitive data types

Installation
============

Install elasticsearch-SQL
-------------------------

1. install [elasticsearch-SQL](https://github.com/NLPchina/elasticsearch-sql/) into your Elasticsearch

Install CLI
-----------

clone my sql-on-es-cli.py
```
$ git clone https://github.com/mysqlguru/sql-on-es-cli.git
```

How to use
==========

Table Output
------------

```
$ cd sql-on-es-cli/
$ ./sql-on-es-cli.py http://localhost:9200/
SQL> SELECT area_depth1, area_depth2, area_depth3, zipcode FROM jsheo LIMIT 5
| _id                  | _type   | area_depth1 | area_depth2  | area_depth3    | zipcode |
|----------------------|---------|-------------|--------------|----------------|---------|
| AVELB3nkuplgpa0tYzi6 | zipcode |  Gangwon-do | Gangneung-si | Gangdong-myeon |  210821 |
| AVELB3npuplgpa0tYzi8 | zipcode |  Gangwon-do | Gangneung-si | Gangdong-myeon |  210823 |
| AVELB3nruplgpa0tYzi9 | zipcode |  Gangwon-do | Gangneung-si | Gangdong-myeon |  210823 |
| AVELB3nzuplgpa0tYzjB | zipcode |  Gangwon-do | Gangneung-si | Gangdong-myeon |  210822 |
| AVELB3n2uplgpa0tYzjD | zipcode |  Gangwon-do | Gangneung-si | Gangdong-myeon |  210824 |

5 rows printed
2044 docs hitted (0.009 sec)

SQL> SELECT zipcode, COUNT(*) FROM jsheo GROUP BY zipcode LIMIT 10
| zipcode | COUNT(*) |
|---------|----------|
|  219839 |       16 |
|  200829 |       10 |
|  215852 |        9 |
|  220802 |        9 |
|  200959 |        8 |
|  230805 |        8 |
|  252829 |        8 |
|  215801 |        7 |
|  215821 |        7 |
|  215842 |        7 |

10 rows printed
2044 docs hitted (0.004 sec)
```

Json Output Format
------------------

- use `-j` option so that print Elasticsearch's Json output

```
$ ./sql-on-es-cli.py -j http://localhost:9200/
SQL> SELECT zipcode, area_depth1 FROM jsheo limit 2
{
  "hits": {
    "hits": [
      {
        "_score": 1.0, 
        "_type": "zipcode", 
        "_id": "AVELB3nkuplgpa0tYzi6", 
        "_source": {
          "area_depth1": "Gangwon-do", 
          "zipcode": "210821"
        }, 
        "_index": "jsheo"
      }, 
      {
        "_score": 1.0, 
        "_type": "zipcode", 
        "_id": "AVELB3npuplgpa0tYzi8", 
        "_source": {
          "area_depth1": "Gangwon-do", 
          "zipcode": "210823"
        }, 
        "_index": "jsheo"
      }
    ], 
    "total": 2044, 
    "max_score": 1.0
  }, 
  "_shards": {
    "successful": 5, 
    "failed": 0, 
    "total": 5
  }, 
  "took": 2, 
  "timed_out": false
}
```
