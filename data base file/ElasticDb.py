import os
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers ,ConnectionTimeout
from requests_aws4auth import AWS4Auth

class ElasticDb:
    def __init__(self,type="google"):
        self.host_name_google = "searmazonaws.com"
        self.host_name_user = "search-oamazonaws.com"
        self.host_name_ubuylog = "seaonaws.com"

        self.region = 'eu--1'
        self.access_key = ""
        self.access_secret = "wB5Ecwfk98GzirSvxQHp9fILOr54tZG0wpcNdgz7"
        self.service = "es"
        self.connection_type = type
        self.es_connect = None
        self.getConnect()

    def getConnect(self):
        print(f"self.connection_type=={self.connection_type}")
        if self.connection_type:
            if self.connection_type == "google":
                host = self.host_name_google
            elif self.connection_type == "ubuylog":
                host = self.host_name_ubuylog
            else:
                host = self.host_name_user
            if self.es_connect is None:
                os.environ['AWS_DEFAULT_REGION'] = self.region
                os.environ['AWS_ACCESS_KEY_ID'] = self.access_key
                os.environ['AWS_SECRET_ACCESS_KEY'] = self.access_secret
                credentials = boto3.Session().get_credentials()
                awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, self.region, self.service,
                                   session_token=credentials.token)
                self.es_connect = Elasticsearch(
                    hosts=[{'host': host, 'port': 443}],
                    http_auth=awsauth,
                    use_ssl=True,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True
                )
                print("ubuylog elastic connected")

    def makeDataForInsert(self,index_name, product_data, bulk_insert_data_elastic):
        product_data_insert = dict(product_data)
        product_data_insert["_op_type"] = "create"
        product_data_insert["_index"] = index_name
        bulk_insert_data_elastic.append(product_data_insert)
        product_data = {}

    def makeDataForUpdate(self,index_name, id, product_data, update_list):
        update_query_data = {
            "_op_type": 'update',
            "_index": index_name,
            "_id": id,
            "doc": product_data
        }
        update_list.append(update_query_data)

    def makeDataForUpdate(self,index_name, id, product_data, update_list):
        update_query_data = {
            "_op_type": 'update',
            "_index": index_name,
            "_id": id,
            "doc": product_data
        }
        update_list.append(update_query_data)

    def makeDataForUpsert(self,index_name, id, product_data, update_list):
        update_query_data = {
            "_op_type": 'update',
            "_index": index_name,
            "_id": id,
            "doc": product_data,
            "doc_as_upsert": True
        }
        update_list.append(update_query_data)


    def get_product_mapping(self):
        mapping = {
            "settings": {
                "number_of_shards": 30,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "brand_filter": {
                        "properties": {
                            "key": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "value": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            }
                        }
                    },
                    "breadcrumbs": {
                        "type": "keyword",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "category": {
                        "properties": {
                            "level1": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            "level2": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            "level3": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            "level4": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            "level5": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            "level6": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            "level7": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            "level8": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            },
                            "level9": {
                                "properties": {
                                    "name": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    },
                                    "node_id": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 256
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "created": {
                        "type": "long"
                    },
                    "customer_rating": {
                        "type": "float"
                    },
                    "entity_id": {
                        "type": "integer"
                    },
                    "filter_data": {
                        "type": "nested",
                        "properties": {
                            "filter_id": {
                                "type": "integer"
                            },
                            "filter_text": {
                                "type": "keyword"
                            }
                        }
                    },
                    "is_prime": {
                        "type": "integer"
                    },
                    "is_in_stock": {
                        "type": "integer"
                    },
                    "is_price_range": {
                        "type": "integer"
                    },
                    "keywords": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "original_price": {
                        "type": "long"
                    },
                    "price": {
                        "type": "float"
                    },
                    "price_range_from": {
                        "type": "float"
                    },
                    "price_range_to": {
                        "type": "float"
                    },
                    "product_image": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "product_title": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "slug": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "ubuy_price": {
                        "type": "float"
                    },
                    "ubuy_original_price": {
                        "type": "float"
                    },
                    "ubuy_sku": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "ubuy_store": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "ubuy_substore": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "updated": {
                        "type": "long"
                    },
                }
            }
        }
        return mapping

    def fireBulkQuery(self,query):
        try:
            data = helpers.bulk(self.es_connect, query)
        except helpers.BulkIndexError as exception:
            exceptions_ids = []
            print(exception)
            exp_data = exception.args[1]
            print("exception======fireBulkQuery===" + str(exception.args[0]))
            if exp_data:
                print("exp_data========================"+str(type(exp_data)))


    def create_index(self,product_index,mapping=None):
        if not self.es_connect.indices.exists(index=product_index):
            if mapping is not None:
                res = self.es_connect.indices.create(index=product_index, ignore=400, body=mapping)
            else:
                res = self.es_connect.indices.create(index=product_index, ignore=400)
        else:
            print("index already exist")

    def getData(self,product_index,query):
        return_data = []
        try:
            return_data = self.es_connect.search(index=product_index,body=query)
        except (ConnectionTimeout , Exception) as e:
            print(e)
            return_data.append(e)
        return return_data

    def getCount(self, product_index, query):
        return_data = {}
        try:
            return_data = self.es_connect.count(index=product_index, body=query)
        except Exception as e:
            print(e)
        return return_data