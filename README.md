# es
ES wrapper for bulk insert, search,etc.

We grow tired of writing repetitive code for ES. Hence this repos.
This also makes us aware and ready for 2.0.0 which deprecates Rivers

# Todo
 - Bulk insert should be able to have bulk threads configurable before insertion, keep bulk active + queue in control 
 - Search and bulk should be able to understand responses
 - Bull insert should have tcp no_delay type of mode where bulk requests should be buffered or NOT
 - ORM layer search  ( if possible)
 - CLI option for cluster reallocation stopping and starting , threadpool control, node close , and IF possible graceful restart of a node
 - 


# How it works
- We connect to ES using HTTP

# How to use

# Paginate
```

var esOpts = {
  
  /* These opts are what is passed as is to Request HTTP Module */
  requestOpts : {
    uri : "http://localhost:9200"
  }

};

// Create an instance of ES Object which will be used everywhere
var esObject = new es(esOpts);

esObject.setPaginateOpts(uri, queryBody, pageSize);


```

# Bulk commands
