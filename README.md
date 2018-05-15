## Spark streaming ETL

This project aims to create ETL configurations via spark streaming using lighbend configuration library, so that an end user can configure a simple ETL process without knowledge of Scala/Spark. It's a side project that I'm doing to test spark structured streaming potential.
Recently I added support to listeners, that monitor state of the spark streaming application and write metrics.

The project aims to be pluggable, so that everyone can extend it adding new sources, sinks and transformations. 

##### Current supported sources:
_I will look at streams joins in the immediate future_
1. File
2. Kafka
3. Joinables


##### Current supported sinks:
1. File
2. Kafka
3. Elastic Search
4. Console

##### Current supported transformations:
_all transformations can be applied sequentially and are optional_
1. watermark support
2. hard fixed values
3. filters
4. selects
5. column removal
6. counts

##### Current supported listeners:
1. kafka
2. file

The sources and sinks can be configured via the *application.conf* file, using HOCON syntax.

I will explain usage with a real configuration. (Conf file is simplified)
This examples takes csv files and writes them to Elastic Search, applying a schema given from the user and adding a column represeting process time. It also writes to file metrics about the state of the process 

```
application {
  checkpointLocation = "file:///home/stefano/Documents/SPARKINGEST/checkpoints"
}

sources {

  sourceToApply = "file"

  //example of configuration for csv files
  file {
    required {
      path = "/home/stefano/Documents/SPARKINGEST/*.csv"
      format = "csv"
    }
    optional {
      refreshTime = 1 //check every X ms for new files
      sep = ","
    }
    //IF SCHEMA IS PRESENT, IT WILL BE TAKEN
    schema = [
      {name: string},
      {surname: string}
      {age: int}
    ]
  }
}

sinks {

  sinkToApply = "es"

  es {
    index = "spark"
    type = "people"

    required {}
    optional {}
  }
}

transformations {
  //defined order of transformations to apply
  order = ["date", "watermark"]
  
  date {
    column = "sparkProcessTime"
  }
}

listeners {

  listenersToApply = ["file"]
  file {
    filename = "/home/stefano/Documents/SPARKINGEST/sparkstream.log"
  }

}
```

Source and sink needs can have some optional parameters. You can figure them out by looking at the [spark documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) for the given component. You need to add them in the optional {} and required{} options present in the application.conf file

I want to spend some more words on a new feature I recently added, joinable sources.
One can specify *N* sources, for example 10 file sources, and 9 join conditions, they will be joined sequentially with the order given from the user.
Every source can have its own transformations *before* being joined. 
After all the sources are joined the process will continue transparently and apply its configurated transformations and sinks. 

I will add documentation in the future for custom features, like transformations. However writing documents is boring, while coding is fun..
PM me if you need something, I will be happy to text back. 

You can find every feature available in the application.conf file in the resources folder, i will keep it updated.
