## Spark streaming ETL

This project aims to create ETL configurations via spark streaming using lighbend configuration library, so that an end user can configure a simple ETL process without knowledge of Scala/Spark. It's a side project that I'm doing to test spark structured streaming potential.

The project aims to be pluggable, so that everyone can extend it adding new sources, sinks and transformations. I want to implement a ElasticSearch sink in the immediate future and test kafka sink/sources performance.

The spark app takes two simple string parameters at runtime: source and sink. I will add new sources and sinks over time. 
The sources and sinks can be configured via the *application.conf* file, using HOCON syntax.

I will explain usage with a real configuration.
This examples takes csv files and writes json files, applying a schema given from the user and adding a column represeting process time. 

```
sources {

  //example of configuration for csv files
  file {
    required {
      path = "/home/stefano/Documents/SPARKINGEST/*.csv"
      format = "csv"
    }
    optional {
      refreshTime = 1 
      sep = ","
    }
    schema = [
      {name: string},
      {surname: string}
      {age: int}
    ]
  }
}

sinks {
  file {
    required {
      path = "file:///home/stefano/Documents/SPARKINGEST"
      format = "json"
    }
    optional {
      outputMode = "append"
    }
  }
}

//transformation are all optionals
transformations {
  date {
    column="sparkProcessTime"
  }
}
```

Every source or sink needs some required parameters. You can figure them out by looking at the [spark documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) for the given component.

I will add documentation in the future for custom features, like transformations. However writing documents is boring, while coding is fun..
Translated: I may _never_ write documentation, so PM me if you need something, I will be happy to text back. 

You can find every feature available in the application.conf file in the resources folder, i will keep it updated.
