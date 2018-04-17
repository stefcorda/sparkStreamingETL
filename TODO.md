TODO (random order):
1. now required params are applied twice: in fold left and manually sometimes. AVOID
2. look how to handle dots in HOCON
3. With kafka source and kafka sink the original kafka message is inside "value", flatten that json  object -> SEEMS NOT POSSIbLE IN STRUCTURED STREAMING: i would need to pass a schema. Option is to stream a simple RDD[String]. Will think.
4. cast types appropriately in the add function, now they are always strings.
5. add support for stream join 