
### Build Instructions

1. Navigate to the project directory:

```sh
cd ConnectNestedSMT
```

2. Build the project using Maven:

```sh
mvn clean package
```

3. The JAR file will be generated in the `target/` directory.

4. Add the JAR to your Kafka Connect plugins path.

5. Deploy a connector with the SMT:

```
    "transforms": "NestedField",
    "transforms.NestedField.type": "com.github.cdpop.smt.NestedFieldTransform",  
    "transforms.NestedField.delimiter": "_",
```
