[![Build Status](https://travis-ci.org/zalando-incubator/spark-json-schema.svg?branch=master)](https://travis-ci.org/zalando-incubator/spark-json-schema)
[![codecov.io](http://codecov.io/github/zalando-incubator/spark-json-schema/coverage.svg?branch=master)](http://codecov.io/github/zalando-incubator/spark-json-schema?branch=master)
[![MIT licensed](https://img.shields.io/badge/license-MIT-green.svg)](https://raw.githubusercontent.com/zalando-incubator/spark-json-schema/master/LICENSE)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/12381b6f4fc34242b2f50178e2ce6eca)](https://www.codacy.com/app/petz2000/spark-json-schema?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=zalando-incubator/spark-json-schema&amp;utm_campaign=Badge_Grade)

# spark-json-schema

The goal of this library is to support input data integrity when loading json data into Apache Spark.
For this purpose the library:
    
- Reads in an existing json-schema file
- Parses the json-schema and builds a Spark DataFrame schema

The generated schema can be used when loading json data into Spark.
This verifies that the input data conforms to the given schema and enables to filter out corrupt input data.
 

# Quickstart

Include the library under the following coordinates:
```scala
libraryDependencies += "org.zalando" %% "spark-json-schema" % "0.6.1"
```
Parse a given json-schema file by providing the path to the input file.
This file should be relative to the resources folder:
```scala
val schema = SchemaConverter.convert("schemaFile.json")
```
Alternatively you can use convertContent with the schema json content as a string.
This means you can use any file loader to access your schema.json file and put the resulting
string into this function. So this file may be stored anywhere:
```
val schema = SchemaConverter.convertContent(<schema json as string>)
```
Use the created schema when loading json-files into Spark:
```scala
val dataFrame = sqlContext.read.schema(schema).json(source)
```
Jsons that are not according to the schema have only `null` in the dataFrame fields and can be filtered out:
```scala
val filteredData = dataFrame.rdd.filter( x => Range(0, x.length).contains(!x.isNullAt(_)))
```
After these steps you can be sure that all files that were loaded for further processing comply to your schema.

# JSON

The root of the schema json is required to be type 'object' and to have a field 'properties'
having the contents of the schema. Currently supported types are:
"string", "number" (double), "float", "integer" (long), "boolean", "object" and "array"

The root level may have a field called 'definitions' having additional structures. These
structures can be referenced in the schema. This might be useful, if you have certain
objects that are used multiple times. In order to use such a reference in the schema 
the corresponding field name should be '$ref'. The value is the address in the
definitions, i.e. the path following the first occurrence of 'definitions/' will be
applied on the definitions field. So a reference will look like:

    "$ref": "definitions/path/to/struct"

# Contact

Feel free to contact us at team-payana@zalando.de or to create a pull request. We welcome community contributions. Please read [CONTRIBUTING](CONTRIBUTING.md) for more information.

# License

This software is licensed under the MIT license (see below), unless otherwise stated in the license files in higher directories (if any).

The MIT License (MIT) Copyright © 2016 Zalando SE, https://tech.zalando.com

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
