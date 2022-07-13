<img alt="logo" src="http://img.jcabi.com/logo-square.png" width="64px" height="64px" />

[![EO principles respected here](https://www.elegantobjects.org/badge.svg)](https://www.elegantobjects.org)
[![DevOps By Rultor.com](http://www.rultor.com/b/jcabi/jcabi-dynamo)](http://www.rultor.com/p/jcabi/jcabi-dynamo)
[![We recommend IntelliJ IDEA](https://www.elegantobjects.org/intellij-idea.svg)](https://www.jetbrains.com/idea/)

[![mvn](https://github.com/jcabi/jcabi-dynamo/actions/workflows/mvn.yml/badge.svg)](https://github.com/jcabi/jcabi-dynamo/actions/workflows/mvn.yml)
[![PDD status](http://www.0pdd.com/svg?name=jcabi/jcabi-dynamo)](http://www.0pdd.com/p?name=jcabi/jcabi-dynamo)
[![Javadoc](https://javadoc.io/badge/com.jcabi/jcabi-dynamo.svg)](http://www.javadoc.io/doc/com.jcabi/jcabi-dynamo)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.jcabi/jcabi-dynamo/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.jcabi/jcabi-dynamo)
[![codecov](https://codecov.io/gh/jcabi/jcabi-dynamo/branch/master/graph/badge.svg)](https://codecov.io/gh/jcabi/jcabi-dynamo)

More details are here: [dynamo.jcabi.com](http://dynamo.jcabi.com/index.html).

Also, read this blog post: [Object-Oriented DynamoDB API](http://www.yegor256.com/2014/04/14/jcabi-dynamo-java-api-of-aws-dynamodb.html).

Set of classes in `com.jcabi.dynamo`
is an object layer on top of
[AWS SDK for Dynamo DB](http://aws.amazon.com/sdkforjava/).

For example, to read an item from your Dynamo table:

```java
public class Main {
  public static void main(String[] args) {
    Credentials credentials = new Credentials.Simple("AWS key", "AWS secret");
    Region region = new Region.Simple(credentials);
    Table table = region.table("foo");
    Collection<Item> items = table.frame().where("id", Conditions.equalTo(123));
    for (Item item : items) {
      System.out.println(item.get("name").getS());
    }
  }
}
```

## How to contribute?

Fork the repository, make changes, submit a pull request.
We promise to review your changes same day and apply to
the `master` branch, if they look correct.

Please run Maven build before submitting a pull request:

```
$ mvn clean install -Pqulice
```
