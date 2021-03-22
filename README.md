# Java UNiform Query

*A type-unsafe version of C#'s Linq*

A common interface for java developers who know SQL and write code which reads, transforms, and writes data in multiple formats.


## Operations

Start by creating a new `Query` instance from your data.  Use one of the following static factory methods.

### `from(Stream<T>, Class<T>)`

Converts a `Stream<T>` into a `Stream<Row>`.  This is done via reflection.

The input type `T` must contain a public getter method named `getX` for any field that should be mapped to a column of `Query`.  Transformations on `Query` will refer to this field with name `x`.


### Transformations

Transform your data via the following instance methods on `Query`.

### `select(String...)`

Project the `Stream<Row>` into a new `Stream<Row>` with a different column mapping.  

### `where(RowMatcher...)`

Filter the `Stream<Row>`.

### `groupBy(column1, column2, ..., RowAggregator...)`

Materializes the `Stream<Row>` in memory and produces a new `Stream<Row>` from it.  This is a terminal operation on the original `Stream<Row>`.

### `orderBy(RowComparator...)`

Sort the `Stream<Row>`

## Extract the result

### `<O> stream(..., Class<O>)`

The output type `O` must contain a public getter method named `setX(value)` for a field that should be assigned from the values in column `x` of the `Query`.  

Convert the result to a `Stream<O>`.

# Example Queries

Let's get started.




# Public types

This library exposes the following types.

## Query

A `Query` contains two things.

* `Stream<Row>`: the data (as a relational table)
* `Map<String, Integer>`: a mapping from column name to its index in Row.

## Row

A `Row` represents a single row of the input data.  It does not contain column information.
 
* `Object[]`

## RowMatcher

A `Predicate<Row>` which knows how to map column name to `Row` index.  Used to specify behavior when calling `where`.  

### SingleColumnRowMatcher

### TwoColumnRowMatcher

A `BiPredicate<Object, Object>` which implements `RowMatcher`.

## RowAggregator

A `Function<Stream<Row>, Row>` which knows how to map column name to `Row` index.  Used to specify behavior when calling `groupBy`.

### SingleColumnRowAggregator

## RowComparator

A `Comparator<Row>` which knows how to map column name to `Row` index.  Used to specify behavior when calling `orderBy`.
