# Magpie - Mir Data Analysis and Processing Library

[![Build Status](https://travis-ci.org/Kriyszig/magpie.svg?branch=master)](https://travis-ci.org/Kriyszig/magpie)

DataFrame project for GSoC 2019.

The goal of the project is to deliver a DataFrame that behaves just like Pandas in Python.

## Usage

```d
import magpie.dataframe: DataFrame;

// Creating a homogeneous DataFrame of 20 integer columns
DataFrame!(int, 20) homogeneous;

// Creating a heterogeneous DataFrame of 10 integer columns and 10 double columns
DataFrame!(int, 10, double, 10) homogeneous;

```

## Structure

- The DataFrame structure is defined as:

```d
struct DataFrame(Fields)
{
    alias RowType = getArgsList!(Fields);
    alias FrameType = staticMap!(toArr, RowType);

    // Dimension of data
    size_t rows = 0;
    size_t cols = RowType.length;

    Index indx;
    FrameType data; 
}
```


## Functions

#### `display(bool getStr = false)`

Displays the content of the dataframe on the terminal.

* getStr - If set to true, will return the evaluated display string instead of the terminal output

#### `to_csv(string path, bool writeIndex = true, bool writeColumn = true, char sep = ",")`

Writes the DataFrame to CSV format.

* writeIndex - If set true writes row indexes to the file.
* writeColumn - If set rue writes column indexes to the file
* sep - Is the data seperator

#### `from_csv(string path, int indexDepth = 1, int columnDepth = 1, char sep = ',')` (Development)

Parsing of CSV file into a DataFrame

* indexDepth - How many columns from left do row index span
* columnDepth - How many rows from top column index span
* sep - Data Seperator
