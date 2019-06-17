# Magpie - Mir Data Analysis and Processing Library

[![Build Status](https://travis-ci.org/Kriyszig/magpie.svg?branch=master)](https://travis-ci.org/Kriyszig/magpie)

DataFrame project for GSoC 2019.

The goal of the project is to deliver a DataFrame that behaves just like Pandas in Python.

## Usage

```d
import magpie.dataframe: DataFrame;
import magpie.index: Index;

DataFrame!(int, 2, double, 1) df;
Index index;
index.setIndex([0,1,2,3,4,5], ["Row Index"], [0,1,2], ["Column Index"]);
df.setFrameIndex(index);
df.display();
/*
 *  Column Index  0  1  2    
 *  Row Index     
 *  0             0  0  nan  
 *  1             0  0  nan  
 *  2             0  0  nan  
 *  3             0  0  nan  
 *  4             0  0  nan  
 *  5             0  0  nan
 */

df.assign!1(2, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
df.display();
/*
 *  Column Index  0  1  2  
 *  Row Index     
 *  0             0  0  1  
 *  1             0  0  2  
 *  2             0  0  3  
 *  3             0  0  4  
 *  4             0  0  5  
 *  5             0  0  6
 */

df.assign!1(1, [1, 2, 3]);
df.display();
/*
 *  Column Index  0  1  2  
 *  Row Index     
 *  0             0  1  1  
 *  1             0  2  2  
 *  2             0  3  3  
 *  3             0  0  4  
 *  4             0  0  5  
 *  5             0  0  6
 */

df.assign!0(0, 4, 5, 1.6);
df.display();
/*
 *  Column Index  0  1  2    
 *  Row Index     
 *  0             4  5  1.6  
 *  1             0  2  2    
 *  2             0  3  3    
 *  3             0  0  4    
 *  4             0  0  5    
 *  5             0  0  6  
 */

index.extend!0([6]);
df.setFrameIndex(index);
df.display();
/*
 *  Column Index  0  1  2    
 *  Row Index     
 *  0             4  5  1.6  
 *  1             0  2  2    
 *  2             0  3  3    
 *  3             0  0  4    
 *  4             0  0  5    
 *  5             0  0  6    
 *  6             0  0  nan 
 */
```

## Different ways of creating a DataFrame

```d
import magpie.dataframe: DataFrame;

DataFrame!(int, 10) df;
DataFrame!(int, 10, double, 10) df;
DataFrame!(int[10], double[10]) df;
DataFrame!(int, 10, double[10]) df;

// DataFrame from Structure
struct S
{
    int[10] a;
    double[10] b;
}

import std.traits: Fields;
DataFrame!(Fields!S) df;

// In case all the fields are of primitive types, you can add
// true in the beginning to reduce compile time
DataFrame!(true, int, int, double, double) df;

struct RS
{
    int a;
    int b;
    double c;
    double d;
}

DataFrame!(Fields!(RS)) df;
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
- Index is defined as folows:

```d
struct Index
{

    struct Indexing
    {
        string[] titles;
        string[][] index;
        int[][] codes;
    }

    /// To know if data is multi-indexed
    bool isMultiIndexed = false;

    /// Row and Column indexing
    Indexing[2] indexing;
}
```

## Features

* [Index](#Index)
* [Access](#Access)
* [Assignment](#Assignment)
* [Binary Operations](#BinaryOps)
* [I/O](#I/O)

### Index

Index is a structure that stores the indexes as strings with a special space optimization for integer indexes.

```d
import magpie.index: Index;

// Declaration
Index indx;

// Setting Indexes
indx.setIndex([1, 2, 3, 4], ["Row Index"], [1, 2, 3], ["Column Index"]);
/*
 *  Provides the following basic skeleton for the DataFrame:
 *
 *  Column Index  1  2  3
 *  Row Index
 *  1
 *  2
 *  3
 *  4
 */
```

#### `setIndex(rowIndex, rowIndexTitles, columnIndex?, columnIndexTitles?)`

Setting indexes of an empty Index.

* rowIndex - Can be a single or two dimensional array of string or integers
* rowIndexTitles - Single Dimensional array of strings
* columnIndex[Optional] - Can be a single or two dimensional array of string or integers
* columnIndexTitles[Optional] - Single Dimensional array of strings

Usage:

```d
import magpie.index: Index;

Index inx;
inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["RL1", "RL2"],
             [["Hello", "Hi"], ["Hi", "Hello"]], ["CL1", "CL2"]);
/*
 *  The basic skeleton:
 *  
 *         CL1    Hello  Hi
 *         CL2    Hi     Hello
 *  RL1    Rl2
 *  Hello  Hi
 *  Hi     Hello
 */
```

Note: In case the dimension of columnIndex don't match the dimension of DataFrame, the default indexing will be applied.

#### `constructFromPairs(rowIndex, rowIndexTitles, columnIndex?, columnIndexTitles?)`

Setting row indexes row wise and column indexes column wise

* rowIndex - Two dimensional array of string or integer
* rowIndexTitles - Single Dimensional array of strings
* columnIndex[Optional] - Two dimensional array of string or integers
* columnIndexTitles[Optional] - Single Dimensional array of strings

```d
import magpie.index: Index;

Index inx;
inx.constructFromPairs([["Hello", "Hi"], ["Hi", "Hello"], ["Hey", "Hey"]],
                        ["RL1", "RL2"],
                        [["Hello", "Hi"], ["Hi", "Hello"], ["Hey", "Hey"]],
                        ["CL1", "CL2"]);
/*
 *  The basic skeleton:
 *  
 *         CL1    Hello  Hi     Hey
 *         CL2    Hi     Hello  Hey
 *  RL1    Rl2
 *  Hello  Hi
 *  Hi     Hello
 *  Hey    Hey
 */
```

#### `constructFromZip(axis, levels)(index, titles)`

Constructing Index from a Zip range

* axis - 0 to construct row index, 1 for constructing column index
* levels - depth of indexing
* index - Zip containing the indexes
* titles - Index titles [Mandatory for axis = 0]

```d
import magpie.index: Index;
import std.range: zip;

Index inx;
auto z = zip([1, 2, 3, 4], ["Hello", "Hi", "Hello", "Hi"]);
inx.constructFromZip!(0, 2)(z, ["Index1", "Index2"]);
/*
 *  The basic skeleton:
 * 
 *  Index1  Index2
 *  1       Hello
 *  2       Hi
 *  3       Hello
 *  4       Hi
 */

auto zc = zip([1, 2, 3, 4], ["Hello", "Ho", "Hello", "Ho"]);
inx.constructFromZip!(1, 2)(zc);
/*
 *  The basic skeleton:
 *  
                    1      2   3      4
 *                  Hello  Hi  Hello  Hi
 *  Index1  Index2
 *  1       Hello
 *  2       Hi
 *  3       Hello
 *  4       Hi
 */
```

#### `constructFromLevels(axis)(index, titles)`

Construct indexes based on unique levels

* axis - 0 to construct row index, 1 for constructing column index
* index - Two dimensional array of string containing unique level of indexes
* titles - Index titles [Mandatory for axis = 0]

```d
import magpie.index: Index;

Index inx;
inx.constructFromLevels!0([["Air", "Water"],
                           ["Transportation"],
                           ["Net Income", "Gross Income"]],
                          ["Index1", "Index2", "Index3"]);

/*
 *  The basic skeleton:
 * 
 *  Index1  Index2          Index3
 *  Air     Transportation  Net Income
 *  Air     Transportation  Gross Income
 *  Water   Transportation  Net Income
 *  Water   Transportation  Gross Income
 */

inx.constructFromLevels!1([["Air", "Water"], ["Transportation", "What_to_put_here"], ["Net Income", "Gross Income"]]);

/*
 *  The basic skeleton:
 *                                        Air             Air             Air               Air               Water           Water           Water               Water
 *                                        Transportation  Transportation  What_to put_here  What_to_put_here  Transportation  Transportation  What_to put_here  What_to_put_here 
 *  Index1  Index2          Index3        Net Income      Gross Income    Net Income        Gross Income      Net Income      Gross Income    Net Income        Gross Income      
 *  Air     Transportation  Net Income
 *  Air     Transportation  Gross Income
 *  Water   Transportation  Net Income
 *  Water   Transportation  Gross Income
 */
```

#### `extend(axis)(next)`

Extending indexing of a previously assigned Index.

* axis - set 0 to extend row index else set 1
* next - element to extend index (Needs to be a 1D array of string or integer)

Usage:

```d
import magpie.index: Index;

Index inx;
inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["RL1", "RL2"],
             [["Hello", "Hi"], ["Hi", "Hello"]], ["CL1", "CL2"]);
/*
 *  The basic skeleton:
 *  
 *         CL1    Hello  Hi
 *         CL2    Hi     Hello
 *  RL1    Rl2
 *  Hello  Hi
 *  Hi     Hello
 */

inx.extend!0(["Hey", "Hey"]);
inx.extend!1(["Yo", "Yo"]);

/*
 *  The basic skeleton:
 *  
 *         CL1    Hello  Hi     Yo
 *         CL2    Hi     Hello  Yo
 *  RL1    Rl2
 *  Hello  Hi
 *  Hi     Hello
 *  Hey    Hey 
 */
```
### Access

In addition to array like access to elements, some of the other ways to access elements are:

#### `at!(row, column)`

Direct access to element using integral indexes

* row - Integral index of row
* column - Integral Index of column

Usage:

```d
import magpie.dataframe: DataFrame;
import magpie.index: Index;

Index inx;
inx.setIndex([1, 2, 3],["rindex"]);

DataFrame!(int, 2) df;
df.setFrameIndex(inx);
df.at!(0,0);        // Will return 0
df[0, 0];           // Same as above, returns 0
df[["1"], ["0"]];   // Same as above - usig string indexes - returns 0
```
#### Getting row and column position from string indexes

#### `getRowPosition(indexes)`

Getting integer position of a row in DataFrame based on string index

* indexes - 1D array of string indexes of the row you desire

#### `getColumnPosition(indexes)`

Getting integer position of a column in DataFrame based on string index

* indexes - 1D array of string indexes of the column you desire

Usage:
```d
Index inx;
DataFrame!(int, 2) df;
inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["RL1", "RL2"],
            [["Hello", "Hi"], ["Hi", "Hello"]], ["CL1", "CL2"]);
df.setFrameIndex(inx);

df.getRowPosition(["Hello", "Hi"]); // 0
df.getColumnPosition(["Hi", "Hello"]); // 1
```

### Assignment

#### Direct Assignment

```d
import magpie.dataframe: DataFrame;
import magpie.index: Index;

Index inx;
inx.setIndex([1, 2, 3],["rindex"]);

DataFrame!(int, 2, double) df;
df.setFrameIndex(inx);  // If column index isn't specified, default indexing takes over
df.display();
/*
 *  rindex  0  1  2    
 *  1       0  0  nan  
 *  2       0  0  nan  
 *  3       0  0  nan
 */

df = [[1.0], [1.0, 2.0], [1.0, 2.0, 3.5]];
df.display();
/*
 *  rindex  0  1  2    
 *  1       1  0  nan  
 *  2       1  2  nan  
 *  3       1  2  3.5
 */

// Assignment based on direct integer index
df[0, 0] = 42;
df.display();
/*
 *  rindex  0   1  2    
 *  1       42  0  nan  
 *  2       1   2  nan  
 *  3       1   2  3.5
 */

// Assignment based on string index
df[["2"], ["1"]] = 17;
df.display();
/*
 *  rindex  0   1   2    
 *  1       42  0   nan  
 *  2       1   17  nan  
 *  3       1   2   3.5
 */
```
Note: Direct assignment works with only 2D array. Each element will be implicitly casted to the data type of the given column.

#### `assign(axis)(index, data)`

Assign data completely or partially to a row or a column.

* axis - set 0 to assign to a row else set 1 to assign to a particular column
* index - Integer or string index of the location to assign
* data - Data to set at the particular row / column

Usage:

```d
import magpie.dataframe: DataFrame;
import magpie.index: Index;

Index inx;
inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],
             [["Hello", "Hi"], ["Hi", "Hello"]]);

DataFrame!(double, int) df;
df.setFrameIndex(inx);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     nan    0      
 *  Hi     Hello  nan    0    
 */

df.RowType ele;
ele[0] = 1.77;
ele[1] = 4;

// Using RowType alais
df.assign!0(["Hi", "Hello"], ele);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     nan    0      
 *  Hi     Hello  1.77   4    
 */

// Without RowType
df.assign!0(["Hi", "Hello"], 1.688, 6);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     nan    0      
 *  Hi     Hello  1.688  6  
 */

// Assigning usig direct index
df.assign!0(1, 1.588, 6);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     nan    0      
 *  Hi     Hello  1.588  6  
 */

// Assigning column
df.assign!1(["Hello", "Hi"], [1.2, 3.6]);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     1.2    0      
 *  Hi     Hello  3.6    6  
 */

// Assigning columns using direct index
df.assign!1(0, [1.26, 4.6]);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     1.26   0     
 *  Hi     Hello  4.6    6  
 */

// Partial Assignment - rows
df.assign!0(1, 3.588);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     1.26   0     
 *  Hi     Hello  3.588  6  
 */

// Partial Assignment - columns
df.assign!1(0, [2.26]);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     2.26   0     
 *  Hi     Hello  4.6    6  
 */
```

### BinaryOps

DataFrame supports row and column binary operations. Supported operations:
* Assignment (Assigning values of one row/column to another)
* Addition
* Subtraction
* Multiplication
* Division

#### Usage

```d
import magpie.dataframe: DataFrame;
import magpie.index: Index;

DataFrame!(int, 3) df;
Index inx;
inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
df.setFrameIndex(inx);
df.display();
/*
 *  Index  Index  0  1  2
 *  Hello  Hi     0  0  0   
 *  Hi     Hello  0  0  0  
 */

df.assign!1(0, [1, 4]);
df.assign!1(1, [1, 6]);
df.assign!1(2, [1, 8]);
df.display();
/*
 *  Index  Index  0  1  2
 *  Hello  Hi     1  1  1   
 *  Hi     Hello  4  6  8  
 */

df[["0"]] = df[["1"]] + df[["2"]];
df.display();
/*
 *  Index  Index  0   1  2
 *  Hello  Hi     2   1  1   
 *  Hi     Hello  14  6  8  
 */

df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0];
df.display();
/*
 *  Index  Index  0   1  2
 *  Hello  Hi     14  6  8   
 *  Hi     Hello  14  6  8  
```
Note:
* For now, binary operations only work with string based indexes.
* The first argument is always an array of string [even if level of indexing is 1]
* Don't specify axis for column binary operation. Using column binary operations as `df[["0"], 1]` will not work.


### I/O

#### `display(getStr = false, maxSize = 0)`

Displays the content of the dataframe on the terminal.

* getStr - If set to true, will return the evaluated display string instead of the terminal output
* maxSize - Override termianal size [Dynaimically detecting terminal size isn't implemented yet]

Usage:

```d
import magpie.dataframe: DataFrame;
import magpie.index: Index;

Index inx;
inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],
             [["Hello", "Hi"], ["Hi", "Hello"]]);

DataFrame!(double, int) df;
df.setFrameIndex(inx);
df.display();
/*
 *                Hello  Hi     
 *  Index  Index  Hi     Hello  
 *  Hello  Hi     nan    0      
 *  Hi     Hello  nan    0    
 */

 string display_string = df.display(true);  // If set to false, will return an empty string
 string if_terminal_width_150 = df.display(true, 150);  // Assumes terminal can accomodate 150 characters
```


#### `to_csv(string path, bool writeIndex = true, bool writeColumn = true, char sep = ",")`

Writes the DataFrame to CSV format.

* writeIndex - If set true writes row indexes to the file.
* writeColumn - If set rue writes column indexes to the file
* sep - Is the data seperator

Usage:

```d
df.to_csv("./test.csv");
```

#### `from_csv(string path, int indexDepth = 1, int columnDepth = 1,int[] columns = [], char sep = ',')` (Development)

Parsing of CSV file into a DataFrame

* indexDepth - How many columns from left do row index span
* columnDepth - How many rows from top column index span
* columns - indexes of columns to selectively parse
* sep - Data Seperator

Usage:

```d
import magpie.dataframe: DataFrame;
import magpie.index: Index;

DataFrame!(double, int, 2, double) df;
df.from_csv("any.csv", 1, 1);
/* Thie assumes any.csv has 1 column dedicated to row indexes 
 * and 1 row dedicated to column indexes
 */
```

##### Dataset Sources

* [Dataset1 - UCI Statlog (Heart) Data Set](http://archive.ics.uci.edu/ml/datasets/statlog+(heart))
* [Dataset2 - U.S. Education Datasets: Unification Project](https://www.kaggle.com/noriuk/us-education-datasets-unification-project)