# Magpie - Mir Data Analysis and Processing Library

DataFrame project for GSoC 2019. The goal of the project is to deliver a DataFrame that behaves just like Pandas in Python.

### Usage

```d
import magpie.frame: DataFrame;

Dataframe!double df;    // This declared a dataframe such that it contains homogeneous data of type double
df = [[1.2,2.4],[3.6, 4.8]];
assert(df.data == [1.2,2.4, 3.6, 4.8].sliced(2,2).universal);   // Data is stored as a Universal 2D slice
df.display();
df.to_csv("./example.csv");
df.from_csv("./example.csv", 1, 1);
df.display();
```

### Structure

- The DataFrame structure is defined as:

```d
struct DataFrame(T)
{
    Index frameIndex;
    Slice!(T*, 2, Universal) data;
}
```


## Functions

#### `display()`

Displays the content of the dataframe on the terminal.

#### `to_csv(string path, bool writeIndex = true, bool writeColumn = true, char sep = ",")`

Writes the DataFrame to CSV format.

* writeIndex - If set true writes row indexes to the file.
* writeColumn - If set rue writes column indexes to the file
* sep - Is the data seperator

#### `from_csv(string path, int indexDepth = 1, int columnDepth = 1, char sep = ',')` (Experimental)

Parsing of CSV file into a DataFrame

* indexDepth - How many columns from left do row index span
* columnDepth - How many rows from top column index span
* sep - Data Seperator
