### Magpie - Mir Data Analysis and Processing Library

Current Specification:
- <b>Index</b>: Index must be designed to support multi-indexing further on. Right now the structure I have in mind is as follows:

```d
struct Index
{
    bool isMultiIndexed = false;
    
    string[] rIndexTitles = [];
    string[][] rHeaders = [];
    int[][] rCodes = [];

    string[] cIndexTitles = [];
    string[][] cHeaders = [];
    int[][] cCodes = [];
}
```
* rIndexTitles - Titles for row Indexes.
* rHeaders - Headers for each row
* rCodes - Codes to represent multi-indexing
<br />
<b>Note</b>:
* In case rHeaders is an empty array, the default indexing will take over - 0 .. $

<br>

- Dataframe:

```d
struct DataFrame(T)
{
    Slice!(T*, 2, Universal) data;
    Index frameIndex;
}
```
* data - The data on which numeric coputation is needed
* frameIndex - Indexes to locate and access DataFrame element.
