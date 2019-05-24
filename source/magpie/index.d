module magpie.index;

/++
Index: Structure that represents indexing of the given dataframe
+/
struct Index
{
    /// To know if data is multi-indexed for displaying and saving to a CSV file
    bool isMultiIndexed = false;

    /// Field Tiyle for all the rowIndex
    string[] rIndexTitles = [];
    /// Strings representing row indexes
    string[][]  rIndices = [];
    /// Codes linking the position of the index to it's location in rIndices
    int[][] rCodes = [];

    /// Field Titles for Column Index
    string[] cIndexTitles = [];
    /// Strings representing column index
    string[][] cIndices = [];
    /// Codes linking the position of each column index to it's location in cIndices
    int[][] cCodes = [];

}