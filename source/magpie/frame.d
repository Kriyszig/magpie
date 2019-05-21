module magpie.frame;

import mir.ndslice;

/++
Index: Structure that represents indexing of the given dataframe
+/
struct Index
{
    /// To know if data is multi-indexed for displaying and saving to a CSV file
    bool isMultiIndexed = false;

    /// Field Tiyle for all the rowIndex
    string[] rIndexTitles;
    /// Strings representing row indexes
    string[][]  rIndices;
    /// Codes linking the position of the index to it's location in rIndices
    int[][] rCodes;

    /// Field Titles for Column Index
    string[] cIndexTitles;
    /// Strings representing column index
    string[][] cIndices;
    /// Codes linking the position of each column index to it's location in cIndices
    int[][] cCodes;

}


/++
DataFrame: Structure representing a DataFrame.
Declaration Syntax: DataFrame!double foo;
                    Here we are declaring a DataFrame with variable name foo that has it's data in form of double datatype.
                    The datatype helps in defining operations possible on the DataFrame.
+/
struct DataFrame(T)
{
    /// ndslice holding the tabular data that can be operated upon
    Slice!(T*, 2, Universal) data;
    /// Indexing of the DataFrame
    Index frameIndex;

private:
    class DataFrameExceptions : Exception
    {
        this(string msg, string file = __FILE__, size_t line = __LINE__) {
            super(msg, file, line);
        }
    }

    // Given a set of indexes and codes in random order, arranges the indices in ascending order and swaps around the codes
    // Note: Any changes made should be reflected below in the unit test for this function
    void arrangeIndex(string[] indices, int[] code)
    {
        // If length of indices is 0, codes themself represent indexing which doesn't require sorting
        if(indices.length == 0)
        {
            return;
        }
        // Selection sort
        for(int i = 0; i < indices.length; ++i)
        {
            int pos = i;
            for(int j = i + 1; j < indices.length; ++j)
            {
                if(indices[j] < indices[pos])
                {
                    pos = j;
                }
            }

            // In case the first element is the smallest element
            if(pos == i)
            {
                continue;
            }

            // Swaping index around
            immutable string tmp = indices[pos];
            indices[pos] = indices[i];
            indices[i] = tmp;

            // Swapping codes around
            for(int j = 0; j < code.length; ++j)
            {
                if(code[j] == i)
                {
                    code[j] = pos;
                }
                else if(code[j] == pos)
                {
                    code[j] = i;
                }
            }
        }
    }

public:
    /++
    Display: Displays the dataframe on the terminal in a tabular form.
    +/
    void display()
    {
        import std.stdio: write, writeln;
        // Checking if the given dataframe is empty
        if(data.shape[0] == 0 || data.shape[1] == 0)
        {
            writeln("The DataFrame is empty");
            return;
        }

        import std.conv: to;
        immutable int maxColSize = 45;                              // Maximum size of a column. If this size is excedded ... is added at the end of data
        immutable int terminalWidth = 200;                          // Terminal width to display correct form in all cases
        immutable ulong rindexDepth = frameIndex.rCodes.length;     // Cells from left, row indexes will occupy
        immutable ulong cindexDepth = frameIndex.cCodes.length;     // Cells from top column indexes will occupy
        ulong[] colSize;                                            // An array storing the max size of each column to indent the data properly

        // Finding max gap for row index columns
        for(int i = 0; i < rindexDepth; ++i)
        {
            // User will need to specify title to each roww index levels so this won't throw any error
            // Will be taken care of in assignment ops
            ulong maxGap = frameIndex.rIndexTitles[i].length;
            for(int j = 0; j < frameIndex.rCodes[i].length; ++j)
            {
                // If the indexes are of type integer, the rCodes will themself represent indexes
                // In the above case, the particular level in rIndices will be left empty
                if(frameIndex.rIndices[i].length == 0)
                {
                    if(maxGap < to!string(frameIndex.rCodes[i][j]).length)
                    {
                        maxGap = to!string(frameIndex.rCodes[i][j]).length;
                    }
                }
                else
                {
                    if(maxGap < frameIndex.rIndices[i][frameIndex.rCodes[i][j]].length)
                    {
                        maxGap = frameIndex.rIndices[i][frameIndex.rCodes[i][j]].length;
                    }
                }

                // If max gap goes over the maximum column size allowed, column size is set to maxcolumnsize and further searching stops
                if(maxGap >= maxColSize)
                {
                    maxGap = maxColSize;
                    break;
                }
            }

            // Adding the size to colSize array
            colSize ~= [maxGap];
        }

        // If the column index titles exist (It's not necessary for them to exist), the innermost row index column will we same
        // as the column displaying the column index. Making changes in the gaps as necessary
        if(frameIndex.cIndexTitles.length != 0)
        {
            // The max colun size for this particular column will be stored in colSize[-1] at the moment it is calculated
            // Note: Here subtracting from unsigned int won't lead to undefined behavior because of the condition in the above if statement
            ulong maxGap = colSize[colSize.length - 1];
            // Calculating max size
            for(int i = 0; i < frameIndex.cIndexTitles.length; ++i)
            {
                if(maxGap < frameIndex.cIndexTitles[i].length)
                {
                    maxGap = frameIndex.cIndexTitles[i].length;
                }

                // If max gap goes over the maximum column size allowed, column size is set to maxcolumnsize and further searching stops
                if(maxGap >= maxColSize)
                {
                    maxGap = maxColSize;
                    break;
                }
            }

            // Changing colSize as necessary
            colSize[colSize.length - 1] = maxGap;
        }

        // Setting gap for the columns containing the column indexes and the data
        for(int i = 0; i < data.shape[1]; ++i)
        {
            ulong maxGap = 0;
            // Checking through columnIndexes
            for(int j = 0; j < cindexDepth; ++j)
            {
                // In case the particular field of cIndices in empty, cCodes will default to the indexes
                if(frameIndex.cIndices[j].length == 0)
                {
                    if(maxGap < to!string(frameIndex.cCodes[j][i]).length)
                    {
                        maxGap = to!string(frameIndex.cCodes[j][i]).length;
                    }
                }
                else
                {
                    if(maxGap < frameIndex.cIndices[j][frameIndex.cCodes[j][i]].length)
                    {
                        maxGap = frameIndex.cIndices[j][frameIndex.cCodes[j][i]].length;
                    }
                }

                // If gap exceeds max column size, setting gap as the maxColSize and stopping traversing
                if(maxGap >= maxColSize)
                {
                    maxGap = maxColSize;
                    break;
                }
            }

            // Going through the data fields
            for(int j = 0 ; j < data.shape[0]; ++j)
            {
                if(maxGap < to!string(data[j][i]).length)
                {
                    maxGap = to!string(data[j][i]).length;
                }

                // If gap exceeds max column size, setting gap as the maxColSize and stopping traversing
                if(maxGap >= maxColSize)
                {
                    maxGap = maxColSize;
                    break;
                }
            }

            colSize ~= maxGap;
        }

        int forward = 0;        // Number of colums from front that can fit in the terminal
        int backward = 1;       // Number of columns from back that can fit within the terminal
        int sum = 0;            // Sum of column size

        // Checking forward
        for(int i = 0; i < colSize.length; ++i)
        {
            if(sum + colSize[i] + 4 >= terminalWidth / 2)
            {
                break;
            }
            ++forward;
            sum += colSize[i] + 2;
        }

        sum = 0;
        // Checking backward
        for(int i = to!int(colSize.length) - 1; i > -1; --i)
        {
            if(sum + colSize[i] + 3 >= terminalWidth / 2)
            {
                break;
            }
            ++backward;
            sum += colSize[i] + 2;
        }

        // Backward starts from 1 as default
        if(backward > 1)
        {
            --backward;
        }

        // If backward + forward > colSize.length, the total table can fit on the screen
        if(backward + forward >= colSize.length)
        {
            forward = to!int(colSize.length);
            backward = 0;
        }

        const ulong stretch = (frameIndex.cCodes.length + data.shape[0] + ((frameIndex.cIndexTitles.length > 0)?1: 0));
        int top = 0, bottom = 0;
        if(stretch <= 50)
        {
            top = to!int(stretch);
            bottom = 0;
        }
        else
        {
            top = 25;
            bottom = 25;
        }
        foreach(size_t p, int[2] startstop; [[0, top],[to!int(stretch - bottom), to!int(stretch)]])
        {
            ulong dataIndex = 0;  // For traversing throught the data
            if(p == 1 && data.shape[0] - 25 > 0)
            {
                dataIndex = data.shape[0] - 25;
            }

            // Display Loop
            for(int i = startstop[0]; i < startstop[1]; ++i)
            {
                // Display the column Indexes
                if(i < frameIndex.cCodes.length)
                {
                    // Traverse right
                    foreach(size_t inx, int[2] ele;
                    [[0, forward], [to!int(colSize.length - backward), to!int(colSize.length)]])
                    {
                        for(int j = ele[0]; j < ele[1]; ++j)
                        {
                            // Adding blank spaces and display row Index Titles
                            // Note: The below subtraction will not give undefined behavior as rCodes will always be > 0
                            if(j < frameIndex.rCodes.length - 1)
                            {
                                // If only row index exists, the index must be printed on the last line of column indexes
                                if(i == frameIndex.cCodes.length - 1 && frameIndex.cIndexTitles.length == 0)
                                {
                                    // If the length of string goes over the prescribed limits
                                    if(frameIndex.rIndexTitles[j].length > colSize[j])
                                    {
                                        write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3],"...  ");
                                    }
                                    else
                                    {
                                        write(frameIndex.rIndexTitles[j]);
                                        for(ulong  k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                        {
                                            write(" ");
                                        }
                                    }
                                }
                                else
                                {
                                    // Printing blank spaces otherwise
                                    for(int k = 0; k < colSize[j] + 2; ++k)
                                    {
                                        write(" ");
                                    }
                                }
                            }
                            else if(j == frameIndex.rCodes.length - 1)
                            {
                                // In case the column index titles exists, the row indexes should be printed in the next line
                                if(frameIndex.cIndexTitles.length != 0)
                                {
                                    // In case the string goes over the max column size
                                    if(frameIndex.cIndexTitles[i].length > maxColSize)
                                    {
                                        write(frameIndex.cIndexTitles[i][0 .. maxColSize - 3], "...  ");
                                    }
                                    else
                                    {
                                        write(frameIndex.cIndexTitles[i]);
                                        for(ulong k = frameIndex.cIndexTitles[i].length; k < colSize[j] + 2; ++k)
                                        {
                                            write(" ");
                                        }
                                    }
                                }
                                else
                                {
                                    // Printing row index titles in a seperate line if column Index Titles are present
                                    // Note: The above subtraction will not give any undefined behavior as cCodes.length is always > 0
                                    if(i == frameIndex.cCodes.length - 1)
                                    {
                                        if(frameIndex.rIndexTitles[j].length > colSize[j])
                                        {
                                            write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3],"...  ");
                                        }
                                        else
                                        {
                                            write(frameIndex.rIndexTitles[j]);
                                            for(ulong  k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                            {
                                                write(" ");
                                            }
                                        }
                                    }
                                    else
                                    {
                                        for(int k = 0; k < colSize[j] + 2; ++k)
                                        {
                                            write(" ");
                                        }
                                    }
                                }
                            }
                            else
                            {
                                // Displaying column index
                                string index2disp;
                                // Skipping similar indexes in case of multi-indexing
                                if(frameIndex.isMultiIndexed == true && j - frameIndex.rCodes.length > 0
                                && i != frameIndex.cCodes.length - 1
                                && frameIndex.cCodes[i][j - frameIndex.rCodes.length]
                                == frameIndex.cCodes[i][j - frameIndex.rCodes.length - 1])
                                {
                                    index2disp = "";
                                }
                                else if(frameIndex.cIndices[i].length == 0)
                                {
                                    // In case indices are empty, codes default to index
                                    index2disp = to!string(frameIndex.cCodes[i][j - frameIndex.rCodes.length]);
                                }
                                else
                                {
                                    index2disp = 
                                    frameIndex.cIndices[i][frameIndex.cCodes[i][j - frameIndex.rCodes.length]];
                                }
                                if(index2disp.length > colSize[j])
                                {
                                    index2disp = index2disp[0 .. colSize[j] - 3] ~ "...";
                                }
                                write(index2disp);
                                for(ulong k = index2disp.length; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }

                        }

                        // In case the entire dataframe cannot be displayed in the terminal, the dataframe will be displayed partially from left to right
                        // and partially from right to left till the middle of the screen
                        if(backward > 0 && inx == 0)
                        {
                            // Adding the continuation dots
                            write("...  ");
                        }
                    }
                    write("\n");
                }
                else if(i == frameIndex.cCodes.length && frameIndex.cIndexTitles.length != 0)
                {
                    // In case both column index titles and row index titles, printing row index titles in a new line of it's own
                    ulong stop = frameIndex.rCodes.length;
                    if(frameIndex.rCodes.length > forward)
                    {
                        stop = forward;
                    }
                    for(int j = 0;j < stop; ++j)
                    {
                        if(frameIndex.rIndexTitles[j].length > colSize[j])
                        {
                            write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3], "...  ");
                        }
                        else
                        {
                            write(frameIndex.rIndexTitles[j]);
                            for(ulong k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                            {
                                write(" ");
                            }
                        }
                    }
                    if(stop != frameIndex.rCodes.length)
                    {
                        write("...  ");
                        for(ulong j = colSize.length - backward; j < frameIndex.rCodes.length; ++j)
                        {
                            if(frameIndex.rIndexTitles[j].length > colSize[j])
                            {
                                write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3], "...  ");
                            }
                            else
                            {
                                write(frameIndex.rIndexTitles[j]);
                                for(ulong k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                    }
                    write("\n");
                }
                else
                {
                    // Printing row indexes and data
                    foreach(size_t inx, int[2] ele;
                    [[0, forward], [to!int(colSize.length - backward), to!int(colSize.length)]])
                    {
                        for(int j = ele[0]; j < ele[1]; ++j)
                        {
                            if(j < frameIndex.rCodes.length)
                            {
                                // Skiping index in case of multi-indexing and the index above is came as the current index
                                if(frameIndex.isMultiIndexed == true && dataIndex > 0 
                                && j < frameIndex.rCodes.length - 1
                                && frameIndex.rCodes[j][dataIndex] == frameIndex.rCodes[j][dataIndex - 1])
                                {
                                    for(int k = 0; k < colSize[j] + 2; ++k)
                                    {
                                        write(" ");
                                    }
                                }
                                else
                                {
                                    string dispstr;
                                    // If indices are empty, the codes will default as index
                                    if(frameIndex.rIndices[j].length == 0)
                                    {
                                        dispstr = to!string(frameIndex.rCodes[j][dataIndex]);
                                    }
                                    else
                                    {
                                        dispstr = frameIndex.rIndices[j][frameIndex.rCodes[j][dataIndex]];
                                    }

                                    // Checking if the display string is longer than max column size
                                    if(dispstr.length > colSize[j])
                                    {
                                        write(dispstr[0 .. colSize[j] - 3], "...  ");
                                    }
                                    else
                                    {
                                        write(dispstr);
                                        for(ulong k = dispstr.length; k < colSize[j] + 2; ++k)
                                        {
                                            write(" ");
                                        }
                                    }
                                }
                            }
                            else
                            {
                                // Displaying data
                                string dispstr = to!string(data[dataIndex][j - frameIndex.rCodes.length]);
                                // Checking if it's larger than max column size
                                if(dispstr.length > colSize[j])
                                {
                                    write(dispstr[0 .. colSize[j] - 3], "...  ");
                                }
                                else
                                {
                                    write(dispstr);
                                    for(ulong k = dispstr.length; k < colSize[j] + 2; ++k)
                                    {
                                        write(" ");
                                    }
                                }
                            }
                        }

                        // Adding continuation dots in case the dataframe cannot be fit in
                        if(backward > 0 && inx == 0)
                        {
                            write("...  ");
                        }
                    }
                    ++dataIndex;
                    write("\n");
                }
            }
            if(p == 0 && bottom > 0)
            {
                // Adding continuation dots in case the data is long length-wise
                foreach(size_t inx, int[2] ele;
                [[0, forward], [to!int(colSize.length - backward), to!int(colSize.length)]])
                {
                    for(int i = ele[0]; i < ele[1]; ++i)
                    {
                        for(int j = 0; j < colSize[i]; ++j)
                        {
                            write(".");
                        }
                        write("  ");
                    }

                    if(inx == 0 && backward > 0)
                    {
                        write("...  ");
                    }
                }

                write("\n");
            }
        }
        // Displaying DataFrame dimension for user to hanve an understanding of the quantity of data in case the total data was cut-off while displaying
        writeln("Dataframe Dimension: [ ", stretch, " X ", colSize.length, " ]");
        writeln("Operable Data Dimension: [ ", data.shape[0], " X ", data.shape[1], " ]");
        // writeln(colSize);
        // writeln(forward, "\t", backward);
        // writeln(data);
    }

    /++
    Assignment operation with 1D array as input
    +/
    void opAssign(T[] data1d)
    {
        // Checking if data1d is empty
        if(data1d.length == 0)
        {
            throw new DataFrameExceptions("Expected data to be entered into the DataFrame bur recieved empty array");
        }
        // Starting with new set of Indexes
        frameIndex = Index();
        // Converting data to 2d slice of dimension 1 x n
        data = data1d.sliced(1, data1d.length).universal;
        frameIndex.rIndexTitles = ["Index"];
        frameIndex.rCodes = [[0]];
        frameIndex.rIndices = [[]];
        frameIndex.cIndices = [[]];
        frameIndex.cCodes = [[]];
        // Setting column indexes
        for(int i = 0; i < data1d.length; ++i)
        {
            frameIndex.cCodes[0] ~= [i];
        }
    }

    /++
    Assignment operation with 2D array as an input
    defval: Default value for padding in case the input is not rectangular
    +/
    void opAssign(T[][] data2d)
    {
        // Checking if data2d in empty - first pass
        if(data2d.length == 0)
        {
            throw new DataFrameExceptions("Expected data to be entered into the DataFrame bur recieved empty array");
        }
        ulong len = data2d[0].length;   // Stores the width of the DataFrame
        for(int i = 0; i < data2d.length; ++i)
        {
            if(data2d[i].length > len)
            {
                len = data2d[i].length;
            }
        }
        // Checking if data2d is empty - second pass
        // In case data 2d resembles: [[], [], []]
        if(len == 0)
        {
            throw new DataFrameExceptions(
                "Expected data to be entered into the DataFrame but recieved array with innermost dimension 0"
            );
        }

        // Flattening the data into a 1D array
        T[] flattened = [];
        for(int i = 0; i < data2d.length; ++i)
        {
            flattened ~= data2d[i];
            if(data2d[i].length != len)
            {
                // Padding in case the data is not rectangular
                for(ulong j = data2d[i].length; j < len; ++j)
                {
                    flattened ~= [T.init];
                }
            }
        }

        // Resetting index
        frameIndex = Index();
        // Converting data to 2d Slice
        data = flattened.sliced(data2d.length, len).universal;
        frameIndex.rIndexTitles = ["Index"];
        frameIndex.rIndices = [[]];
        frameIndex.cIndices = [[]];
        frameIndex.rCodes = [[]];
        frameIndex.cCodes = [[]];
        // Setting default row index
        for(int i = 0; i < data2d.length; ++i)
        {
            frameIndex.rCodes[0] ~= [i];
        }
        // Setting default column index
        for(int i = 0; i < len; ++i)
        {
            frameIndex.cCodes[0] ~= [i];
        }
    }
}

/// assignment operation with 1D array
unittest
{
    DataFrame!double df;
    df = [1.2,2.4,3.6];
    assert(df.data == [1.2,2.4,3.6].sliced(1,3).universal);
    // df.display();
}

/// assignment operation with 2d array
unittest
{
    DataFrame!double df;
    df = [[1.2,2.4],[3.6, 4.8]];
    assert(df.data == [1.2,2.4, 3.6, 4.8].sliced(2,2).universal);
    // df.display();
}

/// Assignment that requires padding
unittest
{
    import std.stdio: writeln;
    // df is of type int instead of float as assert will not consided 2 nan equal
    DataFrame!int df;
    df = [[1],[3, 4]];
    assert(df.data == [1,0,3,4].sliced(2,2).universal);
    // df.display();
}

// Unit test for private member function
unittest
{
    // This code is same as arrangeIndex private method of dataframe
    void arrangeIndex(string[] indices, int[] code)
    {
        // If length of indices is 0, codes themself represent indexing which doesn't require sorting
        if(indices.length == 0)
        {
            return;
        }
        // Selection sort
        for(int i = 0; i < indices.length; ++i)
        {
            int pos = i;
            for(int j = i + 1; j < indices.length; ++j)
            {
                if(indices[j] < indices[pos])
                {
                    pos = j;
                }
            }

            // In case the first element is the smallest element
            if(pos == i)
            {
                continue;
            }

            // Swaping index around
            immutable string tmp = indices[pos];
            indices[pos] = indices[i];
            indices[i] = tmp;

            // Swapping codes around
            for(int j = 0; j < code.length; ++j)
            {
                if(code[j] == i)
                {
                    code[j] = pos;
                }
                else if(code[j] == pos)
                {
                    code[j] = i;
                }
            }
        }
    }

    string[] index = ["b","a","d","c"];
    int[] code = [0,1,2,3];
    // Arranging index in ascending order while changing index so the result will still be ["b","a","d","c"]
    arrangeIndex(index, code);
    assert(index == ["a", "b", "c", "d"]);
    assert(code == [1, 0, 3, 2]);
}