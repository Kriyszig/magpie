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

        ulong totalWidth = 0u;
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
            totalWidth += maxGap;
        }

        // If the column index titles exist (It's not necessary for them to exist), the innermost row index column will we same
        // as the column displaying the column index. Making changes in the gaps as necessary
        if(frameIndex.cIndexTitles.length != 0)
        {
            // The max colun size for this particular column will be stored in colSize[-1] at the moment it is calculated
            // Note: Here subtracting from unsigned int won't lead to undefined behavior because of the condition in the above if statement
            ulong maxGap = colSize[colSize.length - 1];
            totalWidth -= maxGap;
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
            totalWidth += maxGap;
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
            totalWidth += maxGap;
        }

        int forward = 0;        // Number of colums from front that can fit in the terminal
        int backward = 1;       // Number of columns from back that can fit within the terminal
        int sum = 0;            // Sum of column size

        // Checking forward
        for(int i = 0; i < colSize.length; ++i)
        {
            if(sum + colSize[i] + 2 >= terminalWidth / 2)
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
            if(sum + colSize[i] + 5 >= terminalWidth / 2)
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
        if(backward + forward > colSize.length)
        {
            forward = to!int(colSize.length);
            backward = 0;
        }

        int dataIndex = 0;  // For traversing throught the data
        // Display Loop
        for(int i = 0; i < (frameIndex.cCodes.length + data.shape[0] + ((frameIndex.cIndexTitles.length > 0)?1: 0)); ++i)
        {
            // Display the column Indexes
            if(i < frameIndex.cCodes.length)
            {
                // Traverse right
                for(int j = 0; j < forward; ++j)
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
                            index2disp = frameIndex.cIndices[i][frameIndex.cCodes[i][j - frameIndex.rCodes.length]];
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
                if(backward > 0)
                {
                    // Adding the continuation dots
                    write("...  ");
                }

                // Going from backward to forward - same logic as displaying from forward
                for(ulong j = colSize.length - backward; j < colSize.length; ++j)
                {
                    // Checking all the conditions as above in case there are un-necessarily long column indexes
                    // Note: The below subtraction will not lead to undefined behavior as rCodes.length is always > 0
                   if(j < frameIndex.rCodes.length - 1)
                    {
                        // In case column index titles aren't present, printing row index titles alongside the last column index titles line
                        // Note: The below subtraction will not lead to undefined behavior as cCodes.length is always > 0
                        if(i == frameIndex.cCodes.length - 1 && frameIndex.cIndexTitles.length == 0)
                        {
                            // In case the index string is larger than the max size alowed
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
                        // Checking for existance of column index titles before printing the last column index line
                        if(frameIndex.cIndexTitles.length != 0)
                        {
                            // Checking if index is longer than the maximum size
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
                            // Printing row index titles in case the column titles don't exist
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
                                // Printing blank spaces otherwise
                                for(int k = 0; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                    }
                    else
                    {
                        // Displaying the column indexes
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
                            // If the collun indices don't exist, the codes default to indices
                            index2disp = to!string(frameIndex.cCodes[i][j - frameIndex.rCodes.length]);
                        }
                        else
                        {
                            index2disp = frameIndex.cIndices[i][frameIndex.cCodes[i][j - frameIndex.rCodes.length]];
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
                write("\n");
            }
            else if(i == frameIndex.cCodes.length && frameIndex.cIndexTitles.length != 0)
            {
                // In case both column index titles and row index titles, printing row index titles in a new line of it's own 
                for(int j = 0;j < frameIndex.rCodes.length; ++j)
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
                write("\n");
            }
            else
            {
                // Printing row indexes and data
                for(int j = 0; j < forward; ++j)
                {
                    if(j < frameIndex.rCodes.length)
                    {
                        // Skiping index in case of multi-indexing and the index above is came as the current index
                        if(frameIndex.isMultiIndexed == true && dataIndex > 0 && j < frameIndex.rCodes.length - 1
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
                if(backward > 0)
                {
                    write("...  ");
                }

                for(ulong j = colSize.length - backward; j < colSize.length; ++j)
                {
                    // Displaying roww indexes in case they are unnecessarily long
                    if(j < frameIndex.rCodes.length)
                    {
                        // In case of multi-indexing, skipping in case index on top is same
                        if(frameIndex.isMultiIndexed == true && dataIndex > 0 && j < frameIndex.rCodes.length - 1
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
                            // In case indices are impty, the codes will default to indexes
                            if(frameIndex.rIndices[j].length == 0)
                            {
                                dispstr = to!string(dataIndex);
                            }
                            else
                            {
                                dispstr = frameIndex.rIndices[j][frameIndex.rCodes[j][dataIndex]];
                            }

                            // In case the display string is larger than max column size
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
                        // Displaying the data
                        string dispstr = to!string(data[dataIndex][j - frameIndex.rCodes.length]);
                        // Chcking if data string is larger than max column size
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
                ++dataIndex;
                write("\n");
            }
        }
        // writeln(colSize);
        // writeln(forward, "\t", backward);
        // writeln(data);
    }
}
