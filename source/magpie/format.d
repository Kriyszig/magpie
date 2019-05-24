module magpie.format;

import magpie.index: Index;
import mir.ndslice: Slice, Universal;

/++
display(T)(frameIndex, data, terminalw): Function that formats the data frame data to a termianl friendly formateed string.
+/
string formatToString(T)(Index frameIndex, Slice!(T*, 2, Universal) data, int terminalw = 0)
{
    // Checking if the given dataframe is empty
    if(data.shape[0] == 0 || data.shape[1] == 0)
    {
        return "The DataFrame is empty\n";
    }

    import std.conv: to;
    immutable int maxColSize = 45;                                                              // Maximum size of a column. If this size is excedded ... is added at the end of data
    immutable int terminalWidth = ((terminalw > 100)?terminalw: 200);                           // Terminal width to display correct form in all cases
    immutable ulong rindexDepth = frameIndex.rCodes.length;                                     // Cells from left, row indexes will occupy
    immutable ulong cindexDepth = frameIndex.cCodes.length;                                     // Cells from top column indexes will occupy
    ulong[] colSize;                                                                            // An array storing the max size of each column to indent the data properly
    string returnstr = "";

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
                                    returnstr ~= frameIndex.rIndexTitles[j][0 .. colSize[j] - 3] ~ "...  ";
                                }
                                else
                                {
                                    returnstr ~= frameIndex.rIndexTitles[j];
                                    for(ulong  k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                    {
                                        returnstr ~= " ";
                                    }
                                }
                            }
                            else
                            {
                                // Printing blank spaces otherwise
                                for(int k = 0; k < colSize[j] + 2; ++k)
                                {
                                    returnstr ~= " ";
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
                                    returnstr ~= frameIndex.cIndexTitles[i][0 .. maxColSize - 3] ~  "...  ";
                                }
                                else
                                {
                                    returnstr ~= frameIndex.cIndexTitles[i];
                                    for(ulong k = frameIndex.cIndexTitles[i].length; k < colSize[j] + 2; ++k)
                                    {
                                        returnstr ~= " ";
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
                                        returnstr ~= frameIndex.rIndexTitles[j][0 .. colSize[j] - 3] ~ "...  ";
                                    }
                                    else
                                    {
                                        returnstr ~= frameIndex.rIndexTitles[j];
                                        for(ulong  k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                        {
                                            returnstr ~= " ";
                                        }
                                    }
                                }
                                else
                                {
                                    for(int k = 0; k < colSize[j] + 2; ++k)
                                    {
                                        returnstr ~= " ";
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
                            returnstr ~= index2disp;
                            for(ulong k = index2disp.length; k < colSize[j] + 2; ++k)
                            {
                                returnstr ~= " ";
                            }
                        }

                    }

                    // In case the entire dataframe cannot be displayed in the terminal, the dataframe will be displayed partially from left to right
                    // and partially from right to left till the middle of the screen
                    if(backward > 0 && inx == 0)
                    {
                        // Adding the continuation dots
                        returnstr ~= "...  ";
                    }
                }
                returnstr = returnstr[0 .. $ - 2];
                returnstr ~= "\n";
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
                        returnstr ~= frameIndex.rIndexTitles[j][0 .. colSize[j] - 3] ~ "...  ";
                    }
                    else
                    {
                        returnstr ~= frameIndex.rIndexTitles[j];
                        for(ulong k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                        {
                            returnstr ~= " ";
                        }
                    }
                }
                if(stop != frameIndex.rCodes.length)
                {
                    returnstr ~= "...  ";
                    for(ulong j = colSize.length - backward; j < frameIndex.rCodes.length; ++j)
                    {
                        if(frameIndex.rIndexTitles[j].length > colSize[j])
                        {
                            returnstr ~= frameIndex.rIndexTitles[j][0 .. colSize[j] - 3] ~ "...  ";
                        }
                        else
                        {
                            returnstr ~= frameIndex.rIndexTitles[j];
                            for(ulong k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                            {
                                returnstr ~= " ";
                            }
                        }
                    }
                }
                returnstr ~="\n";
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
                                    returnstr ~= " ";
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
                                    returnstr ~= dispstr[0 .. colSize[j] - 3] ~ "...  ";
                                }
                                else
                                {
                                    returnstr ~= dispstr;
                                    for(ulong k = dispstr.length; k < colSize[j] + 2; ++k)
                                    {
                                        returnstr ~= " ";
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
                                returnstr ~= dispstr[0 .. colSize[j] - 3] ~  "...  ";
                            }
                            else
                            {
                                returnstr ~= dispstr;
                                for(ulong k = dispstr.length; k < colSize[j] + 2; ++k)
                                {
                                    returnstr ~= " ";
                                }
                            }
                        }
                    }

                    // Adding continuation dots in case the dataframe cannot be fit in
                    if(backward > 0 && inx == 0)
                    {
                        returnstr ~= "...  ";
                    }
                }
                ++dataIndex;
                returnstr = returnstr[0 .. $ - 2];
                returnstr ~= "\n";
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
                        returnstr ~= ".";
                    }
                    returnstr ~= "  ";
                }

                if(inx == 0 && backward > 0)
                {
                    returnstr ~= "...  ";
                }
            }
            returnstr = returnstr[0 .. $ - 2];
            returnstr ~= "\n";
        }
    }
    
    return returnstr;
}

void writeasCSV(T)(Index frameIndex, Slice!(T*, 2, Universal) data, string path, bool writeIndex = true, bool writeColumns = true, char sep = ',')
{
    import std.stdio: File;
    File outputfile = File(path, "w");
    // Terminating in case the DataFrame is empty. I will still open the file nonetheless and write nothing to signify empty dataframe
    if(data.shape[0] == 0 || data.shape[1] == 0)
    {
        outputfile.close();
        return;
    }

    // In case writeColumns is enabled
    if(writeColumns)
    {  
        // Printing column indexes
        // Note: The below subtraction from ulong will not lead to undefined behavior as frameIndex.cCodes.length is always > 1
        for(int i = 0; i < frameIndex.cCodes.length - 1; ++i)
        {
            // Leaving white spaces and printing column index titles in case writeIndex is also enabled
            if(writeIndex)
            {
                for(int j = 0; j < frameIndex.rCodes.length - 1; ++j)
                {
                    outputfile.write(sep);
                }

                if(frameIndex.cIndexTitles.length == 0)
                {
                    outputfile.write(sep);
                }
                else
                {
                    outputfile.write(frameIndex.cIndexTitles[i], sep);
                }
            }

            // Writing column titles
            for(int j = 0; j < frameIndex.cCodes[i].length; ++j)
            {
                // If cIndices for particular level doesn't exist, rCodes will become default indexes
                if(frameIndex.cIndices[i].length == 0)
                {
                    if(frameIndex.isMultiIndexed && j > 0 && frameIndex.cCodes[i][j] == frameIndex.cCodes[i][j - 1])
                    {
                        if(j < frameIndex.cCodes[i].length - 1)
                        {
                            outputfile.write(sep);
                        }
                    }
                    else if(j < frameIndex.cCodes[i].length - 1)
                    {
                        outputfile.write(frameIndex.cCodes[i][j], sep);
                    }
                    else
                    {
                        outputfile.write(frameIndex.cCodes[i][j]);
                    }
                }
                else
                {
                    if(frameIndex.isMultiIndexed && j > 0 && frameIndex.cCodes[i][j] == frameIndex.cCodes[i][j - 1])
                    {
                        if(j < frameIndex.cCodes[i].length - 1)
                        {
                            outputfile.write(sep);
                        }
                    }
                    else if(j < frameIndex.cCodes[i].length - 1)
                    {
                        outputfile.write(frameIndex.cIndices[i][frameIndex.cCodes[i][j]], sep);
                    }
                    else
                    {
                        outputfile.write(frameIndex.cIndices[i][frameIndex.cCodes[i][j]]);
                    }
                }
            }

            outputfile.write("\n");
        }
        
        // Last level of column indexes
        for(int i = 0; i < frameIndex.rCodes.length - 1; ++i)
        {
            // If writing row index is enabled, eriting row title in last level of column indexes if column index titles doesn't exist
            if(writeIndex && frameIndex.cIndexTitles.length == 0)
            {
                outputfile.write(frameIndex.rIndexTitles[i], sep);
            }
            else if(writeIndex)
            {
                outputfile.write(sep);
            }
        }

        if(writeIndex && frameIndex.cIndexTitles.length == 0)
        {
            outputfile.write(frameIndex.rIndexTitles[frameIndex.rIndexTitles.length - 1], sep);
        }
        else if(writeIndex && frameIndex.cIndexTitles.length != 0)
        {
            outputfile.write(frameIndex.cIndexTitles[frameIndex.cIndexTitles.length - 1], sep);
        }
        
        // Writing last level of column index
        ulong i = frameIndex.cCodes.length - 1;
        for(int j = 0; j < frameIndex.cCodes[i].length; ++j)
        {
            if(frameIndex.cIndices[i].length == 0)
            {
                if(j < frameIndex.cCodes[i].length - 1)
                {
                    outputfile.write(frameIndex.cCodes[i][j], sep);
                }
                else
                {
                    outputfile.write(frameIndex.cCodes[i][j]);
                }
            }
            else
            {
                if(j < frameIndex.cCodes[i].length - 1)
                {
                    outputfile.write(frameIndex.cIndices[i][frameIndex.cCodes[i][j]], sep);
                }
                else
                {
                    outputfile.write(frameIndex.cIndices[i][frameIndex.cCodes[i][j]]);
                }
            }
        }

        outputfile.write("\n");
    }

    // wRiting row index title in seperate ine in case column index title exist and user needs it to be written to file
    if(writeIndex && writeColumns && frameIndex.cIndexTitles.length != 0)
    {
        outputfile.write(frameIndex.rIndexTitles[0]);
        for(int i = 1; i < frameIndex.rIndexTitles.length; ++i)
        {
            outputfile.write(sep, frameIndex.rIndexTitles[i]);
        }
        outputfile.write("\n");
    }

    // Writing row indexes? and data
    for(int i = 0; i < data.shape[0]; ++i)
    {
        // Checking if user wants indexing to be written to file
        if(writeIndex)
        {
            for(int j = 0; j < frameIndex.rCodes.length; ++j)
            {
                if(frameIndex.isMultiIndexed && i > 0 && j < frameIndex.rCodes.length - 1
                && frameIndex.rCodes[j][i] == frameIndex.rCodes[j][i - 1])
                {
                    outputfile.write(sep);
                }
                else if(frameIndex.rIndices[j].length == 0)
                {
                    outputfile.write(frameIndex.rCodes[j][i], sep);
                }
                else
                {
                    outputfile.write(frameIndex.rIndices[j][frameIndex.rCodes[j][i]], sep);
                }
            }
        }

        // Writing data to file
        outputfile.write(data[i][0]);
        for(int j = 1; j < data.shape[1]; ++j)
        {
            outputfile.write(sep, data[i][j]);
        }

        outputfile.write("\n");
    }

    // Closing file once operation is done
    outputfile.close();
}

/// Basic DataFrame
unittest
{
    import magpie.frame: DataFrame;
    DataFrame!int df;
    df = [1,2,3,4,5,6];
    immutable string retstr = "Index  0  1  2  3  4  5\n0      1  2  3  4  5  6\n";
    assert(formatToString!int(df.frameIndex, df.data, 200) == retstr);
}

/// Empty DataFrame
unittest
{
    import magpie.frame: DataFrame;
    DataFrame!double empty;
    immutable string retstr = "The DataFrame is empty\n";
    assert(formatToString!double(empty.frameIndex, empty.data, 200) == retstr);
}

/// A simple but larger DataFrame
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double simpleEx;
    simpleEx.frameIndex.rIndexTitles = ["Index"];
    simpleEx.frameIndex.rCodes = [[0,1,2,3]];
    simpleEx.frameIndex.rIndices = [[]];
    simpleEx.frameIndex.cCodes = [[0,1,2,3]];
    simpleEx.frameIndex.cIndices = [[]];
    double[] data;
    for(int i = 0 ; i < 16; ++i)
    {
        data ~= [3.92387e+179];
    }
    simpleEx.data = (data).sliced(4,4).universal;

    immutable string retstr = "Index  0             1             2             3           \n"
    ~ "0      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "1      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "2      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "3      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n";
    assert(formatToString!double(simpleEx.frameIndex, simpleEx.data, 200) == retstr);
}

/// Wide DataFrame
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double largeEx;
    largeEx.frameIndex.rIndexTitles = ["Index"];
    largeEx.frameIndex.rCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    largeEx.frameIndex.rIndices = [[]];
    largeEx.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    largeEx.frameIndex.cIndices = [[]];
    double[] data;
    for(int i = 0 ; i < 225; ++i)
    {
        data ~= [3.92387e+179];
    }
    largeEx.data = (data).sliced(15,15).universal;

    immutable string retstr = "Index  0             1             2             3             4             5             ..."
    ~ "  8             9             10            11            12            13            14          \n"
    ~ "0      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "1      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "2      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "3      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "4      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "5      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "6      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "7      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "8      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "9      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "10     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "11     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "12     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "13     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "14     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179"
    ~ "  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n";
    assert(formatToString!double(largeEx.frameIndex, largeEx.data, 200) == retstr);
}

/// DataFrame with column title
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double both;
    both.frameIndex.rIndexTitles = ["Index"];
    both.frameIndex.rCodes = [[0,1,2,3]];
    both.frameIndex.rIndices = [[]];
    both.frameIndex.cIndexTitles = ["Column Index:"];
    both.frameIndex.cCodes = [[0,1,2,3]];
    both.frameIndex.cIndices = [[]];
    double[] data;
    for(int i = 0 ; i < 16; ++i)
    {
        data ~= [3.92387e+179];
    }
    both.data = (data).sliced(4,4).universal;

    immutable string retstr = "Column Index:  0             1             2             3           \n"
    ~ "Index          \n"
    ~ "0              3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "1              3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "2              3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "3              3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n";
    assert(formatToString!double(both.frameIndex, both.data, 200) == retstr);
}

/// Multi-Indexed Rows
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double mirows;
    mirows.frameIndex.rIndexTitles = ["Index1", "Index2"];
    mirows.frameIndex.rCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14],[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mirows.frameIndex.rIndices = [[], []];
    mirows.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mirows.frameIndex.cIndices = [[]];
    double[] data;
    for(int i = 0 ; i < 255; ++i)
    {
        data ~= [3.92387e+179];
    }
    mirows.data = (data).sliced(15,15).universal;

    immutable string retstr = "Index1  Index2  0             1             2             3             4             ...  8             9             10            11            12            13            14          \n"
    ~ "0       0       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "1       1       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "2       2       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "3       3       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "4       4       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "5       5       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "6       6       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "7       7       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "8       8       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "9       9       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "10      10      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "11      11      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "12      12      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "13      13      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "14      14      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n";
    assert(formatToString!double(mirows.frameIndex, mirows.data, 200) == retstr);

}

/// Multi-Indexed columns
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double mic;
    mic.frameIndex.rIndexTitles = ["Index1"];
    mic.frameIndex.rCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mic.frameIndex.rIndices = [[], []];
    mic.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14],[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mic.frameIndex.cIndices = [[],[]];
    double[] data;
    for(int i = 0 ; i < 255; ++i)
    {
        data ~= [3.92387e+179];
    }
    mic.data = (data).sliced(15,15).universal;

    immutable string retstr = "        0             1             2             3             4             5             ...  8             9             10            11            12            13            14          \n"
    ~ "Index1  0             1             2             3             4             5             ...  8             9             10            11            12            13            14          \n"
    ~ "0       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "1       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "2       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "3       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "4       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "5       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "6       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "7       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "8       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "9       3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "10      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "11      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "12      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "13      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "14      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n";

    assert(formatToString!double(mic.frameIndex, mic.data, 200) == retstr);
}

/// Multi-Indexed Columns with column title
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double mict;
    mict.frameIndex.rIndexTitles = ["Index1"];
    mict.frameIndex.rCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mict.frameIndex.rIndices = [[], []];
    mict.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14],[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mict.frameIndex.cIndexTitles = ["CIndex1:", "Cindex2:"];
    mict.frameIndex.cIndices = [[],[]];
    double[] data;
    for(int i = 0 ; i < 255; ++i)
    {
        data ~= [3.92387e+179];
    }
    mict.data = (data).sliced(15,15).universal;

    immutable string retstr = "CIndex1:  0             1             2             3             4             5             ...  8             9             10            11            12            13            14          \n"
    ~ "Cindex2:  0             1             2             3             4             5             ...  8             9             10            11            12            13            14          \n"
    ~ "Index1    \n"
    ~ "0         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "1         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "2         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "3         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "4         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "5         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "6         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "7         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "8         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "9         3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "10        3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "11        3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "12        3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "13        3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "14        3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n";

    assert(formatToString!double(mict.frameIndex, mict.data, 200) == retstr);
}

/// DataFrame with field larger than maximum column limit
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double simpleEx;
    simpleEx.frameIndex.rIndexTitles = ["IndexIndexIndexIndexIndexIndexIndexIndexIndexIndexIndexIndex"];
    simpleEx.frameIndex.rCodes = [[0,1,2,3]];
    simpleEx.frameIndex.rIndices = [[]];
    simpleEx.frameIndex.cCodes = [[0,1,2,3]];
    simpleEx.frameIndex.cIndices = [[]];
    double[] data;
    for(int i = 0 ; i < 16; ++i)
    {
        data ~= [3.92387e+179];
    }
    simpleEx.data = (data).sliced(4,4).universal;

    immutable string retstr = "IndexIndexIndexIndexIndexIndexIndexIndexIn...  0             1             2             3           \n"
    ~ "0                                              3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "1                                              3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "2                                              3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "3                                              3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n";

    assert(formatToString!double(simpleEx.frameIndex, simpleEx.data, 200) == retstr);
}

/// Multi-Indexed rows and columns
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double ex1;
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,2,3,0],[1,2,3,5_555_555]];
    //ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,1,2,3],[0,1,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    double[] data;
    for(int i = 0 ; i < 16; ++i)
    {
        data ~= [3.92387e+179];
    }
    ex1.data = (data).sliced(4,4).universal;

    immutable string retstr = "                     d             d lang        d programming lang  C+++        \n"
    ~ "Index       Index2   d             d lang        d programming lang  C+++        \n"
    ~ "yoloy       1        3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "danndo      2        3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "jjjjjjjjjj  3        3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "yo          5555555  3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n";

    assert(formatToString!double(ex1.frameIndex, ex1.data, 200) == retstr);
}

/// Multi-Indexed rows and columns with column titles
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double ex1;
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,2,3,0],[1,2,3,5_555_555]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,1,2,3],[0,1,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    double[] data;
    for(int i = 0 ; i < 16; ++i)
    {
        data ~= [3.92387e+179];
    }
    ex1.data = (data).sliced(4,4).universal;

    immutable string retstr = "            Language        d             d lang        d programming lang  C+++        \n"
    ~ "            Language Again  d             d lang        d programming lang  C+++        \n"
    ~ "Index       Index2          \n"
    ~ "yoloy       1               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "danndo      2               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "jjjjjjjjjj  3               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "yo          5555555         3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n";

    assert(formatToString!double(ex1.frameIndex, ex1.data, 200) == retstr);
}

/// Multi-Indexed rows with skipping similar adjacent indices
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double ex1;
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,1,0,0],[1,2,3,5_555_555]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,1,2,3],[0,1,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    double[] data;
    for(int i = 0 ; i < 16; ++i)
    {
        data ~= [3.92387e+179];
    }
    ex1.data = (data).sliced(4,4).universal;

    immutable string retstr = "       Language        d             d lang        d programming lang  C+++        \n"
    ~ "       Language Again  d             d lang        d programming lang  C+++        \n"
    ~ "Index  Index2          \n"
    ~ "yoloy  1               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "       2               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "yo     3               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "       5555555         3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n";

    assert(formatToString!double(ex1.frameIndex, ex1.data, 200) == retstr);
}

/// Multi-Indexed rows and columns skipping adjacent indices
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double ex1;
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,1,0,0],[1,2,3,5_555_555]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,0,2,3],[0,1,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    double[] data;
    for(int i = 0 ; i < 16; ++i)
    {
        data ~= [3.92387e+179];
    }
    ex1.data = (data).sliced(4,4).universal;

    immutable string retstr = "       Language        d                           d programming lang  C+++        \n"
    ~ "       Language Again  d             d lang        d programming lang  C+++        \n"
    ~ "Index  Index2          \n"
    ~ "yoloy  1               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "       2               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "yo     3               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "       5555555         3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n";

    assert(formatToString!double(ex1.frameIndex, ex1.data, 200) == retstr);
}

/// Multi-Indexing on innermost level will not lead to skipping of indices
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double ex1;
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,1,0,0],[1,1,3,5_555_555]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,0,2,3],[0,0,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    double[] data;
    for(int i = 0 ; i < 16; ++i)
    {
        data ~= [3.92387e+179];
    }
    ex1.data = (data).sliced(4,4).universal;

    immutable string retstr = "       Language        d                           d programming lang  C+++        \n"
    ~ "       Language Again  d             d             d programming lang  C+++        \n"
    ~ "Index  Index2          \n"
    ~ "yoloy  1               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "       1               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "yo     3               3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n"
    ~ "       5555555         3.92387e+179  3.92387e+179  3.92387e+179        3.92387e+179\n";

    assert(formatToString!double(ex1.frameIndex, ex1.data, 200) == retstr);
}

/// Tall DataFrame
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double largeEx;
    largeEx.frameIndex.rIndexTitles = ["Index"];
    largeEx.frameIndex.rCodes = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
    53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63]];
    largeEx.frameIndex.rIndices = [[]];
    largeEx.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    largeEx.frameIndex.cIndices = [[]];
    double[] data;
    for(int i = 0 ; i < 960; ++i)
    {
        data ~= [3.92387e+179];
    }
    largeEx.data = (data).sliced(64,15).universal;

    immutable string retstr = "Index  0             1             2             3             4             5             ...  8             9             10            11            12            13            14          \n"
    ~ "0      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "1      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "2      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "3      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "4      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "5      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "6      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "7      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "8      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "9      3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "10     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "11     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "12     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "13     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "14     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "15     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "16     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "17     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "18     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "19     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "20     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "21     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "22     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "23     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ ".....  ............  ............  ............  ............  ............  ............  ...  ............  ............  ............  ............  ............  ............  ............\n"
    ~ "39     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "40     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "41     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "42     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "43     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "44     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "45     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "46     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "47     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "48     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "49     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "50     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "51     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "52     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "53     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "54     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "55     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "56     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "57     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "58     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "59     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "60     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "61     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "62     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n"
    ~ "63     3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  ...  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179  3.92387e+179\n";

    assert(formatToString!double(largeEx.frameIndex, largeEx.data, 200) == retstr);
}

/// DataFrame with unnecessarily long indexing
unittest
{
    import magpie.frame: DataFrame;
    import mir.ndslice: sliced, universal;

    DataFrame!double ex1;
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["IndexIndexIndexIndexIndexIndex", "IndexIndexIndexIndexIndexIndexIndex2",
    "IndexIndexIndexIndexIndexIndex3", "IndexIndexIndexIndexIndexIndex4", "Index5"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[],[],[],[]];
    ex1.frameIndex.rCodes = [[1,1,0,0],[1,2,3,5_555_555],[1,2,3,4],[1,2,3,4],[1,2,3,4]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,0],[0,1]];
    ex1.frameIndex.cIndices = [["d"],["d programming lang","C+++"]];
    double[] data;
    for(int i = 0 ; i < 8; ++i)
    {
        data ~= [3.92387e+179];
    }
    ex1.data = (data).sliced(4,2).universal;

    immutable string retstr = "                                                                      ...                                   Language        d                               \n"
    ~ "                                                                      ...                                   Language Again  d programming lang  C+++        \n"
    ~ "IndexIndexIndexIndexIndexIndex  IndexIndexIndexIndexIndexIndexIndex2  ...  IndexIndexIndexIndexIndexIndex4  Index5          \n"
    ~ "yoloy                           1                                     ...  1                                1               3.92387e+179        3.92387e+179\n"
    ~ "                                2                                     ...  2                                2               3.92387e+179        3.92387e+179\n"
    ~ "yo                              3                                     ...  3                                3               3.92387e+179        3.92387e+179\n"
    ~ "                                5555555                               ...  4                                4               3.92387e+179        3.92387e+179\n";

    assert(formatToString!double(ex1.frameIndex, ex1.data, 200) == retstr);
}