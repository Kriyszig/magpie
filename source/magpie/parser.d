module magpie.parser;

// Note: Any changes made should be reflected below in the unit test for this function
/// Given a set of indexes and codes in random order, arranges the indices in ascending order and swaps around the codes
void arrangeIndex(string[] indices, int[] code)
{
    // If length of indices is 0, codes themself represent indexing which doesn't require sorting
    if(indices.length == 0)
    {
        return;
    }
    // Selection sort
    foreach(i; 0 .. cast(int)indices.length)
    {
        int pos = i;
        foreach(j; i + 1 .. cast(int)indices.length)
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
        foreach(j; 0 .. code.length)
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

/// Parsing CSV file into a DataFrame
auto readCSV(T)(string path, int indexDepth = 1, int columnDepth = 1, char sep = ',')
{
    import std.stdio: File;
    import std.string: chomp;
    import std.array: split;
    import std.algorithm: countUntil;
    import std.conv: to, ConvException;
    import magpie.index: Index;
    import mir.ndslice: Slice, Universal, sliced, universal;

    // Opening CSV file
    File csvfile = File(path, "r");

    // Initializing values to default
    int i = 0, dataIndex = 0;
    bool containsColTitles = false;

    Index frameIndex = Index();
    T[][] parseddata = [];
    Slice!(T*, 2, Universal) data;

    // Reading the CSV file
    while(!csvfile.eof())
    {
        // Extracting each line
        string[] line = chomp(csvfile.readln()).split(sep);

        // Parsing the area for column index
        if(i < columnDepth)
        {
            // If doesn't contain both column and row titles, the row titles are parsed from the last line of column indexes
            if(i == columnDepth - 1 && indexDepth > 0 && !containsColTitles && line[0].length != 0)
            {
                frameIndex.rIndexTitles = line[0 .. indexDepth];
            }
            else if(!containsColTitles && indexDepth > 0 && line[indexDepth - 1].length != 0)
            {
                // Checking if column index titles occur
                containsColTitles = true;
            }

            // Extracting column titles
            if(containsColTitles)
            {
                frameIndex.cIndexTitles ~= [line[indexDepth - 1]];
            }
            
            // Getting column indexes
            foreach(j; indexDepth .. line.length)
            {
                if(frameIndex.cCodes.length == i)
                {
                    frameIndex.cCodes ~= [[]];
                    frameIndex.cIndices ~= [[]];
                }

                // Finding if the index is already present in cIndices
                immutable int pos = cast(int) countUntil(frameIndex.cIndices[i], line[j]);
                // If the cell is blank, then the dataframe is multi-indexed.
                // Copying code from the previous cell
                if(j > indexDepth && line[j].length == 0)
                {
                    frameIndex.isMultiIndexed = true;
                    frameIndex.cCodes[i] ~= [frameIndex.cCodes[i][j - indexDepth - 1]];
                }
                else if(pos < 0)
                {
                    // Appending to end in case the index doesn't exist already
                    frameIndex.cIndices[i] ~= [line[j]];
                    frameIndex.cCodes[i] ~= [cast(int) frameIndex.cIndices[i].length - 1];
                }
                else
                {
                    // Copying code if the index exists
                    frameIndex.cCodes[i] ~= [pos];
                }
            }
        }
        else if(i == columnDepth && containsColTitles)
        {
            // If contains column index titles then row indexes will be in seperate line
            if(containsColTitles)
            {
                frameIndex.rIndexTitles = line[0 .. indexDepth];
            }
        }
        else if(line.length > 0)
        {
            // In case both row and column index depth is 1, and both rows and column indexes have title then column index titles will be written
            // to row index titles. If the length of line column depth + 1 is 1, then it conatins the row index titles
            if(i == columnDepth && columnDepth == 1 && indexDepth == 1)
            {
                if(line.length == 1)
                {
                    frameIndex.cIndexTitles = frameIndex.rIndexTitles;
                    frameIndex.rIndexTitles = line;
                    containsColTitles = true;
                    continue;
                }
            }

            // Initializing array to hold row indexes and their code
            if(dataIndex == 0)
            {
                foreach(j; 0 .. indexDepth)
                {
                    frameIndex.rIndices ~= [[]];
                    frameIndex.rCodes ~= [[]];
                }
            }
            parseddata ~= [[]];

            foreach(j; 0 .. indexDepth)
            {
                // Chceking is same index is repeated
                immutable int pos = cast(int) countUntil(frameIndex.rIndices[j], line[j]);
                // If a fiels is blank, assuming CSV is multi-indexed
                if(dataIndex > 0 && line[j].length == 0)
                {
                    frameIndex.rCodes[j] ~= [frameIndex.rCodes[j][dataIndex - 1]];
                }
                else if(pos < 0)
                {
                    // Appending new index at end
                    frameIndex.rIndices[j] ~= [line[j]];
                    frameIndex.rCodes[j] ~= [cast(int) frameIndex.rIndices[j].length - 1];
                }
                else
                {
                    // Copying code if index has repeated
                    frameIndex.rCodes[j] ~= [pos];
                }

            }

            // Parsing the core data
            foreach(j; indexDepth .. line.length)
            {
                try
                {
                    parseddata[dataIndex] ~= [to!T(line[j])];
                }
                catch(ConvException e)
                {
                    parseddata[dataIndex] ~= [T.init];
                }
            }

            ++dataIndex;
        }
        ++i;
    }

    // Closing file like a good boi
    csvfile.close();

    // Assigning default indexes for rows
    if(indexDepth == 0)
    {
        frameIndex.rIndexTitles = ["Index"];
        frameIndex.rIndices = [[]];
        frameIndex.rCodes = [[]];
        foreach(j; 0 .. cast(int)parseddata.length)
        {
            frameIndex.rCodes[0] ~= [j];
        }
    }

    // Assigning default indexes for columns
    if(columnDepth == 0)
    {
        frameIndex.cIndices = [[]];
        frameIndex.cCodes = [[]];
        foreach(j; 0 .. cast(int)parseddata[0].length)
        {
            frameIndex.cCodes[0] ~= [j];
        }
        if(indexDepth != 0)
        {
            foreach(j; 0 .. indexDepth)
            {
                frameIndex.rIndexTitles ~= ["Index" ~ to!string(j+1)];
            }
        }
    }

    // Checking for correctness of column indexes
    if(frameIndex.cCodes.length > 1)
    {
        immutable ulong maxindexlen = frameIndex.cCodes[frameIndex.cCodes.length - 1].length;
        foreach(j; 0 .. frameIndex.cCodes.length)
        {
            if(frameIndex.cCodes[j].length > maxindexlen)
            {
                frameIndex.cCodes[j] = frameIndex.cCodes[j][0 .. maxindexlen];
            }
            else
            {
                int paddingele = frameIndex.cCodes[j][frameIndex.cCodes[j].length - 1];
                foreach(k; frameIndex.cCodes[j].length .. maxindexlen)
                {
                    frameIndex.cCodes[j] ~= paddingele;
                }
            }
        }
    }

    // Checking for correctness of row indexes
    if(frameIndex.rCodes.length > 1)
    {
        immutable ulong maxindexlen = frameIndex.rCodes[frameIndex.rCodes.length - 1].length;
        foreach(j; 0 .. frameIndex.rCodes.length)
        {
            if(frameIndex.rCodes[j].length > maxindexlen)
            {
                frameIndex.rCodes[j] = frameIndex.rCodes[j][0 .. maxindexlen];
            }
            else
            {
                int paddingele = frameIndex.rCodes[j][frameIndex.rCodes[j].length - 1];
                foreach(k; frameIndex.rCodes[j].length .. maxindexlen)
                {
                    frameIndex.rCodes[j] ~= paddingele;
                }
            }
        }
    }

    // Arranging indexes in ascending to aid searching
    foreach(j; 0 .. frameIndex.cCodes.length)
    {
        if(frameIndex.cIndices[j].length != 0)
        {
            try
            {
                int[] indexInt = [];
                foreach(k; 0 .. frameIndex.cCodes[j].length)
                {
                    indexInt ~= [to!int(frameIndex.cIndices[j][frameIndex.cCodes[j][k]])];
                }
                frameIndex.cIndices[j] = [];
                frameIndex.cCodes[j] = indexInt;
            }
            catch(ConvException e)
            {
                arrangeIndex(frameIndex.cIndices[j],frameIndex.cCodes[j]);
            }
        }
    }
    
    // Ditto
    foreach(j; 0 .. frameIndex.rCodes.length)
    {
        if(frameIndex.rIndices[j].length != 0)
        {
            try
            {

                int[] indexInt = [];
                foreach(k; 0 .. frameIndex.rCodes[j].length)
                {
                    indexInt ~= [to!int(frameIndex.rIndices[j][frameIndex.rCodes[j][k]])];
                }
                frameIndex.rIndices[j] = [];
                frameIndex.rCodes[j] = indexInt;
            }
            catch(ConvException e)
            {
                arrangeIndex(frameIndex.rIndices[j],frameIndex.rCodes[j]);
            }
        }
    }

    // Flattening the data before making it into a slice
    import std.array: appender;
    auto flattner = appender!(T[]);
    ulong maxlen = frameIndex.cCodes[frameIndex.cCodes.length - 1].length;
    foreach(j; 0 .. parseddata.length)
    {
        if(parseddata[j].length > maxlen)
        {
            flattner.put(parseddata[j][0 .. maxlen]);
        }
        else
        {
            flattner.put(parseddata[j]);
            foreach(k; parseddata[j].length .. maxlen)
            {
                flattner.put(T.init);
            }
        }
    }

    data = flattner.data.sliced(frameIndex.rCodes[0].length, frameIndex.cCodes[0].length).universal;

    // Returning parsed contents as a DataFrame
    import magpie.frame: DataFrame;
    DataFrame!T df;
    df.frameIndex = frameIndex;
    df.data = data;
    return df;
}

unittest
{
    string[] index = ["b","a","d","c"];
    int[] code = [0,1,2,3];
    // Arranging index in ascending order while changing index so the result will still be ["b","a","d","c"]
    arrangeIndex(index, code);
    assert(index == ["a", "b", "c", "d"]);
    assert(code == [1, 0, 3, 2]);
}