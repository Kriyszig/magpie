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

    File csvfile = File(path, "r");

    int i = 0, dataIndex = 0;
    Index frameIndex = Index();
    T[][] parseddata = [];
    bool containsColTitles = false;
    Slice!(T*, 2, Universal) data;

    while(!csvfile.eof())
    {
        string[] line = chomp(csvfile.readln()).split(sep);
        if(i < columnDepth)
        {
            if(i == columnDepth - 1 && indexDepth > 0 && !containsColTitles && line[0].length != 0)
            {
                frameIndex.rIndexTitles = line[0 .. indexDepth];
            }
            else if(!containsColTitles && indexDepth > 0 && line[indexDepth - 1].length != 0)
            {
                containsColTitles = true;
            }

            if(containsColTitles)
            {
                frameIndex.cIndexTitles ~= [line[indexDepth - 1]];
            }

            foreach(j; indexDepth .. line.length)
            {
                if(frameIndex.cCodes.length == i)
                {
                    frameIndex.cCodes ~= [[]];
                    frameIndex.cIndices ~= [[]];
                }

                immutable int pos = cast(int) countUntil(frameIndex.cIndices[i], line[j]);
                if(j > indexDepth && line[j].length == 0)
                {
                    frameIndex.isMultiIndexed = true;
                    frameIndex.cCodes[i] ~= [frameIndex.cCodes[i][j - indexDepth - 1]];
                }
                else if(pos < 0)
                {
                    frameIndex.cIndices[i] ~= [line[j]];
                    frameIndex.cCodes[i] ~= [cast(int) frameIndex.cIndices[i].length - 1];
                }
                else
                {
                    frameIndex.cCodes[i] ~= [frameIndex.cCodes[i][j - indexDepth - 1]];
                }
            }
        }
        else if(i == columnDepth && containsColTitles)
        {
            if(columnDepth == 1 && indexDepth == 1)
            {
                if(line.length == 1)
                {
                    frameIndex.cIndexTitles = frameIndex.rIndexTitles;
                    containsColTitles = true;
                }
            }
            if(containsColTitles)
            {
                frameIndex.rIndexTitles = line[0 .. indexDepth];
            }
        }
        else if(line.length > 0)
        {
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
                immutable int pos = cast(int) countUntil(frameIndex.rIndices[j], line[j]);
                if(dataIndex > 0 && line[j].length == 0)
                {
                    frameIndex.rCodes[j] ~= [frameIndex.rCodes[j][dataIndex - 1]];
                }
                else if(pos < 0)
                {
                    frameIndex.rIndices[j] ~= [line[j]];
                    frameIndex.rCodes[j] ~= [cast(int) frameIndex.rIndices[j].length - 1];
                }
                else
                {
                    frameIndex.rCodes[j] ~= [frameIndex.rCodes[j][dataIndex - 1]];
                }

            }

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
    csvfile.close();

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

    T[] flatten = [];
    ulong maxlen = frameIndex.cCodes[frameIndex.cCodes.length - 1].length;
    foreach(j; 0 .. parseddata.length)
    {
        if(parseddata[j].length > maxlen)
        {
            flatten ~= parseddata[j][0 .. maxlen];
        }
        else
        {
            flatten ~= parseddata[j];
            foreach(k; parseddata[j].length .. maxlen)
            {
                flatten ~= [T.init];
            }
        }
    }

    data = flatten.sliced(frameIndex.rCodes[0].length, frameIndex.cCodes[0].length).universal;

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