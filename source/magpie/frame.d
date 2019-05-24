module magpie.frame;

import mir.ndslice;
import magpie.index: Index;

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
        import std.stdio: writeln;
        import magpie.format: formatToString;
        writeln(formatToString!T(frameIndex, data));
        // Displaying DataFrame dimension for user to hanve an understanding of the quantity of data in case the total data was cut-off while displaying
        ulong rows = (frameIndex.cCodes.length + data.shape[0] + ((frameIndex.cIndexTitles.length > 0)?1: 0));
        writeln("Dataframe Dimension: [ ", rows, " X ", frameIndex.rCodes.length + data.shape[1] , " ]");
        writeln("Operable Data Dimension: [ ", data.shape[0], " X ", data.shape[1], " ]");
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

    /++
    df.to_csv(): Method to write dataframe to a CSV file
    +/
    void to_csv(string path, bool writeIndex = true, bool writeColumns = true, char sep = ',')
    {
        import magpie.format: writeasCSV;
        writeasCSV!T(frameIndex, data, path, writeIndex, writeColumns, sep);
    }

    /++
    from_csv(): Parses data  from a structured csv into a DataFrame
    +/
    void from_csv(string path, int indexDepth = 1, int columnDepth = 1, char sep = ',')
    {
        import std.stdio: File;
        import std.string: chomp;
        import std.array: split;
        import std.algorithm: countUntil;
        import std.conv: to, ConvException;

        File csvfile = File(path, "r");
        int i = 0, dataIndex = 0;
        frameIndex = Index();
        T[][] parseddata = [];
        bool containsColTitles = false;

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

                for(int j = indexDepth; j < line.length; ++j)
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
            else if(i == columnDepth && (containsColTitles || (columnDepth == 1 && indexDepth == 1)))
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
                    for(int j = 0; j < indexDepth; ++j)
                    {
                        frameIndex.rIndices ~= [[]];
                        frameIndex.rCodes ~= [[]];
                    }
                }
                parseddata ~= [[]];

                for(int j = 0; j < indexDepth; ++j)
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

                for(int j = indexDepth; j < line.length; ++j)
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
            for(int j = 0; j < parseddata.length; ++j)
            {
                frameIndex.rCodes[0] ~= [j];
            }
        }
        if(columnDepth == 0)
        {
            frameIndex.cIndices = [[]];
            frameIndex.cCodes = [[]];
            for(int j = 0; j < parseddata[0].length; ++j)
            {
                frameIndex.cCodes[0] ~= [j];
            }
            if(indexDepth != 0)
            {
                for(int j = 0 ;j < indexDepth; ++j)
                {
                    frameIndex.rIndexTitles ~= ["Index" ~ to!string(j+1)];
                }
            }
        }

        if(frameIndex.cCodes.length > 1)
        {
            immutable ulong maxindexlen = frameIndex.cCodes[frameIndex.cCodes.length - 1].length;
            for(int j = 0; j < frameIndex.cCodes.length; ++j)
            {
                if(frameIndex.cCodes[j].length > maxindexlen)
                {
                    frameIndex.cCodes[j] = frameIndex.cCodes[j][0 .. maxindexlen];
                }
                else
                {
                    int paddingele = frameIndex.cCodes[j][frameIndex.cCodes[j].length - 1];
                    for(ulong k = frameIndex.cCodes[j].length; k < maxindexlen; ++k)
                    {
                        frameIndex.cCodes[j] ~= paddingele;
                    }
                }
            }
        }

        if(frameIndex.rCodes.length > 1)
        {
            immutable ulong maxindexlen = frameIndex.rCodes[frameIndex.rCodes.length - 1].length;
            for(int j = 0; j < frameIndex.rCodes.length; ++j)
            {
                if(frameIndex.rCodes[j].length > maxindexlen)
                {
                    frameIndex.rCodes[j] = frameIndex.rCodes[j][0 .. maxindexlen];
                }
                else
                {
                    int paddingele = frameIndex.rCodes[j][frameIndex.rCodes[j].length - 1];
                    for(ulong k = frameIndex.rCodes[j].length; k < maxindexlen; ++k)
                    {
                        frameIndex.rCodes[j] ~= paddingele;
                    }
                }
            }
        }

        for(int j = 0; j < frameIndex.cCodes.length; ++j)
        {
            if(frameIndex.cIndices[j].length != 0)
            {
                try
                {
                    int[] indexInt = [];
                    for(int k = 0; k < frameIndex.cCodes[j].length; ++k)
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

        for(int j = 0; j < frameIndex.rCodes.length; ++j)
        {
            if(frameIndex.rIndices[j].length != 0)
            {
                try
                {

                    int[] indexInt = [];
                    for(int k = 0; k < frameIndex.rCodes[j].length; ++k)
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
        for(int j = 0; j < parseddata.length; ++j)
        {
            if(parseddata[j].length > maxlen)
            {
                flatten ~= parseddata[j][0 .. maxlen];
            }
            else
            {
                flatten ~= parseddata[j];
                for(ulong k = parseddata[j].length; k < maxlen; ++k)
                {
                    flatten ~= [T.init];
                }
            }
        }
        data = flatten.sliced(frameIndex.rCodes[0].length, frameIndex.cCodes[0].length).universal;
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

/// writing dataframe to CSV
unittest
{
    import std.stdio: File;
    import std.string: chomp;

    // Creating a dataframe with both multi indexed rows and columns
    DataFrame!int df;
    df = [[1,2,3], [4,5,6]];
    df.frameIndex.rIndexTitles = ["Index1", "Index2"];
    df.frameIndex.rIndices = [[],[]];
    df.frameIndex.rCodes = [[0,1], [0,1]];
    df.frameIndex.cIndexTitles = ["cindex1","cindex2"];
    df.frameIndex.cIndices = [[], []];
    df.frameIndex.cCodes = [[0,1,2], [0,1,2]];
    // df.display();

    // Writing the entire dataframe to the csv file
    df.to_csv("./test/tocsv/ex1tp1.csv");
    File outfile = File("./test/tocsv/ex1tp1.csv", "r");

    int i = 0;
    string[] lines = [",cindex1,0,1,2",",cindex2,0,1,2","Index1,Index2","0,0,1,2,3","1,1,4,5,6",""];
    // Comparing the output with the above expected string
    while (!outfile.eof()) { 
        immutable string line = chomp(outfile.readln()); 
        assert(line == lines[i]);
        ++i;
    }
    outfile.close();

    // Writing the dataframe without column index
    df.to_csv("./test/tocsv/ex1tp2.csv", true, false);
    outfile = File("./test/tocsv/ex1tp2.csv", "r");
    i = 0;
    lines = ["0,0,1,2,3","1,1,4,5,6",""];
    // Comparing the output with the above expected string
    while (!outfile.eof()) { 
        immutable string line = chomp(outfile.readln()); 
        assert(line == lines[i]);
        ++i; 
    }
    outfile.close();

    // Writing dataframe without row index
    df.to_csv("./test/tocsv/ex1tp3.csv", false, true);
    outfile = File("./test/tocsv/ex1tp3.csv", "r");
    i = 0;
    lines = ["0,1,2","0,1,2","1,2,3","4,5,6",""];
    // Comparing the output with the above expected string
    while (!outfile.eof()) { 
        immutable string line = chomp(outfile.readln()); 
        assert(line == lines[i]);
        ++i; 
    }
    outfile.close();

    // Writing only DataFrame data
    df.to_csv("./test/tocsv/ex1tp4.csv", false, false);
    outfile = File("./test/tocsv/ex1tp4.csv", "r");
    i = 0;
    lines = ["1,2,3","4,5,6",""];
    // Comparing the output with the above expected string
    while (!outfile.eof()) { 
        immutable string line = chomp(outfile.readln()); 
        assert(line == lines[i]);
        ++i; 
    }
    outfile.close();
}

/// Writing multi indexed DataFrame to csv
unittest
{
    import std.stdio: File;
    import std.string: chomp;

    // Building a multi-indexed dataframe
    DataFrame!int df;
    df = [[1,2],[3,4]];
    df.frameIndex.isMultiIndexed = true;
    df.frameIndex.cIndexTitles = ["cindex1","cindex2"];
    df.frameIndex.cCodes = [[0,0],[0,1]];
    df.frameIndex.cIndices = [["D"],["Programming", "Language"]];
    df.frameIndex.rIndexTitles = ["index1", "index2"];
    df.frameIndex.rCodes = [[1,1],[0,1]];
    df.frameIndex.rIndices = [[], ["D", "C+++"]];
    // df.display();
    df.to_csv("./test/tocsv/ex2tp1.csv");
    File outfile = File("./test/tocsv/ex2tp1.csv", "r");

    int i = 0;
    string[] lines = [",cindex1,D,",",cindex2,Programming,Language","index1,index2","1,D,1,2",",C+++,3,4",""];
    // Comparing the output with the above expected string
    while (!outfile.eof()) { 
        immutable string line = chomp(outfile.readln()); 
        assert(line == lines[i]);
        ++i;
    }
    outfile.close();

    // Checking if multi-index on innermost level will not lead to skipping
    df.frameIndex.cCodes = [[0,0], [0,0]];
    df.frameIndex.rCodes = [[1,1], [0,0]];
    df.to_csv("./test/tocsv/ex2tp2.csv");
    outfile = File("./test/tocsv/ex2tp2.csv", "r");

    i = 0;
    lines = [",cindex1,D,",",cindex2,Programming,Programming","index1,index2","1,D,1,2",",D,3,4",""];
    // Comparing the output with the above expected string
    while (!outfile.eof()) { 
        immutable string line = chomp(outfile.readln()); 
        assert(line == lines[i]);
        ++i;
    }
    outfile.close();
}

/// Parsing CSV files to DataFrame
unittest
{
    import std.stdio: File;
    DataFrame!int df;
    df.from_csv("./test/tocsv/ex1tp1.csv", 2, 2);
    // df.display();
    df.to_csv("./test/tocsv/ex3tp1.csv");
    File f1 = File("./test/tocsv/ex1tp1.csv", "r");
    File f2 = File("./test/tocsv/ex3tp1.csv", "r");
    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() == f2.eof());
    f1.close();
    f2.close();
    
    df.from_csv("./test/tocsv/ex1tp2.csv", 2, 0);
    // df.display();
    df.to_csv("./test/tocsv/ex3tp2.csv", true, false);
    f1 = File("./test/tocsv/ex1tp2.csv", "r");
    f2 = File("./test/tocsv/ex3tp2.csv", "r");
    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() == f2.eof());
    f1.close();
    f2.close();
    
    df.from_csv("./test/tocsv/ex1tp3.csv", 0, 2);
    // df.display();
    df.to_csv("./test/tocsv/ex3tp3.csv", false, true);
    f1 = File("./test/tocsv/ex1tp3.csv", "r");
    f2 = File("./test/tocsv/ex3tp3.csv", "r");
    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() == f2.eof());
    f1.close();
    f2.close();
    
    df.from_csv("./test/tocsv/ex1tp4.csv", 0, 0);
    //df.display();
    df.to_csv("./test/tocsv/ex3tp4.csv", false, false);
    f1 = File("./test/tocsv/ex1tp4.csv", "r");
    f2 = File("./test/tocsv/ex3tp4.csv", "r");
    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() == f2.eof());
    f1.close();
    f2.close();
}

/// Parsing Kaggle Datasets as PoW
unittest
{
    import std.stdio: File;
    DataFrame!double df;
    df.from_csv("./test/fromcsv/heart.csv", 0, 1);
    // df.display();
    df.to_csv("./test/tocsv/ex4tp1.csv", false);
    File f1 = File("./test/fromcsv/heart.csv", "r");
    File f2 = File("./test/tocsv/ex4tp1.csv", "r");
    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() == f2.eof());
    f1.close();
    f2.close();
}

/// Parsing multi-indexed dataframe with skipped index
unittest
{
    import std.stdio: File;
    DataFrame!double df;
    df.from_csv("./test/tocsv/ex2tp1.csv", 2, 2);
    // df.display();
    df.to_csv("./test/tocsv/ex5tp1.csv");
    File f1 = File("./test/tocsv/ex2tp1.csv", "r");
    File f2 = File("./test/tocsv/ex5tp1.csv", "r");
    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() == f2.eof());
    f1.close();
    f2.close();

    df.from_csv("./test/tocsv/ex2tp2.csv", 2, 2);
    // df.display();
    df.to_csv("./test/tocsv/ex5tp2.csv");
    f1 = File("./test/tocsv/ex2tp2.csv", "r");
    f2 = File("./test/tocsv/ex5tp2.csv", "r");
    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() == f2.eof());
    f1.close();
    f2.close();
}