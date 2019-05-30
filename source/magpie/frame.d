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

        import std.array: appender;
        auto dataAppender = appender!(int[]);
        // Setting column indexes
        foreach(i; 0 .. cast(int)data1d.length)
            dataAppender.put(i);
        frameIndex.cCodes[0] = dataAppender.data;
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

        import std.algorithm: map, max, reduce;
        auto len = data2d.map!(e => e.length).reduce!max;

        // Checking if data2d is empty - second pass
        // In case data 2d resembles: [[], [], []]
        if(len == 0)
        {
            throw new DataFrameExceptions(
                "Expected data to be entered into the DataFrame but recieved array with innermost dimension 0"
            );
        }

        // Flattening the data into a 1D array
        import std.array: appender;
        auto flattner = appender!(T[]);
        foreach(i; 0 .. data2d.length)
        {
            flattner.put(data2d[i]);
            if(data2d[i].length != len)
            {
                // Padding in case the data is not rectangular
                foreach(j; data2d[i].length .. len)
                    flattner.put([T.init]);
            }
        }

        // Resetting index
        frameIndex = Index();
        // Converting data to 2d Slice
        data = flattner.data.sliced(data2d.length, len).universal;
        frameIndex.rIndexTitles = ["Index"];
        frameIndex.rIndices = [[]];
        frameIndex.cIndices = [[]];
        frameIndex.rCodes = [[]];
        frameIndex.cCodes = [[]];

        // Setting default row index
        auto rCodesAppender = appender!(int[]);
        foreach(i; 0 .. cast(int)data2d.length)
            rCodesAppender.put(i);
        frameIndex.rCodes[0] = rCodesAppender.data;

        // Setting default column index
        auto cCodeAppender = appender!(int[]);
        foreach(i; 0 .. cast(int)len)
            cCodeAppender.put(i);
        frameIndex.cCodes[0] = cCodeAppender.data;
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
        import magpie.parser: readCSV;
        auto df = readCSV!T(path, indexDepth, columnDepth, sep);
        frameIndex = df.frameIndex;
        data = df.data;
    }
}

class DataFrameExceptions : Exception
{
    this(string msg, string file = __FILE__, size_t line = __LINE__) {
        super(msg, file, line);
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

/// CSV with correct structure but with gaps in data
unittest
{
    import std.stdio: File;
    import std.string: chomp;
    import std.array: split;

    DataFrame!double df;
    df.from_csv("./test/fromcsv/states_all.csv", 2, 1);
    //df.display();
    df.to_csv("./test/tocsv/ex6tp1.csv");
    File f1 = File("./test/fromcsv/states_all.csv", "r");
    File f2 = File("./test/tocsv/ex6tp1.csv", "r");
    // Comparing fields that are present completely in botht he files
    while(!f1.eof())
    {
        auto f1line = chomp(f1.readln()).split(",");
        auto f2line = chomp(f2.readln()).split(",");
        if(f1line.length > 0)
        {
            assert(f1line[0 .. 3] == f2line[0 .. 3]);
        }
    }
    assert(f1.eof() == f2.eof());
    f1.close();
    f2.close();
}

// Exmaple in readme
unittest
{
    DataFrame!double df;    // This declared a dataframe such that it contains homogeneous data of type double
    df = [[1.2,2.4],[3.6, 4.8]];
    assert(df.data == [1.2,2.4, 3.6, 4.8].sliced(2,2).universal);   // Data is stored as a Universal 2D slice
    // df.display();
    df.to_csv("./test/readmeex.csv");

    DataFrame!double df2;
    df2.from_csv("./test/readmeex.csv", 1, 1);
    // df2.display();

    assert(df == df2);
}
