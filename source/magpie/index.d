module magpie.index;

/++
Structure for DataFrame Indexing
+/
struct Index
{
    /// To know if data is multi-indexed
    bool isMultiIndexed = false;

    /// Stores title to refer to each index level
    string[] rtitles = [];
    /// The indexes themselves
    string[][] indexes = [];
    /// Codes to map the above index to their positions
    int[][] rcodes = [];

    /// Titles for each column level
    string[] ctitles = [];
    /// The column indexes themself
    string[][] columns = [];
    /// Codes to map the index of above column to their position
    int[][] ccodes = [];

    /++
    void optimize()
    Description: Optimizes a DataFrame - If indexes can be expressed as integer, converts string indexes to int and stores in the codes
    +/
    void optimize()
    {
        foreach(i; 0 .. indexes.length)
        {
            import std.conv: to, ConvException;
            if(indexes[i].length > 0)
            {
                try
                {
                    import std.array: appender;
                    auto inx = appender!(string[]);
                    foreach(j; 0 .. rcodes[i].length)
                        inx.put(indexes[i][rcodes[i][j]]);
                    int[] codes = to!(int[])(inx.data);
                    rcodes[i] = codes;
                    indexes[i] = [];
                }
                catch(ConvException e)
                {
                    import magpie.helper: sortIndex;
                    sortIndex(indexes[i], rcodes[i]);
                }
            }
        }

        foreach(i; 0 .. columns.length)
        {
            import std.conv: to, ConvException;
            if(columns[i].length > 0)
            {
                try
                {
                    import std.array: appender;
                    auto inx = appender!(string[]);
                    foreach(j; 0 .. ccodes[i].length)
                        inx.put(columns[i][ccodes[i][j]]);
                    int[] codes = to!(int[])(inx.data);
                    ccodes[i] = codes;
                    columns[i] = [];
                }
                catch(ConvException e)
                {
                    import magpie.helper: sortIndex;
                    sortIndex(columns[i], ccodes[i]);
                }
            }
        }
    }

    /++
    void setIndex(Args...)(Args args)
    +/
    void setIndex(Args...)(Args args)
        if(Args.length > 1 && Args.length < 5)
    {
        this = Index();

        static if(Args.length > 1)
        {
            static if(is(Args[0] == string[]))
            {
                rtitles = args[1];
                indexes = [args[0]];
                rcodes = [[]];
                foreach(i; 0 .. cast(int)args[0].length)
                    rcodes[0] ~= i;
            }
            else static if(is(Args[0] == int[]))
            {
                rtitles = args[1];
                rcodes = [args[0]];
                indexes = [[]];
            }
            else static if(is(Args[0] == string[][]))
            {
                assert(args[0].length > 0, "Can't construct Indexes from empty array");
                size_t len = args[0][0].length;
                assert(len > 0, "Inner dimension cannot be 0");
                foreach(i; 0 .. args[0].length)
                    assert(args[0][i].length == len && len > 0, "Inner dimension of indexes not equal");
                
                foreach(i; 0 .. args[0].length)
                {
                    indexes ~= [[]];
                    rcodes ~= [[]];
                    foreach(j; 0 .. args[0][i].length)
                    {
                        import std.algorithm: countUntil;
                        int pos = cast(int)countUntil(indexes[i], args[0][i][j]);
                        if(pos > -1)
                        {
                            rcodes[i] ~= pos;
                        }
                        else
                        {
                            indexes[i] ~= args[0][i][j];
                            rcodes[i] ~= cast(int)indexes[i].length - 1;
                        }
                    }
                }

                rtitles = args[1];
            }
            else static if(is(Args[0] == int[][]))
            {
                assert(args[0].length > 0, "Can't construct Indexes from empty array");
                size_t len = args[0][0].length;
                assert(len > 0, "Inner dimension cannot be 0");
                foreach(i; 0 .. args[0].length)
                    assert(args[0][i].length == len, "Inner dimension of indexes not equal");
                rtitles = args[1];
                rcodes = args[0];
                foreach(i; 0 .. args[0].length)
                    indexes ~= [[]];
            }
        }
        
        static if(Args.length > 2)
        {
            static if(is(Args[2] == string[]))
            {
                columns = [args[2]];
                ccodes = [[]];
                foreach(i; 0 .. cast(int)args[2].length)
                    ccodes[0] ~= i;
            }
            else static if(is(Args[2] == int[]))
            {
                ccodes = [args[2]];
                columns = [[]];
            }
            else static if(is(Args[2] == string[][]))
            {
                assert(args[2].length > 0, "Can't construct Indexes from empty array");
                size_t ilen = args[2][0].length;
                assert(ilen > 0, "Inner dimension cannot be 0");
                foreach(i; 0 .. args[2].length)
                    assert(args[2][i].length == ilen, "Inner dimension of indexes not equal");
                
                foreach(i; 0 .. args[2].length)
                {
                    columns ~= [[]];
                    ccodes ~= [[]];
                    foreach(j; 0 .. args[2][i].length)
                    {
                        import std.algorithm: countUntil;
                        int pos = cast(int)countUntil(columns[i], args[2][i][j]);
                        if(pos > -1)
                        {
                            ccodes[i] ~= pos;
                        }
                        else
                        {
                            columns[i] ~= args[2][i][j];
                            ccodes[i] ~= cast(int)columns[i].length - 1;
                        }
                    }
                }
            }
            else static if(is(Args[2] == int[][]))
            {
                assert(args[2].length > 0, "Can't construct Indexes from empty array");
                size_t ilen = args[2][0].length;
                assert(ilen > 0, "Inner dimension cannot be 0");
                foreach(i; 0 .. args[2].length)
                    assert(args[2][i].length == ilen, "Inner dimension of indexes not equal");
                ccodes = args[2];
                foreach(i; 0 .. args[2].length)
                    columns ~= [[]];
            }
        }

        static if(Args.length > 3)
        {
            ctitles = args[3];
        }

        optimize();
    }
}

// Optmization test
unittest
{
    Index inx;
    inx.indexes = [["B", "A"], ["1", "2"]];
    inx.rcodes = [[0, 1], [0, 1]];
    inx.columns = [["B", "A"], ["1", "2"]];
    inx.ccodes = [[0, 1], [0, 1]];

    inx.optimize();

    assert(inx.indexes == [["A", "B"], []]);
    assert(inx.rcodes == [[1, 0], [1, 2]]);
    assert(inx.columns == [["A", "B"], []]);
    assert(inx.ccodes == [[1, 0], [1, 2]]);
}

// Setting integer row index
unittest
{
    Index inx;
    inx.setIndex([1,2,3,4,5,6], ["Index"]);
    assert(inx.indexes == [[]]);
    assert(inx.rtitles == ["Index"]);
    assert(inx.rcodes == [[1,2,3,4,5,6]]);
}

// Setting string row index
unittest
{
    Index inx;
    inx.setIndex(["Hello", "Hi"], ["Index"]);
    assert(inx.indexes == [["Hello", "Hi"]]);
    assert(inx.rtitles == ["Index"]);
    assert(inx.rcodes == [[0, 1]]);
}

// Setting 2D integer index for rows
unittest
{
    Index inx;
    inx.setIndex([[1,2], [3,4]], ["Index", "Index"]);
    assert(inx.indexes == [[], []]);
    assert(inx.rtitles == ["Index", "Index"]);
    assert(inx.rcodes == [[1,2], [3,4]]);
}

// Setting 2D string index for rows
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    assert(inx.indexes == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.rtitles == ["Index", "Index"]);
    assert(inx.rcodes == [[0,1], [1,0]]);
}

// Setting integer column indexes
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [1,2,3,4,5]);
    assert(inx.indexes == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.rtitles == ["Index", "Index"]);
    assert(inx.rcodes == [[0,1], [1,0]]);
    assert(inx.columns == [[]]);
    assert(inx.ccodes == [[1,2,3,4,5]]);
    assert(inx.ctitles == []);
}

// Setting string column index
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], ["Hello", "Hi"]);
    assert(inx.indexes == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.rtitles == ["Index", "Index"]);
    assert(inx.rcodes == [[0,1], [1,0]]);
    assert(inx.columns == [["Hello", "Hi"]]);
    assert(inx.ccodes == [[0, 1]]);
    assert(inx.ctitles == []);
}

// Setting 2D string index for columns
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [["Hello", "Hi"], ["Hi", "Hello"]]);
    assert(inx.indexes == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.rtitles == ["Index", "Index"]);
    assert(inx.rcodes == [[0,1], [1,0]]);
    assert(inx.columns == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.ccodes == [[0,1], [1,0]]);
    assert(inx.ctitles == []);
}

// Setting 2D integer index for columns
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],[[1,2], [3,4]]);
    assert(inx.indexes == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.rtitles == ["Index", "Index"]);
    assert(inx.rcodes == [[0,1], [1,0]]);
    assert(inx.columns == [[], []]);
    assert(inx.ccodes == [[1,2], [3,4]]);
}

// Setting column titles
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],
        [["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    assert(inx.indexes == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.rtitles == ["Index", "Index"]);
    assert(inx.rcodes == [[0,1], [1,0]]);
    assert(inx.columns == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.ccodes == [[0,1], [1,0]]);
    assert(inx.ctitles == ["Index", "Index"]);
}
