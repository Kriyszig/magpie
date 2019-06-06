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
    Description: Method for etting indexes
    @params: rowindex - Can be a 1D or 2D array of int or string
    @params: rowindexTitles - 1D array of string
    @params?: columnindex - Can be a 1D or 2D array of int or string
    @params?: columnIndexTitles - 1D array of string
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

    /++
    void extend(int axis, T)(T next)
    Description:Extends indexes
    @params: axis - 0 for rows, 1 for columns
    @params: next - The element to extend element
    +/
    void extend(int axis, T)(T next)
    {
        static if(is(T == int[]))
        {
            static if(axis == 0)
            {
                assert(next.length == rcodes.length, "Index depth mismatch");
                foreach(i; 0 .. rcodes.length)
                {
                    if(indexes[i].length == 0)
                        rcodes[i] ~= next[i];
                    else
                    {
                        import std.conv: to, ConvException;
                        import std.algorithm: countUntil;
                        string ele = to!string(next[i]);
                        int pos = cast(int)countUntil(indexes[i], next[i]);

                        if(pos > -1)
                        {
                            rcodes[i] ~= pos;
                        }
                        else
                        {
                            indexes[i] ~= ele;
                            rcodes[i] ~= cast(int)indexes[i].length - 1;
                        }
                    }
                }
            }
            else
            {
                assert(next.length == ccodes.length, "Index depth mismatch");
                foreach(i; 0 .. ccodes.length)
                {
                    if(columns[i].length == 0)
                        ccodes[i] ~= next[i];
                    else
                    {
                        import std.conv: to, ConvException;
                        import std.algorithm: countUntil;
                        string ele = to!string(next[i]);
                        int pos = cast(int)countUntil(columns[i], ele);

                        if(pos > -1)
                        {
                            ccodes[i] ~= pos;
                        }
                        else
                        {
                            columns[i] ~= ele;
                            ccodes[i] ~= cast(int)columns[i].length - 1;
                        }
                    }
                }
            }
        }
        else static if(is(T == string[]))
        {
            static if(axis == 0)
            {
                assert(next.length == rcodes.length, "Index depth mismatch");
                foreach(i; 0 .. rcodes.length)
                {
                    if(indexes[i].length > 0)
                    {
                        import std.algorithm: countUntil;
                        int pos = cast(int)countUntil(indexes[i], next[i]);

                        if(pos > -1)
                        {
                            rcodes[i] ~= pos;
                        }
                        else
                        {
                            indexes[i] ~= next[i];
                            rcodes[i] ~= cast(int)indexes[i].length - 1;
                        }
                    }
                    else
                    {
                        import std.conv: to, ConvException;
                        try
                        {
                            int ele = to!int(next[i]);
                            rcodes[i] ~= ele;
                        }
                        catch(ConvException e)
                        {
                            indexes[i] = to!(string[])(rcodes[i]);
                            indexes[i] ~= next[i];
                            rcodes[i] = [];
                            foreach(j; 0 .. cast(int)indexes[i].length)
                                rcodes[i] ~= j;
                        }
                    }
                }
            }
            else
            {
                assert(next.length == ccodes.length, "Index depth mismatch");
                foreach(i; 0 .. ccodes.length)
                {
                    if(columns[i].length > 0)
                    {
                        import std.algorithm: countUntil;
                        int pos = cast(int)countUntil(columns[i], next[i]);

                        if(pos > -1)
                        {
                            ccodes[i] ~= pos;
                        }
                        else
                        {
                            columns[i] ~= next[i];
                            ccodes[i] ~= cast(int)columns[i].length - 1;
                        }
                    }
                    else
                    {
                        import std.conv: to, ConvException;
                        try
                        {
                            int ele = to!int(next[i]);
                            ccodes[i] ~= ele;
                        }
                        catch(ConvException e)
                        {
                            columns[i] = to!(string[])(ccodes[i]);
                            columns[i] ~= next[i];
                            ccodes[i] = [];
                            foreach(j; 0 .. cast(int)columns[i].length)
                                ccodes[i] ~= j;
                        }
                    }
                }
            }
        }
        else
        {
            foreach(i; 0 .. next.length)
                extend!axis(next[i]);
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

// Extending indexes 
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],
        [["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    inx.extend!0(["Hello", "Hi"]);
    assert(inx.indexes == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.rcodes == [[0,1,0], [1,0,1]]);
    inx.extend!1(["Hello", "Hi"]);
    assert(inx.columns == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.ccodes == [[0,1,0], [1,0,1]]);
}

// Extending indexes that require int to be converted to string
unittest
{
    Index inx;
    inx.setIndex([1,2,3], ["Index"], [1,2,3]);
    assert(inx.rcodes == [[1,2,3]]);
    assert(inx.indexes == [[]]);
    assert(inx.ccodes == [[1,2,3]]);
    assert(inx.columns == [[]]);

    // Appending string to integer indexes
    inx.extend!0(["Hello"]);
    assert(inx.indexes == [["1","2","3","Hello"]]);
    assert(inx.rcodes == [[0,1,2,3]]);

    // Appending integer to integer indexes
    inx.extend!1([4]);
    assert(inx.columns == [[]]);
    assert(inx.ccodes == [[1,2,3,4]]);

    // Appending string to integer indexes
    inx.extend!1(["Hello"]);
    assert(inx.columns == [["1","2","3","4","Hello"]]);
    assert(inx.ccodes == [[0,1,2,3,4]]);

    // Checking if optimize() is working
    inx.extend!1(["Arrow"]);
    assert(inx.columns == [["1","2","3","4","Arrow","Hello"]]);
    assert(inx.ccodes == [[0,1,2,3,5,4]]);
}

// Extending indexes with 2D array
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],
        [["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    inx.extend!0([["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexes == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.rcodes == [[0,1,0,0], [1,0,1,1]]);
    inx.extend!1([["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.columns == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.ccodes == [[0,1,0,0], [1,0,1,1]]);
}
