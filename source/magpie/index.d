module magpie.index;

/++
Structure for DataFrame Indexing
+/
struct Index
{
private:
    struct Indexing
    {
        string[] titles;
        string[][] index;
        int[][] codes;
    }

public:
    /// To know if data is multi-indexed
    bool isMultiIndexed = false;

    /// Row and Column indexing
    Indexing[2] indexing;

    /++
    void optimize()
    Description: Optimizes a DataFrame - If indexing[0].index can be expressed as integer, converts string indexing[0].index to int and stores in the codes
    +/
    void optimize()
    {
        foreach(i; 0 .. indexing[0].index.length)
        {
            import std.conv: to, ConvException;
            if(indexing[0].index[i].length > 0)
            {
                try
                {
                    import std.array: appender;
                    auto inx = appender!(string[]);
                    foreach(j; 0 .. indexing[0].codes[i].length)
                        inx.put(indexing[0].index[i][indexing[0].codes[i][j]]);
                    int[] codes = to!(int[])(inx.data);
                    indexing[0].codes[i] = codes;
                    indexing[0].index[i] = [];
                }
                catch(ConvException e)
                {
                    import magpie.helper: sortIndex;
                    sortIndex(indexing[0].index[i], indexing[0].codes[i]);
                }
            }
        }

        foreach(i; 0 .. indexing[1].index.length)
        {
            import std.conv: to, ConvException;
            if(indexing[1].index[i].length > 0)
            {
                try
                {
                    import std.array: appender;
                    auto inx = appender!(string[]);
                    foreach(j; 0 .. indexing[1].codes[i].length)
                        inx.put(indexing[1].index[i][indexing[1].codes[i][j]]);
                    int[] codes = to!(int[])(inx.data);
                    indexing[1].codes[i] = codes;
                    indexing[1].index[i] = [];
                }
                catch(ConvException e)
                {
                    import magpie.helper: sortIndex;
                    sortIndex(indexing[1].index[i], indexing[1].codes[i]);
                }
            }
        }
    }

    /++
    void setIndex(Args...)(Args args)
    Description: Method for etting indexing[0].index
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
                indexing[0].titles = args[1];
                indexing[0].index = [args[0]];
                indexing[0].codes = [[]];
                foreach(i; 0 .. cast(int)args[0].length)
                    indexing[0].codes[0] ~= i;
            }
            else static if(is(Args[0] == int[]))
            {
                indexing[0].titles = args[1];
                indexing[0].codes = [args[0]];
                indexing[0].index = [[]];
            }
            else static if(is(Args[0] == string[][]))
            {
                assert(args[0].length > 0, "Can't construct indexing[0].index from empty array");
                size_t len = args[0][0].length;
                assert(len > 0, "Inner dimension cannot be 0");
                foreach(i; 0 .. args[0].length)
                    assert(args[0][i].length == len && len > 0, "Inner dimension of indexing[0].index not equal");
                
                foreach(i; 0 .. args[0].length)
                {
                    indexing[0].index ~= [[]];
                    indexing[0].codes ~= [[]];
                    foreach(j; 0 .. args[0][i].length)
                    {
                        import std.algorithm: countUntil;
                        int pos = cast(int)countUntil(indexing[0].index[i], args[0][i][j]);
                        if(pos > -1)
                        {
                            indexing[0].codes[i] ~= pos;
                        }
                        else
                        {
                            indexing[0].index[i] ~= args[0][i][j];
                            indexing[0].codes[i] ~= cast(int)indexing[0].index[i].length - 1;
                        }
                    }
                }

                indexing[0].titles = args[1];
            }
            else static if(is(Args[0] == int[][]))
            {
                assert(args[0].length > 0, "Can't construct indexing[0].index from empty array");
                size_t len = args[0][0].length;
                assert(len > 0, "Inner dimension cannot be 0");
                foreach(i; 0 .. args[0].length)
                    assert(args[0][i].length == len, "Inner dimension of indexing[0].index not equal");
                indexing[0].titles = args[1];
                indexing[0].codes = args[0];
                foreach(i; 0 .. args[0].length)
                    indexing[0].index ~= [[]];
            }
        }
        
        static if(Args.length > 2)
        {
            static if(is(Args[2] == string[]))
            {
                indexing[1].index = [args[2]];
                indexing[1].codes = [[]];
                foreach(i; 0 .. cast(int)args[2].length)
                    indexing[1].codes[0] ~= i;
            }
            else static if(is(Args[2] == int[]))
            {
                indexing[1].codes = [args[2]];
                indexing[1].index = [[]];
            }
            else static if(is(Args[2] == string[][]))
            {
                assert(args[2].length > 0, "Can't construct indexing[0].index from empty array");
                size_t ilen = args[2][0].length;
                assert(ilen > 0, "Inner dimension cannot be 0");
                foreach(i; 0 .. args[2].length)
                    assert(args[2][i].length == ilen, "Inner dimension of indexing[0].index not equal");
                
                foreach(i; 0 .. args[2].length)
                {
                    indexing[1].index ~= [[]];
                    indexing[1].codes ~= [[]];
                    foreach(j; 0 .. args[2][i].length)
                    {
                        import std.algorithm: countUntil;
                        int pos = cast(int)countUntil(indexing[1].index[i], args[2][i][j]);
                        if(pos > -1)
                        {
                            indexing[1].codes[i] ~= pos;
                        }
                        else
                        {
                            indexing[1].index[i] ~= args[2][i][j];
                            indexing[1].codes[i] ~= cast(int)indexing[1].index[i].length - 1;
                        }
                    }
                }
            }
            else static if(is(Args[2] == int[][]))
            {
                assert(args[2].length > 0, "Can't construct indexing[0].index from empty array");
                size_t ilen = args[2][0].length;
                assert(ilen > 0, "Inner dimension cannot be 0");
                foreach(i; 0 .. args[2].length)
                    assert(args[2][i].length == ilen, "Inner dimension of indexing[0].index not equal");
                indexing[1].codes = args[2];
                foreach(i; 0 .. args[2].length)
                    indexing[1].index ~= [[]];
            }
        }

        static if(Args.length > 3)
        {
            indexing[1].titles = args[3];
        }

        optimize();
    }

    /++
    void extend(int axis, T)(T next)
    Description:Extends indexing[0].index
    @params: axis - 0 for rows, 1 for indexing[1].index
    @params: next - The element to extend element
    +/
    void extend(int axis, T)(T next)
    {
        static if(is(T == int[]))
        {
            static if(axis == 0)
            {
                assert(next.length == indexing[0].codes.length, "Index depth mismatch");
                foreach(i; 0 .. indexing[0].codes.length)
                {
                    if(indexing[0].index[i].length == 0)
                        indexing[0].codes[i] ~= next[i];
                    else
                    {
                        import std.conv: to, ConvException;
                        import std.algorithm: countUntil;
                        string ele = to!string(next[i]);
                        int pos = cast(int)countUntil(indexing[0].index[i], ele);

                        if(pos > -1)
                        {
                            indexing[0].codes[i] ~= pos;
                        }
                        else
                        {
                            indexing[0].index[i] ~= ele;
                            indexing[0].codes[i] ~= cast(int)indexing[0].index[i].length - 1;
                        }
                    }
                }
            }
            else
            {
                assert(next.length == indexing[1].codes.length, "Index depth mismatch");
                foreach(i; 0 .. indexing[1].codes.length)
                {
                    if(indexing[1].index[i].length == 0)
                        indexing[1].codes[i] ~= next[i];
                    else
                    {
                        import std.conv: to, ConvException;
                        import std.algorithm: countUntil;
                        string ele = to!string(next[i]);
                        int pos = cast(int)countUntil(indexing[1].index[i], ele);

                        if(pos > -1)
                        {
                            indexing[1].codes[i] ~= pos;
                        }
                        else
                        {
                            indexing[1].index[i] ~= ele;
                            indexing[1].codes[i] ~= cast(int)indexing[1].index[i].length - 1;
                        }
                    }
                }
            }
        }
        else static if(is(T == string[]))
        {
            static if(axis == 0)
            {
                assert(next.length == indexing[0].codes.length, "Index depth mismatch");
                foreach(i; 0 .. indexing[0].codes.length)
                {
                    if(indexing[0].index[i].length > 0)
                    {
                        import std.algorithm: countUntil;
                        int pos = cast(int)countUntil(indexing[0].index[i], next[i]);

                        if(pos > -1)
                        {
                            indexing[0].codes[i] ~= pos;
                        }
                        else
                        {
                            indexing[0].index[i] ~= next[i];
                            indexing[0].codes[i] ~= cast(int)indexing[0].index[i].length - 1;
                        }
                    }
                    else
                    {
                        import std.conv: to, ConvException;
                        try
                        {
                            int ele = to!int(next[i]);
                            indexing[0].codes[i] ~= ele;
                        }
                        catch(ConvException e)
                        {
                            indexing[0].index[i] = to!(string[])(indexing[0].codes[i]);
                            indexing[0].index[i] ~= next[i];
                            indexing[0].codes[i] = [];
                            foreach(j; 0 .. cast(int)indexing[0].index[i].length)
                                indexing[0].codes[i] ~= j;
                        }
                    }
                }
            }
            else
            {
                assert(next.length == indexing[1].codes.length, "Index depth mismatch");
                foreach(i; 0 .. indexing[1].codes.length)
                {
                    if(indexing[1].index[i].length > 0)
                    {
                        import std.algorithm: countUntil;
                        int pos = cast(int)countUntil(indexing[1].index[i], next[i]);

                        if(pos > -1)
                        {
                            indexing[1].codes[i] ~= pos;
                        }
                        else
                        {
                            indexing[1].index[i] ~= next[i];
                            indexing[1].codes[i] ~= cast(int)indexing[1].index[i].length - 1;
                        }
                    }
                    else
                    {
                        import std.conv: to, ConvException;
                        try
                        {
                            int ele = to!int(next[i]);
                            indexing[1].codes[i] ~= ele;
                        }
                        catch(ConvException e)
                        {
                            indexing[1].index[i] = to!(string[])(indexing[1].codes[i]);
                            indexing[1].index[i] ~= next[i];
                            indexing[1].codes[i] = [];
                            foreach(j; 0 .. cast(int)indexing[1].index[i].length)
                                indexing[1].codes[i] ~= j;
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
    inx.indexing[0].index = [["B", "A"], ["1", "2"]];
    inx.indexing[0].codes = [[0, 1], [0, 1]];
    inx.indexing[1].index = [["B", "A"], ["1", "2"]];
    inx.indexing[1].codes = [[0, 1], [0, 1]];

    inx.optimize();

    assert(inx.indexing[0].index == [["A", "B"], []]);
    assert(inx.indexing[0].codes == [[1, 0], [1, 2]]);
    assert(inx.indexing[1].index == [["A", "B"], []]);
    assert(inx.indexing[1].codes == [[1, 0], [1, 2]]);
}

// Setting integer row index
unittest
{
    Index inx;
    inx.setIndex([1,2,3,4,5,6], ["Index"]);
    assert(inx.indexing[0].index == [[]]);
    assert(inx.indexing[0].titles == ["Index"]);
    assert(inx.indexing[0].codes == [[1,2,3,4,5,6]]);
}

// Setting string row index
unittest
{
    Index inx;
    inx.setIndex(["Hello", "Hi"], ["Index"]);
    assert(inx.indexing[0].index == [["Hello", "Hi"]]);
    assert(inx.indexing[0].titles == ["Index"]);
    assert(inx.indexing[0].codes == [[0, 1]]);
}

// Setting 2D integer index for rows
unittest
{
    Index inx;
    inx.setIndex([[1,2], [3,4]], ["Index", "Index"]);
    assert(inx.indexing[0].index == [[], []]);
    assert(inx.indexing[0].titles == ["Index", "Index"]);
    assert(inx.indexing[0].codes == [[1,2], [3,4]]);
}

// Setting 2D string index for rows
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    assert(inx.indexing[0].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].titles == ["Index", "Index"]);
    assert(inx.indexing[0].codes == [[0,1], [1,0]]);
}

// Setting integer column indexing[0].index
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [1,2,3,4,5]);
    assert(inx.indexing[0].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].titles == ["Index", "Index"]);
    assert(inx.indexing[0].codes == [[0,1], [1,0]]);
    assert(inx.indexing[1].index == [[]]);
    assert(inx.indexing[1].codes == [[1,2,3,4,5]]);
    assert(inx.indexing[1].titles == []);
}

// Setting string column index
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], ["Hello", "Hi"]);
    assert(inx.indexing[0].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].titles == ["Index", "Index"]);
    assert(inx.indexing[0].codes == [[0,1], [1,0]]);
    assert(inx.indexing[1].index == [["Hello", "Hi"]]);
    assert(inx.indexing[1].codes == [[0, 1]]);
    assert(inx.indexing[1].titles == []);
}

// Setting 2D string index for indexing[1].index
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [["Hello", "Hi"], ["Hi", "Hello"]]);
    assert(inx.indexing[0].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].titles == ["Index", "Index"]);
    assert(inx.indexing[0].codes == [[0,1], [1,0]]);
    assert(inx.indexing[1].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[1].codes == [[0,1], [1,0]]);
    assert(inx.indexing[1].titles == []);
}

// Setting 2D integer index for indexing[1].index
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],[[1,2], [3,4]]);
    assert(inx.indexing[0].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].titles == ["Index", "Index"]);
    assert(inx.indexing[0].codes == [[0,1], [1,0]]);
    assert(inx.indexing[1].index == [[], []]);
    assert(inx.indexing[1].codes == [[1,2], [3,4]]);
}

// Setting column titles
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],
        [["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    assert(inx.indexing[0].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].titles == ["Index", "Index"]);
    assert(inx.indexing[0].codes == [[0,1], [1,0]]);
    assert(inx.indexing[1].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[1].codes == [[0,1], [1,0]]);
    assert(inx.indexing[1].titles == ["Index", "Index"]);
}

// Extending indexing[0].index 
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],
        [["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    inx.extend!0(["Hello", "Hi"]);
    assert(inx.indexing[0].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].codes == [[0,1,0], [1,0,1]]);
    inx.extend!1(["Hello", "Hi"]);
    assert(inx.indexing[1].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[1].codes == [[0,1,0], [1,0,1]]);
}

// Extending indexing[0].index that require int to be converted to string
unittest
{
    Index inx;
    inx.setIndex([1,2,3], ["Index"], [1,2,3]);
    assert(inx.indexing[0].codes == [[1,2,3]]);
    assert(inx.indexing[0].index == [[]]);
    assert(inx.indexing[1].codes == [[1,2,3]]);
    assert(inx.indexing[1].index == [[]]);

    // Appending string to integer indexing[0].index
    inx.extend!0(["Hello"]);
    assert(inx.indexing[0].index == [["1","2","3","Hello"]]);
    assert(inx.indexing[0].codes == [[0,1,2,3]]);

    // Appending integer to integer indexing[0].index
    inx.extend!1([4]);
    assert(inx.indexing[1].index == [[]]);
    assert(inx.indexing[1].codes == [[1,2,3,4]]);

    // Appending string to integer indexing[0].index
    inx.extend!1(["Hello"]);
    assert(inx.indexing[1].index == [["1","2","3","4","Hello"]]);
    assert(inx.indexing[1].codes == [[0,1,2,3,4]]);

    // Checking if optimize() is working
    inx.extend!1(["Arrow"]);
    assert(inx.indexing[1].index == [["1","2","3","4","Arrow","Hello"]]);
    assert(inx.indexing[1].codes == [[0,1,2,3,5,4]]);
}

// Extending indexing[0].index with 2D array
unittest
{
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"],
        [["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    inx.extend!0([["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[0].codes == [[0,1,0,0], [1,0,1,1]]);
    inx.extend!1([["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[1].index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(inx.indexing[1].codes == [[0,1,0,0], [1,0,1,1]]);
}
