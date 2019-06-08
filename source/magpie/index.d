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
        foreach(k; 0 .. 2)
        {
            foreach(i; 0 .. indexing[k].index.length)
            {
                import std.conv: to, ConvException;
                if(indexing[k].index[i].length > 0)
                {
                    try
                    {
                        import std.array: appender;
                        auto inx = appender!(string[]);
                        foreach(j; 0 .. indexing[k].codes[i].length)
                            inx.put(indexing[k].index[i][indexing[k].codes[i][j]]);
                        int[] codes = to!(int[])(inx.data);
                        indexing[k].codes[i] = codes;
                        indexing[k].index[i] = [];
                    }
                    catch(ConvException e)
                    {
                        import magpie.helper: sortIndex;
                        sortIndex(indexing[k].index[i], indexing[k].codes[i]);
                    }
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

        static foreach(j; 0 .. 2)
        {
            static if(Args.length > 2 * j)
            {
                static if(is(Args[j * 2] == string[]))
                {
                    indexing[j].index = [args[j * 2]];
                    indexing[j].codes = [[]];
                    foreach(i; 0 .. args[j * 2].length)
                        indexing[j].codes[0] ~= cast(int)i;
                }
                else static if(is(Args[j * 2] == int[]))
                {
                    indexing[j].codes = [args[j * 2]];
                    indexing[j].index = [[]];
                }
                else static if(is(Args[j* 2] == string[][]))
                {
                    assert(args[j * 2].length > 0, "Can't construct indexing[0].index from empty array");
                    assert(args[j * 2][0].length > 0, "Inner dimension cannot be 0");
                    foreach(i; 0 .. args[j * 2].length)
                        assert(args[j * 2][i].length == args[j * 2][0].length && args[j * 2][0].length > 0, "Inner dimension of indexing[0].index not equal");
                    
                    foreach(i; 0 .. args[j * 2].length)
                    {
                        indexing[j].index ~= [[]];
                        indexing[j].codes ~= [[]];
                        foreach(k; 0 .. args[j * 2][i].length)
                        {
                            import std.algorithm: countUntil;
                            int pos = cast(int)countUntil(indexing[j].index[i], args[j * 2][i][k]);
                            if(pos > -1)
                            {
                                indexing[j].codes[i] ~= pos;
                            }
                            else
                            {
                                indexing[j].index[i] ~= args[j * 2][i][k];
                                indexing[j].codes[i] ~= cast(int)indexing[j].index[i].length - 1;
                            }
                        }
                    }
                }
                else static if(is(Args[j * 2] == int[][]))
                {
                    assert(args[j * 2].length > 0, "Can't construct indexing[0].index from empty array");
                    size_t len = args[j * 2][0].length;
                    assert(len > 0, "Inner dimension cannot be 0");
                    foreach(i; 0 .. args[j * 2].length)
                        assert(args[j * 2][i].length == len, "Inner dimension of indexing[0].index not equal");
                    indexing[j].codes = args[j * 2];
                    foreach(i; 0 .. args[j * 2].length)
                        indexing[j].index ~= [[]];
                }
            }

            static if(Args.length > j * 2 + 1)
                indexing[j].titles = args[j*2 + 1];
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
            
            assert(next.length == indexing[axis].codes.length, "Index depth mismatch");
            foreach(i; 0 .. indexing[axis].codes.length)
            {
                if(indexing[axis].index[i].length == 0)
                    indexing[axis].codes[i] ~= next[i];
                else
                {
                    import std.conv: to, ConvException;
                    import std.algorithm: countUntil;
                    string ele = to!string(next[i]);
                    int pos = cast(int)countUntil(indexing[axis].index[i], ele);

                    if(pos > -1)
                    {
                        indexing[axis].codes[i] ~= pos;
                    }
                    else
                    {
                        indexing[axis].index[i] ~= ele;
                        indexing[axis].codes[i] ~= cast(int)indexing[axis].index[i].length - 1;
                    }
                }
            }
        }
        else static if(is(T == string[]))
        {
            assert(next.length == indexing[axis].codes.length, "Index depth mismatch");
            foreach(i; 0 .. indexing[axis].codes.length)
            {
                if(indexing[axis].index[i].length > 0)
                {
                    import std.algorithm: countUntil;
                    int pos = cast(int)countUntil(indexing[axis].index[i], next[i]);

                    if(pos > -1)
                    {
                        indexing[axis].codes[i] ~= pos;
                    }
                    else
                    {
                        indexing[axis].index[i] ~= next[i];
                        indexing[axis].codes[i] ~= cast(int)indexing[axis].index[i].length - 1;
                    }
                }
                else
                {
                    import std.conv: to, ConvException;
                    try
                    {
                        int ele = to!int(next[i]);
                        indexing[axis].codes[i] ~= ele;
                    }
                    catch(ConvException e)
                    {
                        indexing[axis].index[i] = to!(string[])(indexing[axis].codes[i]);
                        indexing[axis].index[i] ~= next[i];
                        indexing[axis].codes[i] = [];
                        foreach(j; 0 .. indexing[axis].index[i].length)
                            indexing[axis].codes[i] ~= cast(int)j;
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
