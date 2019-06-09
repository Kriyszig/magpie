module magpie.axis;

/++
Structure to return an entire row or column of DataFrame.
The operations on this structure enables column/row binary operations.
+/
struct Axis(T...)
{
    static if(T.length == 1)
        alias AxisType = T[0];
    else
        alias AxisType = T;

    /// Data being returned by the DataFrame
    AxisType data;

    auto convertTo(U...)() @property
        if(U.length == T.length)
    {
        import std.conv: to;
        static if(U.length == 1)
        {
            Axis!U ret;
            ret.data = to!(U[0])(data);
            return ret;
        }
        else
        {
            Axis!U ret;
            static foreach(i; 0 .. U.length)
                ret.data[i] = to!(U[i])(data[i]);
            return ret;
        }
    }
}

// Covert a probable column
unittest
{
    Axis!(int[]) column;
    column.data = [1,2,3,4];
    auto res = column.convertTo!(double[]);
    assert(res.data == [1.0, 2.0, 3.0, 4.0]);
    assert(is(typeof(res.data) == double[]));
}

// Convert a probable row
unittest
{
    Axis!(int, double) row;
    row.data[0] = 12;
    row.data[1] = 14.7;
    auto res = row.convertTo!(double, int);
    assert(res.data[0] == 12.0);
    assert(res.data[1] == 14);
    assert(is(typeof(res.data[0]) == double));
    assert(is(typeof(res.data[1]) == int));
}