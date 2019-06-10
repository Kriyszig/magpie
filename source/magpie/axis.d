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

    /++
    auto convertTo(U...)()
    Description: Change the type of Axis.data
    @params: u - The type to which data needs to be converted to.
    +/
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

    /++
    Binary Operations on DataFrame row/column
    +/
    Axis!T opBinary(string op, U...)(Axis!U rhs)
        if(U.length == T.length)
    {
        Axis!T ret;
        static if(op == "+")
        {
            static if(U.length == 1)
            {
                assert(data.length == rhs.data.length, "Size mismatch");

                foreach(i; 0 .. data.length)
                    ret.data ~= data[i] + rhs.data[i];

                return ret;                
            }
            else
            {
                static foreach(i; 0 .. U.length)
                    ret.data[i] = data[i] + rhs.data[i];

                return ret;
            }
        }
        else static if(op == "-")
        {
            static if(U.length == 1)
            {
                assert(data.length == rhs.data.length, "Size mismatch");

                foreach(i; 0 .. data.length)
                    ret.data ~= data[i] - rhs.data[i];

                return ret;                
            }
            else
            {
                static foreach(i; 0 .. U.length)
                    ret.data[i] = data[i] - rhs.data[i];

                return ret;
            }
        }
        else static if(op == "*")
        {
            static if(U.length == 1)
            {
                assert(data.length == rhs.data.length, "Size mismatch");

                foreach(i; 0 .. data.length)
                    ret.data ~= data[i] * rhs.data[i];

                return ret;                
            }
            else
            {
                static foreach(i; 0 .. U.length)
                    ret.data[i] = data[i] * rhs.data[i];

                return ret;
            }
        }
        else static if(op == "/")
        {
            static if(U.length == 1)
            {
                assert(data.length == rhs.data.length, "Size mismatch");

                foreach(i; 0 .. data.length)
                    ret.data ~= data[i] / rhs.data[i];

                return ret;                
            }
            else
            {
                static foreach(i; 0 .. U.length)
                    ret.data[i] = data[i] / rhs.data[i];

                return ret;
            }
        }

        assert(0);
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

// Addition operation on multiple column
unittest
{
    Axis!(double[]) a1;
    a1.data = [1,2,3.7,4];
    Axis!(int[]) a2;
    a2.data = [1,2,3,4];
    Axis!(double[]) a3;
    a3.data = [1,2,3.7,4];
    Axis!(double[]) a4;
    a4.data = [1,2,3,4];

    assert((a1 + a2).data == [2, 4, 6.7, 8]);
    assert(a1.data == [1,2,3.7,4]);
    assert(a2.data == [1,2,3,4]);
    assert((a1 + a2 + a3 + a4).data == [4, 8, 13.4, 16]);
}

// Addition operation on multiple rows
unittest
{
    Axis!(int, double) a1;
    a1.data[0] = 1;
    a1.data[1] = 1.786;

    Axis!(int, int) a2;
    a2.data[0] = 1;
    a2.data[1] = 1;

    Axis!(int, double) a3;
    a3.data[0] = 1;
    a3.data[1] = 1.786;

    Axis!(int, double) a4;
    a4.data[0] = 1;
    a4.data[1] = 1;

    import std.math: approxEqual;
    auto res = a1 + a2;
    assert(res.data[0] == 2);
    assert(approxEqual(res.data[1], 2.786, 1e-3));

    auto res2 = a1 + a2 + a3 + a4;
    assert(res2.data[0] == 4);
    assert(approxEqual(res2.data[1], 5.572, 1e-3));
}

// Subtraction operation on multiple column
unittest
{
    Axis!(double[]) a1;
    a1.data = [1,2,3.765,4];
    Axis!(int[]) a2;
    a2.data = [1,2,3,4];
    Axis!(double[]) a3;
    a3.data = [1,2,3.765,4];
    Axis!(double[]) a4;
    a4.data = [1,2,3,4];

    import std.math: approxEqual;
    static foreach(i; 0 .. 4)
    {
        assert(approxEqual((a1 - a2).data[i], a1.data[i] - a2.data[i], 1e-3));
        assert(approxEqual((a1 - a2 - a3 - a4).data[i], a1.data[i] - a2.data[i] - a3.data[i] - a4.data[i], 1e-3));
    }

}

// Subtraction operation on multiple rows
unittest
{
    Axis!(int, double) a1;
    a1.data[0] = 1;
    a1.data[1] = 1.786;

    Axis!(int, int) a2;
    a2.data[0] = 1;
    a2.data[1] = 1;

    Axis!(int, double) a3;
    a3.data[0] = 1;
    a3.data[1] = 1.786;

    Axis!(int, double) a4;
    a4.data[0] = 1;
    a4.data[1] = 1;

    auto res = a1 - a2;
    auto res2 = a1 - a2 - a3 - a4;
    
    import std.math: approxEqual;
    static foreach(i; 0 .. 2)
    {
        assert(approxEqual(res.data[i], a1.data[i] - a2.data[i], 1e-3));
        assert(approxEqual(res2.data[i], a1.data[i] - a2.data[i] - a3.data[i] - a4.data[i], 1e-3));
    }
}

// Multiplication operation on multiple column
unittest
{
    Axis!(double[]) a1;
    a1.data = [1,2,3.765,4];
    Axis!(int[]) a2;
    a2.data = [1,2,3,4];
    Axis!(double[]) a3;
    a3.data = [1,2,3.765,4];
    Axis!(double[]) a4;
    a4.data = [1,2,3,4];

    import std.math: approxEqual;
    static foreach(i; 0 .. 4)
    {
        assert(approxEqual((a1 * a2).data[i], a1.data[i] * a2.data[i], 1e-6));
        assert(approxEqual((a1 * a2 * a3 * a4).data[i], a1.data[i] * a2.data[i] * a3.data[i] * a4.data[i], 1e-6));
    }

}

// Multiplication operation on multiple rows
unittest
{
    Axis!(int, double) a1;
    a1.data[0] = 1;
    a1.data[1] = 1.786;

    Axis!(int, int) a2;
    a2.data[0] = 1;
    a2.data[1] = 1;

    Axis!(int, double) a3;
    a3.data[0] = 1;
    a3.data[1] = 1.786;

    Axis!(int, double) a4;
    a4.data[0] = 1;
    a4.data[1] = 1;

    auto res = a1 * a2;
    auto res2 = a1 * a2 * a3 * a4;
    
    import std.math: approxEqual;
    static foreach(i; 0 .. 2)
    {
        assert(approxEqual(res.data[i], a1.data[i] * a2.data[i], 1e-6));
        assert(approxEqual(res2.data[i], a1.data[i] * a2.data[i] * a3.data[i] * a4.data[i], 1e-6));
    }
}

// Division operation on multiple column
unittest
{
    Axis!(double[]) a1;
    a1.data = [1,2,3.765,4];
    Axis!(int[]) a2;
    a2.data = [1,2,3,4];
    Axis!(double[]) a3;
    a3.data = [1,2,3.765,4];
    Axis!(double[]) a4;
    a4.data = [1,2,3,4];

    import std.math: approxEqual;
    static foreach(i; 0 .. 4)
    {
        assert(approxEqual((a1 / a2).data[i], a1.data[i] / a2.data[i], 1e-3));
        assert(approxEqual((a1 / a2 / a3 / a4).data[i], a1.data[i] / a2.data[i] / a3.data[i] / a4.data[i], 1e-3));
    }

}

// Division operation on multiple rows
unittest
{
    Axis!(int, double) a1;
    a1.data[0] = 1;
    a1.data[1] = 1.786;

    Axis!(int, int) a2;
    a2.data[0] = 1;
    a2.data[1] = 1;

    Axis!(int, double) a3;
    a3.data[0] = 1;
    a3.data[1] = 1.786;

    Axis!(int, double) a4;
    a4.data[0] = 1;
    a4.data[1] = 1;

    auto res = a1 / a2;
    auto res2 = a1 / a2 / a3 / a4;
    
    import std.math: approxEqual;
    static foreach(i; 0 .. 2)
    {
        assert(approxEqual(res.data[i], a1.data[i] / a2.data[i], 1e-3));
        assert(approxEqual(res2.data[i], a1.data[i] / a2.data[i] / a3.data[i] / a4.data[i], 1e-3));
    }
}
