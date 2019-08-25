module magpie.operation;

/*
 * This file will contain work on operations of aggregate and join
 * aggregate:   Operation to work on a row/ column of DataFrame to give results like mean/median/mode/elc
 * merge:       It joins two DataFrame in a way similar to that in Pandas or any RDBMS software
 */

import magpie.dataframe: DataFrame;
import magpie.group: Group;
import magpie.helper: isDataFrame, isGroup;

/// Enums for joins
enum JoinTypes
{
    left = 0,
    right = 1,
    outer = 2,
    inner = 3,
}

alias Left = JoinTypes.left;
alias Right = JoinTypes.right;
alias Inner = JoinTypes.inner;
alias Outer = JoinTypes.outer;

/++
Description: Merges two DataFrames based on indexes
@params: type - Type of join: Inner, Outer, Left, Right
@params: df1 - The left DataFrame
@params: df2 - The right DataFrame
@params: lsuffix - Suffix to add in case there is an index collision between left and right DataFrame
@parmas: rsuffix - Suffix to add in case there is an index collision between left and right DataFrame
+/
auto merge(JoinTypes type = Inner, T, U)(T df1, U df2, string lsuffix = "x_", string rsuffix = "y_")
    if(isDataFrame!T && isDataFrame!U)
{

    assert(df1.indx.row.codes.length == df2.indx.row.codes.length,
        "Index level mismatch. Cannot merge dataframes with different level of indexing");

    if(df1.indx.column.codes.length > df2.indx.column.codes.length)
    {
        int[] codes;
        codes.length = df2.indx.column.codes[0].length;

        foreach(i; 0 .. df1.indx.column.codes.length - df2.indx.column.codes.length)
        {
            df2.indx.column.index ~= [[""]];
            df2.indx.column.codes ~= [codes];
        }
    }
    else if(df1.indx.column.codes.length < df2.indx.column.codes.length)
    {
        int[] codes;
        codes.length = df1.indx.column.codes[0].length;
        
        foreach(i; 0 .. df2.indx.column.codes.length - df1.indx.column.codes.length)
        {
            df1.indx.column.index ~= [[""]];
            df1.indx.column.codes ~= [codes];
        }
    }

    DataFrame!(true, df1.RowType, df2.RowType) combinator;

    import magpie.helper: ensureUnique, transposed;
    combinator.indx.column = ensureUnique!1(df1.indx, df2.indx, lsuffix, rsuffix).column;

    string[] unpack(size_t i)
    {
        string[] indx;
        indx.length = combinator.indx.row.codes.length;

        import std.range: lockstep;
        foreach(j, a, b; lockstep(combinator.indx.row.index, combinator.indx.row.codes))
        {
            if(a.length == 0)
            {
                import std.conv: to;
                indx[j] = to!string(b[i]);
            }
            else
            {
                indx[j] = a[b[i]];
            }
        }

        return indx;
    }

    static if(type == JoinTypes.left)
    {
        combinator.indx.row = df1.indx.row;
        static foreach(i; 0 .. df1.RowType.length)
            combinator.data[i] = df1.data[i];
        static foreach(i; 0 .. df2.RowType.length)
            combinator.data[df1.RowType.length + i].length = df1.rows;

        foreach(i; 0 .. combinator.indx.row.codes[0].length)
        {
            string[] indx = unpack(i);
            ptrdiff_t pos = df2.indx.getPosition!0(indx);

            if(pos > -1)
            {
                static foreach(j; 0 .. df2.RowType.length)
                {
                    combinator.data[df1.RowType.length + j][i] = df2.data[j][pos];
                }
            }
        }

        combinator.rows = df1.rows;
        return combinator;
    }
    else static if(type == JoinTypes.right)
    {
        combinator.indx.row = df2.indx.row;
        static foreach(i; 0 .. df2.RowType.length)
            combinator.data[df1.RowType.length + i] = df2.data[i];
        static foreach(i; 0 .. df1.RowType.length)
            combinator.data[i].length = df2.rows;

        foreach(i; 0 .. combinator.indx.row.codes[0].length)
        {
            string[] indx = unpack(i);
            ptrdiff_t pos = df1.indx.getPosition!0(indx);

            if(pos > -1)
            {
                static foreach(j; 0 .. df1.RowType.length)
                    combinator.data[j][i] = df1.data[j][pos];
            }
        }

        combinator.rows = df2.rows;
        return combinator;
    }
    else static if(type == JoinTypes.outer)
    {
        import magpie.helper: indexUnion;
        combinator.indx.row = indexUnion!0(df1.indx, df2.indx).row;
        combinator.indx.row.titles = df1.indx.row.titles;

        combinator.rows = combinator.indx.row.codes[0].length;

        static foreach(i; 0 .. combinator.RowType.length)
            combinator.data[i].length = combinator.rows;

        foreach(i; 0 .. combinator.rows)
        {
            string[] indx = unpack(i);

            ptrdiff_t pos = df1.indx.getPosition!0(indx);
            if(pos > -1)
            {
                static foreach(j; 0 .. df1.RowType.length)
                    combinator.data[j][i] = df1.data[j][pos];
            }

            pos = df2.indx.getPosition!0(indx);
            if(pos > -1)
            {
                static foreach(j; 0 .. df2.RowType.length)
                    combinator.data[df1.RowType.length + j][i] = df2.data[j][pos];
            }
        }

        return combinator;
    }
    else static if(type == JoinTypes.inner)
    {
        import magpie.helper: indexIntersection;
        combinator.indx.row = indexIntersection!0(df1.indx, df2.indx).row;
        combinator.indx.row.titles = df1.indx.row.titles;

        combinator.rows = combinator.indx.row.codes[0].length;

        static foreach(i; 0 .. combinator.RowType.length)
            combinator.data[i].length = combinator.rows;

        foreach(i; 0 .. combinator.rows)
        {
            string[] indx = unpack(i);

            ptrdiff_t pos = df1.indx.getPosition!0(indx);
            if(pos > -1)
            {
                static foreach(j; 0 .. df1.RowType.length)
                    combinator.data[j][i] = df1.data[j][pos];
            }

            pos = df2.indx.getPosition!0(indx);
            if(pos > -1)
            {
                static foreach(j; 0 .. df2.RowType.length)
                    combinator.data[df1.RowType.length + j][i] = df2.data[j][pos];
            }
        }

        return combinator;
    }
    else static assert(0, "Invalid join type. Available join type: left, right, outer, inner(default)");
}

// Left Join
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 2) df1;
    DataFrame!(double, 2) df2;

    Index i1, i2;
    i1.setIndex([["Hello", "Hi"]], ["I1"], [["Hello", "Hi"]]);
    i2.setIndex([["Hello", "Hi"]], ["I1"], [["Hello", "Hi"], ["Hey", "Hey"]]);

    df1.setFrameIndex(i1);
    df1.assign!1(0, [1, 2]);
    df2.setFrameIndex(i2);
    df2.assign!1(0, [1.0, 2.0]);

    assert(merge!(JoinTypes.right)(df1, df2).display(true, 200) == "       Hello  Hi  Hello  Hi \n"
        ~ "I1                Hey    Hey\n"
        ~ "Hello  1      0   1      nan\n"
        ~ "Hi     2      0   2      nan\n"
    );
}

// Left Join
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 2) df1;
    DataFrame!(double, 2) df2;

    Index i1, i2;
    i1.setIndex([["Hello", "Hey"]], ["I1"], [["Hello", "Hi"]]);
    i2.setIndex([["Hello", "Hi"]], ["I1"], [["Hello", "Hi"], ["Hey", "Hey"]]);

    df1.setFrameIndex(i1);
    df1.assign!1(0, [1, 2]);
    df2.setFrameIndex(i2);
    df2.assign!1(0, [1.0, 2.0]);

    assert(merge!(JoinTypes.left)(df1, df2).display(true, 200) == "       Hello  Hi  Hello  Hi \n"
        ~ "I1                Hey    Hey\n"
        ~ "Hello  1      0   1      nan\n"
        ~ "Hey    2      0   nan    nan\n"
    );
}

// Right Join
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 2) df1;
    DataFrame!(double, 2) df2;

    Index i1, i2;
    i1.setIndex([["Hello", "Hi"]], ["I1"], [["Hello", "Hi"]]);
    i2.setIndex([["Hello", "Hey"]], ["I1"], [["Hello", "Hi"], ["Hey", "Hey"]]);

    df1.setFrameIndex(i1);
    df1.assign!1(0, [1, 2]);
    df2.setFrameIndex(i2);
    df2.assign!1(0, [1.0, 2.0]);

    assert(merge!(JoinTypes.right)(df1, df2).display(true, 200) == "       Hello  Hi  Hello  Hi \n"
        ~ "I1                Hey    Hey\n"
        ~ "Hello  1      0   1      nan\n"
        ~ "Hey    0      0   2      nan\n"
    );
}

// Outer join
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 2) df1;
    DataFrame!(double, 2) df2;

    Index i1, i2;
    i1.setIndex([["Hello", "Hi"]], ["I1"], [["Hello", "Hi"]]);
    i2.setIndex([["Hello", "Hey"]], ["I1"], [["Hello", "Hi"], ["Hey", "Hey"]]);

    df1.setFrameIndex(i1);
    df1.assign!1(0, [1, 2]);
    df2.setFrameIndex(i2);
    df2.assign!1(0, [1.0, 2.0]);

    assert(merge!(JoinTypes.outer)(df1, df2).display(true, 200) == "       Hello  Hi  Hello  Hi \n"
        ~ "I1                Hey    Hey\n"
        ~ "Hello  1      0   1      nan\n"
        ~ "Hi     2      0   nan    nan\n"
        ~ "Hey    0      0   2      nan\n"
    );
}

// Outer join
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 2) df1;
    DataFrame!(double, 2) df2;

    Index i1, i2;
    i1.setIndex([["Hello", "Hi"]], ["I1"], [["Hello", "Hi"]]);
    i2.setIndex([["Hello", "Hi"]], ["I1"], [["Hello", "Hi"], ["Hey", "Hey"]]);

    df1.setFrameIndex(i1);
    df1.assign!1(0, [1, 2]);
    df2.setFrameIndex(i2);
    df2.assign!1(0, [1.0, 2.0]);

    assert(merge!(JoinTypes.outer)(df1, df2).display(true, 200) == "       Hello  Hi  Hello  Hi \n"
        ~ "I1                Hey    Hey\n"
        ~ "Hello  1      0   1      nan\n"
        ~ "Hi     2      0   2      nan\n"
    );
}

// Inner Join
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 2) df1;
    DataFrame!(double, 2) df2;

    Index i1, i2;
    i1.setIndex([["Hello", "Hi"]], ["I1"], [["Hello", "Hi"]]);
    i2.setIndex([["Hello", "Hey"]], ["I1"], [["Hello", "Hi"], ["Hey", "Hey"]]);

    df1.setFrameIndex(i1);
    df1.assign!1(0, [1, 2]);
    df2.setFrameIndex(i2);
    df2.assign!1(0, [1.0, 2.0]);

    assert(merge!(JoinTypes.inner)(df1, df2).display(true, 200) == "       Hello  Hi  Hello  Hi \n"
        ~ "I1                Hey    Hey\n"
        ~ "Hello  1      0   1      nan\n"
    );
}
