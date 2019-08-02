module magpie.operation;

/*
 * This file will contain work on operations of aggregate and join
 * aggregate:   Operation to work on a row/ column of DataFrame to give results like mean/median/mode/elc
 * merge:       It joins two DataFrame in a way similar to that in Pandas or any RDBMS software
 */

import magpie.dataframe: DataFrame;
import magpie.group: Group;
import magpie.helper: isDataFrame, isGroup;
import magpie.ops;

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

enum AggregateOP
{
    count = 1,
    max = 2,
    min = 3,
    mean = 4,
    median = 5
}

/++
Mathematical operation on an entire row/column
Params:
    axis: 0 to calculate alon row, 1 to calculate along column
    df: DataFrame to apply aggregate on
    ops: Operation to operate on the given DataFrame
Returns:
    A DataFFrame with the calculated aggregates
+/
auto aggregate(int axis, T, Ops...)(T df, Ops ops)
    if(isDataFrame!T)
{
    static if(axis)
    {
        DataFrame!(float, df.RowType.length) ret;
        ret.indx.column = df.indx.column;
        ret.indx.row.titles = ["Operation"];
        ret.indx.row.index.length = 1;
        ret.indx.row.index[0].length = Ops.length;
        ret.indx.row.codes.length = 1;

        foreach(i; 0 .. df.RowType.length)
            ret.data[i].length = Ops.length;

        double opres;

        static foreach(i; 0 .. Ops.length)
        {
            static foreach(j; 0 .. df.RowType.length)
            {
                if(ops[i] == AggregateOP.count)
                {
                    opres = count(df.data[j]);
                    ret.indx.row.index[0][i] = "Count";
                }
                else if(ops[i] == AggregateOP.max)
                {
                    opres = max(df.data[j]);
                    ret.indx.row.index[0][i] = "Max";
                }
                else if(ops[i] == AggregateOP.min)
                {
                    opres = min(df.data[j]);
                    ret.indx.row.index[0][i] = "Min";
                }
                else if(ops[i] == AggregateOP.mean)
                {
                    opres = max(df.data[j]);
                    ret.indx.row.index[0][i] = "Mean";
                }
                else if(ops[i] == AggregateOP.median)
                {
                    opres = max(df.data[j]);
                    ret.indx.row.index[0][i] = "Median";
                }
                else assert(0, "Operation specified not found");

                ret.data[j][i] = opres;
            }
        }

        ret.rows = Ops.length;
        ret.indx.optimize();

        return ret;
    }
    else
    {
        // static assert(df.isHomogeneousType, "Row Aggregate is only supported for Homogeneous DataFrames");
        DataFrame!(double, Ops.length) ret;
        ret.indx.row = df.indx.row;
        ret.indx.column.index.length = 1;
        ret.indx.column.codes.length = 1;
        ret.indx.column.index[0].length = Ops.length;

        foreach(i; 0 .. Ops.length)
            ret.data[i].length = df.rows;

        foreach(i; 0 .. df.rows)
        {
            double[df.RowType.length] opArr;
            int k;
            static foreach(j; 0 .. df.RowType.length)
                static if(__traits(isArithmetic, df.RowType[j]))
                {
                    opArr[k] = cast(double)df.data[j][i];
                    ++k;
                }

            double opres;
            if(opArr.length)
            {
                static foreach(j; 0 .. Ops.length)
                {
                    if(ops[j] == AggregateOP.count)
                    {
                        opres = count(opArr[0 .. k]);
                        ret.indx.column.index[0][j] = "Count";
                    }
                    else if(ops[j] == AggregateOP.max)
                    {
                        opres = max(opArr[0 .. k]);
                        ret.indx.column.index[0][j] = "Max";
                    }
                    else if(ops[j] == AggregateOP.min)
                    {
                        opres = min(opArr[0 .. k]);
                        ret.indx.column.index[0][j] = "Min";
                    }
                    else if(ops[j] == AggregateOP.mean)
                    {
                        opres = max(opArr[0 .. k]);
                        ret.indx.column.index[0][j] = "Mean";
                    }
                    else if(ops[j] == AggregateOP.median)
                    {
                        opres = max(opArr[0 .. k]);
                        ret.indx.column.index[0][j] = "Median";
                    }
                    else assert(0, "Operation specified not found");

                    ret.data[j][i] = opres;
                }
            }
        }

        ret.rows = df.rows;
        ret.indx.optimize();

        return ret;
    }
}

auto aggregate(int axis, T , Ops...)(T grp, Ops ops)
    if(isGroup!T)
{
    import std.meta: Repeat;
    static if(axis)
    {
        Group!(Repeat!(grp.GrpType.length, float)) ret;
        ret.groups = grp.groups;
        ret.elementCountTill.length = grp.groups.length + 1;
        ret.grpIndex.column = grp.grpIndex.column;

        foreach(i; 0 .. ret.elementCountTill.length)
            ret.elementCountTill[i] = cast(int)(i * Ops.length);

        foreach(i; 0 .. grp.GrpType.length)
            ret.data[i].length = ret.groups.length * Ops.length;   
        
        ret.grpIndex.row.titles = ["Operation"];
        ret.grpIndex.row.index.length = 1;
        ret.grpIndex.row.index[0].length = ret.groups.length * Ops.length;
        ret.grpIndex.row.codes.length = 1;

        foreach(i; 0 .. ret.groups.length)
        {
            double opres;
            static foreach(j; 0 .. Ops.length)
            {
                static foreach(k; 0 .. grp.GrpType.length)
                {
                    if(ops[j] == AggregateOP.count)
                    {
                        opres = count(grp.data[k][grp.elementCountTill[i] .. grp.elementCountTill[i + 1]]);
                        ret.grpIndex.row.index[0][i * Ops.length + j] = "Count";
                    }
                    else if(ops[j] == AggregateOP.max)
                    {
                        opres = max(grp.data[k][grp.elementCountTill[i] .. grp.elementCountTill[i + 1]]);
                        ret.grpIndex.row.index[0][i * Ops.length + j] = "Max";
                    }
                    else if(ops[j] == AggregateOP.min)
                    {
                        opres = min(grp.data[k][grp.elementCountTill[i] .. grp.elementCountTill[i + 1]]);
                        ret.grpIndex.row.index[0][i * Ops.length + j] = "Min";
                    }
                    else if(ops[j] == AggregateOP.mean)
                    {
                        opres = max(grp.data[k][grp.elementCountTill[i] .. grp.elementCountTill[i + 1]]);
                        ret.grpIndex.row.index[0][i * Ops.length + j] = "Mean";
                    }
                    else if(ops[j] == AggregateOP.median)
                    {
                        opres = max(grp.data[k][grp.elementCountTill[i] .. grp.elementCountTill[i + 1]]);
                        ret.grpIndex.row.index[0][i * Ops.length + j] = "Median";
                    }
                    else assert(0, "Operation specified not found");

                    ret.data[k][i * Ops.length + j] = opres;
                }
            }
        }

        ret.grpIndex.generateCodes();
        return ret;
    }
    else
    {
        Group!(Repeat!(Ops.length, float)) ret;
        ret.groups = grp.groups;
        ret.grpIndex.row = grp.grpIndex.row;
        ret.elementCountTill = grp.elementCountTill;

        foreach(i; 0 .. Ops.length)
            ret.data[i].length = ret.elementCountTill[$ -1];

        ret.grpIndex.column.index.length = 1;
        ret.grpIndex.column.codes.length = 1;
        ret.grpIndex.column.index[0].length = Ops.length;

        foreach(i; 0 .. ret.elementCountTill[$ - 1])
        {
            double[grp.GrpType.length] opArr;
            size_t k;
            static foreach(j; 0 .. grp.GrpType.length)
                static if(__traits(isArithmetic, typeof(grp.data[j][i])))
                {
                    opArr[k] = cast(double)grp.data[j][i];
                    ++k;
                }    

            double opres;
            if(opArr.length)
            {
                static foreach(j; 0 .. Ops.length)
                {
                    if(ops[j] == AggregateOP.count)
                    {
                        opres = count(opArr[0 .. k]);
                        ret.grpIndex.column.index[0][j] = "Count";
                    }
                    else if(ops[j] == AggregateOP.max)
                    {
                        opres = max(opArr[0 .. k]);
                        ret.grpIndex.column.index[0][j] = "Max";
                    }
                    else if(ops[j] == AggregateOP.min)
                    {
                        opres = min(opArr[0 .. k]);
                        ret.grpIndex.column.index[0][j] = "Min";
                    }
                    else if(ops[j] == AggregateOP.mean)
                    {
                        opres = max(opArr[0 .. k]);
                        ret.grpIndex.column.index[0][j] = "Mean";
                    }
                    else if(ops[j] == AggregateOP.median)
                    {
                        opres = max(opArr[0 .. k]);
                        ret.grpIndex.column.index[0][j] = "Median";
                    }
                    else assert(0, "Operation specified not found");

                    ret.data[j][i] = opres;
                }
            }
        }

        ret.grpIndex.generateCodes();
        return ret;
    }
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

// Aggregate with single op
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 3, double, 2) df;
    Index inx;
    inx[0] = ["Row1", "Row2"];
    inx[1] = ["Col1", "Col2", "Col3", "Col4", "Col5"];

    df.setFrameIndex(inx);
    df = [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]];
    // df.display();

    assert(aggregate!(1)(df, AggregateOP.count).display(true, 200) == "Operation  Col1  Col2  Col3  Col4  Col5\n"
        ~ "Count      2     4     6     8     10  \n"
    );

    assert(aggregate!(0)(df, AggregateOP.count).display(true, 200) == "      Count\n"
        ~ "Row1  15   \n"
        ~ "Row2  15   \n"
    );
}

// Aggregate with multiple op
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 3, double, 2) df;
    Index inx;
    inx[0] = ["Row1", "Row2"];
    inx[1] = ["Col1", "Col2", "Col3", "Col4", "Col5"];

    df.setFrameIndex(inx);
    df = [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]];
    // df.display();

    assert(aggregate!(1)(df, AggregateOP.count, AggregateOP.max).display(true, 200) == "Operation  Col1  Col2  Col3  Col4  Col5\n"
        ~ "Count      2     4     6     8     10  \n"
        ~ "Max        1     2     3     4     5   \n"
    );

    assert(aggregate!(0)(df, AggregateOP.count, AggregateOP.max).display(true, 200) == "      Count  Max\n"
        ~ "Row1  15     5  \n"
        ~ "Row2  15     5  \n"
    );
}

// Aggregate with Group
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hello", "Hey"], ["Hi", "Hi", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,1,3]);
    df.assign!1(4, [1,2,3]);

    auto gp = df.groupBy!([2])([0, 1]);
    // gp.display();
    assert(aggregate!(1)(gp, AggregateOP.count).display(true, 200) == "Operation  0  1  3  4\n"
        ~ "Count      0  0  0  3\n\n"
        ~ "Operation  0  1  3  4\n"
        ~ "Count      0  0  0  3\n\n"
    );

    assert(aggregate!(0)(gp, AggregateOP.count).display(true, 200) == "3      Count\n"
        ~ "Hey    1    \n"
        ~ "Hello  2    \n\n"
        ~ "3   Count\n"
        ~ "Hi  3    \n\n"
    );
}

// Aggregate with Group
unittest
{
    import magpie.index: Index;

    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hello", "Hey"], ["Hi", "Hi", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,1,3]);
    df.assign!1(4, [1,2,3]);

    auto gp = df.groupBy!([2])([0, 1]);
    // gp.display();
    assert(aggregate!(1)(gp, AggregateOP.count, AggregateOP.max).display(true, 200) == "Operation  0  1  3  4\n"
        ~ "Count      0  0  0  3\n"
        ~ "Max        0  0  0  2\n\n"
        ~ "Operation  0  1  3  4\n"
        ~ "Count      0  0  0  3\n"
        ~ "Max        0  0  0  3\n\n"
    );

    assert(aggregate!(0)(gp, AggregateOP.count, AggregateOP.max).display(true, 200) == "3      Count  Max\n"
        ~ "Hey    1      1  \n"
        ~ "Hello  2      2  \n\n"
        ~ "3   Count  Max\n"
        ~ "Hi  3      3  \n\n"
    );
}
