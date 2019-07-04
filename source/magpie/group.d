module magpie.group;

import magpie.axis: Axis, DataType;
import magpie.dataframe: DataFrame;
import magpie.index: Index;
import magpie.helper: dropper, transposed, toArr, vectorize;

import std.meta: staticMap;

/// Struct for groupBy Operation
struct Group(GrpRowType...)
{
    /// Group names
    string[][] groups;

    /// number of elements before the particular group
    int[] elementCountTill;

    alias GrpType = staticMap!(toArr, GrpRowType);
    /// Data of Group
    GrpType data;

    /// Index for group
    Index grpIndex;

private:
    int positionInGroup(int axis)(string[] index, int grpPos)
    {
        import std.array: appender;
        import std.algorithm: countUntil;
        import std.conv: to;
        auto codes = appender!(int[]);

        foreach(i; 0 .. grpIndex.indexing[axis].codes.length)
        {
            if(grpIndex.indexing[axis].index[i].length == 0)
                codes.put(to!int(index[i]));
            else
            {
                int indxpos = cast(int)countUntil(grpIndex.indexing[axis].index[i], index[i]);
                if(indxpos < 0)
                    return -1;
                codes.put(indxpos);
            }
        }

        foreach(i; 0 .. (axis == 0)? elementCountTill[grpPos +1] - elementCountTill[grpPos]: GrpType.length)
        {
            bool flag = true;
            foreach(j; 0 .. grpIndex.indexing[axis].codes.length)
            {
                if(grpIndex.indexing[axis].codes[j][i + ((axis == 0)? elementCountTill[grpPos]: 0)] != codes.data[j])
                    flag = false;
            }

            if(flag)
                return cast(int)i;
        }

        return -1;
    }

public:
    /++
    int getGroupPosition(string[] grpTitles)
    Description: Get position of a particular group in the sruct
    @params: grpTitles - the group you wnant to search for
    +/
    int getGroupPosition(string[] grpTitles)
    {
        foreach(i, ele; groups)
            if(grpTitles == ele)
                return cast(int)i;

        return -1;
    }

    /++
    void createGroup(T)(DataFrame!T df, int[] levels)
    Description: Funtion to create a groupBy object from a DataFrame
    @parmas: df - DataFrame on which groupBy is operated
    @params: levels - levels to consider for grouping
    +/
    void createGroup(int[] dataLevels = [], T...)(DataFrame!T df, int[] indexLevels)
    {
        assert(dataLevels.length + indexLevels.length > 0, "Cannot group without specifying any levels");
        import std.algorithm: reduce, max, min, sort;

        if(indexLevels.length > 0)
        {
            assert(indexLevels.reduce!max < df.indx.row.index.length);
            assert(indexLevels.reduce!min > -1);
        }

        static if(dataLevels.length > 0)
        {
            assert(dataLevels.reduce!max < df.RowType.length);
            assert(dataLevels.reduce!min > -1);
        }

        string[][] levels;
        levels.length = df.rows;
        foreach(i; 0 .. df.rows)
        {
            levels[i].length = indexLevels.length + dataLevels.length;
            foreach(j, ele; indexLevels)
            {
                if(df.indx.row.index[ele].length == 0)
                {
                    import std.conv: to;
                    levels[i][j] = to!string(df.indx.row.codes[ele][i]);
                }
                else
                    levels[i][j] = df.indx.row.index[ele][df.indx.row.codes[ele][i]];
            }

            static foreach(j, ele; dataLevels)
            {
                static if(is(df.RowType[ele] == string))
                    levels[i][indexLevels.length + j] = df.data[ele][i];
                else
                {
                    import std.conv: to;
                    levels[i][indexLevels.length + j] = to!string(df.data[ele][i]);
                }
            }
        }

        // This is required as dropper!(dataLevels, df.data) won't work
        auto dropped = df.drop!(1, dataLevels);

        data = dropped.data;
        grpIndex.column = dropped.indx.column;
        grpIndex.row.titles = dropper(indexLevels, df.indx.row.titles);
        grpIndex.row.index = dropper(indexLevels, df.indx.row.index);

        import std.range: zip;
        int[] codes = vectorize(levels);
        int[][] rcodes = transposed(dropper(indexLevels, df.indx.row.codes));
        // Simultaneously arranging all the relavent fields using a zip as displayed in the docs
        auto arrange = zip(levels, codes[1 .. $], data, rcodes).sort!((a, b) => a[1] < b[1]);

        grpIndex.row.codes = transposed(rcodes);

        elementCountTill.length = codes[0] + 1;
        groups.length = codes[0];
        groups[0] = levels[0];
        elementCountTill[0] = 0;
        int uele = 0, count = 0;

        foreach(i; codes[1 .. $])
        {
            if(i != uele)
            {
                groups[uele + 1] = levels[count];
                elementCountTill[uele + 1] = count;
                ++uele;
            }
            count++;
        }

        elementCountTill[uele + 1] = count;
    }

    /++
    string display(T)(T[] groupIndex = [],bool getStr = false, int termianlw = 0)
    Description: Display groupBy on terminal
    @params: groupIndex - Group index in form of integer array or string array
    @params: getStr - Returns the generated display sring
    @params: termianlw - Set terminal width
    +/
    string display(T)(T[] groupIndex = [],bool getStr = false, int termianlw = 0)
    {
        import std.array: appender;
        auto retstr = appender!(string);

        int[] pos;
        static if(is(T == int))
        {
            import std.algorithm: reduce, max, min;
            assert(groupIndex.reduce!max < groups.length, "Index out of bound");
            assert(groupIndex.reduce!min > -1, "Index out of bound");
            pos = groupIndex;
        }
        else static if(is(T == string))
        {
            pos.length = 1;
            int indxpos = getGroupPosition(groupIndex);
            assert(indxpos > -1, "Group not found");
            pos[0] = indxpos;
        }
        else static if(is(T == string[]))
        {
            pos.length = groupIndex.length;
            foreach(i, ele; groupIndex)
            {
                int indxpos = getGroupPosition(ele);
                assert(indxpos > -1, "Group not found");
                pos[i] = indxpos;
            }
        }
        else
        {
            assert(0, "Group Indexes must be an array of integer, 1D array of string or 2D array of string");
        }

        DataFrame!(true, GrpRowType) displayHelper;
        displayHelper.indx.column = grpIndex.column;
        displayHelper.indx.row.titles = grpIndex.row.titles;
        displayHelper.indx.row.index = grpIndex.row.index;
        displayHelper.indx.row.codes.length = grpIndex.row.codes.length;

        foreach(i; 0 .. pos.length)
        {
            foreach(j; 0 .. displayHelper.indx.row.codes.length)
            {
                displayHelper.indx.row.codes[j] = grpIndex.row.codes[j][elementCountTill[pos[i]] .. elementCountTill[pos[i] + 1]];
            }

            static foreach(j; 0 .. GrpRowType.length)
                displayHelper.data[j] = data[j][elementCountTill[pos[i]] .. elementCountTill[pos[i] + 1]];

            displayHelper.rows = elementCountTill[pos[i] + 1] - elementCountTill[pos[i]];
            string display = displayHelper.display(true, termianlw);

            if(getStr)
            {
                retstr.put(display);
                retstr.put("\n");
            }
            else
            {
                import std.stdio: writeln;
                writeln("Group: ", groups[i]);
                writeln("Group Dimension: [ ", elementCountTill[pos[i] + 1] - elementCountTill[pos[i]]," X ", GrpType.length, " ]");
                writeln(display);
            }
        }

        return retstr.data;
    }

    /++
    string display(bool getStr = false, int termianlw = 0)
    Description: Display complete groupBy on terminal
    @params: getStr - Returns the generated display sring
    @params: termianlw - Set terminal width
    +/
    string display(bool getStr = false, int termianlw = 0)
    {
        int[] pos;
        pos.length = groups.length;
        foreach(i; 0 .. groups.length)
            pos[i] = cast(int)i;

        return display(pos, getStr, termianlw);
    }

    /++
    string[][] getGroups() @property
    Description: Get the generated string array containing all the groups
    +/
    string[][] getGroups() @property
    {
        return groups;
    }

    /++
    auto combine(T)(T[] groupIndex)
    Description: Combines selective groups into a DataFrame
    @params: groupIndex - Group index in form of integer array or string array
    +/
    auto combine(T)(T[] groupIndex)
    {
        int[] pos;
        static if(is(T == int))
        {
            import std.algorithm: reduce, max, min;
            assert(groupIndex.reduce!max < groups.length, "Index out of bound");
            assert(groupIndex.reduce!min > -1, "Index out of bound");
            pos = groupIndex;
        }
        else static if(is(T == string))
        {
            pos.length = 1;
            int indxpos = getGroupPosition(groupIndex);
            assert(indxpos > -1, "Group not found");
            pos[0] = indxpos;
        }
        else static if(is(T == string[]))
        {
            pos.length = groupIndex.length;
            foreach(i, ele; groupIndex)
            {
                int indxpos = getGroupPosition(ele);
                assert(indxpos > -1, "Group not found");
                pos[i] = indxpos;
            }
        }
        else
        {
            assert(0, "Group Indexes must be an array of integer, 1D array of string or 2D array of string");
        }

        DataFrame!(true, GrpRowType) combinator;
        combinator.indx.column = grpIndex.column;
        foreach(i; 0 .. groups[0].length)
        {
            import std.conv: to;
            ++combinator.indx.row.titles.length;
            combinator.indx.row.titles[i] = "GroupL" ~ to!(string)(i + 1);
        }

        combinator.indx.row.titles ~= grpIndex.row.titles;
        combinator.indx.row.index.length = combinator.indx.row.titles.length;
        combinator.indx.row.codes.length = combinator.indx.row.titles.length;

        foreach(i; 0 .. grpIndex.row.index.length)
            combinator.indx.row.index[groups[0].length + i] = grpIndex.row.index[i];

        foreach(i; 0 .. pos.length)
        {
            foreach(j, ele; groups[pos[i]])
            {
                combinator.indx.row.index[j].length += elementCountTill[pos[i] + 1] - elementCountTill[pos[i]];
                foreach(k; 0 .. elementCountTill[pos[i] + 1] - elementCountTill[pos[i]])
                    combinator.indx.row.index[j][$ - 1 - k] = ele;
            }

            foreach(j; 0 .. grpIndex.row.index.length)
                combinator.indx.row.codes[groups[0].length + j] ~= grpIndex.row.codes[j][elementCountTill[pos[i]] .. elementCountTill[pos[i] + 1]];

            static foreach(j; 0 .. GrpRowType.length)
                combinator.data[j] ~= data[j][elementCountTill[pos[i]] .. elementCountTill[pos[i] + 1]];

            combinator.rows += elementCountTill[pos[i] + 1] - elementCountTill[pos[i]];
        }

        combinator.indx.generateCodes();
        combinator.indx.optimize();

        return combinator;
    }

    /++
    auto combine(T)(T[] groupIndex)
    Description: Combines all te groups into a DataFrame
    +/
    auto combine()
    {
        int[] pos;
        pos.length = groups.length;
        foreach(i; 0 .. groups.length)
            pos[i] = cast(int)i;

        return combine(pos);
    }

    /++
    Index operation of the form gp[size_t, size_t, size_t]
    +/
    auto opIndex(size_t i1, size_t i2, size_t i3)
    {
        static foreach(i; 0 .. GrpRowType.length)
            if(i == i3)
                return data[i][elementCountTill[i1] + i2];

        assert(0);
    }

    /++
    Index operation of the form gp[["Grp Index"], ["Row-Index"], ["Column-Index"]]
    +/
    auto opIndex(T, U, V)(T i1, U i2, V i3)
        if((is(T == string) || is(T == string[]))
        && (is(U == string) || is(U == string[]))
        && (is(V == string) || is(V == string[])))
    {
        int[3] pos;
        static if(is(T == string))
            pos[0] = getGroupPosition([i1]);
        else
            pos[0] = getGroupPosition(i1);
        assert(pos[0] > -1, "Index out of bound");

        static if(is(U == string))
            pos[1] = positionInGroup!(0)([i2], pos[0]);
        else
            pos[1] = positionInGroup!(0)(i2, pos[0]);
        assert(pos[1] > -1, "Index out of bound");

        static if(is(V == string))
            pos[2] = positionInGroup!(1)([i3], pos[0]);
        else
            pos[2] = positionInGroup!(1)(i3, pos[0]);
        assert(pos[2] > -1, "Index out of bound");

        static foreach(i; 0 .. GrpRowType.length)
            if(i == pos[2])
                return data[i][elementCountTill[pos[0]] + pos[1]];

        assert(0);
    }

    /++
    opIndex that returns Axis for column/row binary operations
    +/
    auto opIndex(Args...)(Args args)
        if(Args.length > 0 && Args.length < 4)
    {
        static assert(is(Args[0] == string[]) || is(Args[0] == string), "Group Index must be an integer or array of string");
        static assert(is(Args[1] == string[]) || is(Args[1] == string), "Index must be an integer or array of string");

        int[2] pos;
        static if(is(Args[0] == string))
        {
            int grpPos = getGroupPosition([args[0]]);
            assert(grpPos > -1, "Index out of bound");
            pos[0] = grpPos;
        }
        else
        {
            int grpPos = getGroupPosition(args[0]);
            assert(grpPos > -1, "Index out of bound");
            pos[0] = grpPos;
        }

        static if(is(Args[1] == string))
        {
            int axisPos = positionInGroup!(3 - Args.length)([args[1]], pos[0]);
            assert(axisPos > -1, "Index out of bound");
            pos[1] = axisPos;
        }
        else
        {
            int axisPos = positionInGroup!(3 - Args.length)(args[1], pos[0]);
            assert(axisPos > -1, "Index out of bound");
            pos[1] = axisPos;
        }

        static if(3 - Args.length)
        {
            Axis!(void) ret;
            static foreach(i; 0 .. GrpRowType.length)
            {
                if(i == pos[1])
                {
                    foreach(j; data[i][elementCountTill[pos[0]] .. elementCountTill[pos[0] + 1]])
                        ret.data ~= DataType(j);
                }
            }

            return ret;
        }
        else
        {
            Axis!(GrpRowType) ret;
            static foreach(i; 0 .. GrpRowType.length)
            {
                ret.data[i] = data[i][elementCountTill[pos[0]] + pos[1]];
            }

            return ret;
        }
    }

    /// Binary Operation on DataFrame Columns
    void opIndexAssign(T...)(Axis!T elements, string[] groupTitle, string[] index, int axis = 1)
    {
        int[2] pos;
        int p = getGroupPosition(groupTitle);
        assert(p > -1, "Group index out of bound");
        pos[0] = p;

        if(axis)
        {
            p = positionInGroup!(1)(index, pos[0]);
            assert(p > -1, "Index out of bound");
            pos[1] = p;
        }
        else
        {
            p = positionInGroup!(0)(index, pos[0]);
            assert(p > -1, "Index out of bound");
            pos[1] = p;
        }

        static if(is(T[0] == void))
        {
            assert(elements.data.length == elementCountTill[pos[0] + 1] - elementCountTill[pos[0]],
                "Size of data doesn't match size of group column");
            static foreach(i; 0.. GrpRowType.length)
            {
                if(i == pos[1])
                {
                    foreach(j; elementCountTill[pos[0]] .. elementCountTill[pos[0] + 1])
                    {
                        import std.variant: VariantException;
                        try
                        {
                            data[i][j] = elements.data[j - elementCountTill[pos[0]]].get!(GrpRowType[i]);
                        }
                        catch(VariantException e)
                        {
                            data[i][j] = cast(GrpRowType[i])elements.data[j - elementCountTill[pos[0]]].get!(double);
                        }
                    }
                }
            }
        }
        else
        {
            assert(elements.data.length == GrpRowType.length, "Size of data doesn't match size of group column");
            static foreach(i; 0.. GrpRowType.length)
            {
                static foreach(i; 0 .. GrpRowType.length)
                    data[i][elementCountTill[pos[0]] + pos[1]] = elements.data[i];
            }
        }
    }

    /// Short Hand Binary operation
    void opIndexOpAssign(string op, T...)(Axis!T elements, string[] groupTitle, string[] index, int axis = 1)
    {
       /*
        * This code looks very much like the code above
        * Initially this could be written as ofIndexAssign(opIndex[...indexes] + element, ...indexes)
        * The problem here is that opIndex will create a new Axis struct. This additional allocation
        * can be avoided by rewriting the implementation above to account for everything.
        */

        int[2] pos;
        int p = getGroupPosition(groupTitle);
        assert(p > -1, "Group index out of bound");
        pos[0] = p;

        if(axis)
        {
            p = positionInGroup!(1)(index, pos[0]);
            assert(p > -1, "Index out of bound");
            pos[1] = p;
        }
        else
        {
            p = positionInGroup!(0)(index, pos[0]);
            assert(p > -1, "Index out of bound");
            pos[1] = p;
        }

        static if(is(T[0] == void))
        {
            assert(elements.data.length == elementCountTill[pos[0] + 1] - elementCountTill[pos[0]],
                "Size of data doesn't match size of group column");
            static foreach(i; 0.. GrpRowType.length)
            {
                if(i == pos[1])
                {
                    foreach(j; elementCountTill[pos[0]] .. elementCountTill[pos[0] + 1])
                    {
                        import std.variant: VariantException;
                        try
                        {
                            static if(op == "+")
                                data[i][j] += elements.data[j - elementCountTill[pos[0]]].get!(GrpRowType[i]);
                            else static if(op == "-")
                                data[i][j] -= elements.data[j - elementCountTill[pos[0]]].get!(GrpRowType[i]);
                            else static if(op == "*")
                                data[i][j] *= elements.data[j - elementCountTill[pos[0]]].get!(GrpRowType[i]);
                            else static if(op == "/")
                                data[i][j] /= elements.data[j - elementCountTill[pos[0]]].get!(GrpRowType[i]);
                        }
                        catch(VariantException e)
                        {
                            static if(op == "+")
                                data[i][j] += cast(GrpRowType[i])elements.data[j - elementCountTill[pos[0]]].get!(double);
                            else static if(op == "-")
                                data[i][j] -= cast(GrpRowType[i])elements.data[j - elementCountTill[pos[0]]].get!(double);
                            else static if(op == "*")
                                data[i][j] *= cast(GrpRowType[i])elements.data[j - elementCountTill[pos[0]]].get!(double);
                            else static if(op == "/")
                                data[i][j] /= cast(GrpRowType[i])elements.data[j - elementCountTill[pos[0]]].get!(double);
                        }
                    }
                }
            }
        }
        else
        {
            assert(elements.data.length == GrpRowType.length, "Size of data doesn't match size of group column");
            static foreach(i; 0.. GrpRowType.length)
            {
                static foreach(i; 0 .. GrpRowType.length)
                {
                    static if(op == "+")
                        data[i][elementCountTill[pos[0]] + pos[1]] += elements.data[i];
                    else static if(op == "-")
                        data[i][elementCountTill[pos[0]] + pos[1]] -= elements.data[i];
                    else static if(op == "*")
                        data[i][elementCountTill[pos[0]] + pos[1]] *= elements.data[i];
                    else static if(op == "/")
                        data[i][elementCountTill[pos[0]] + pos[1]] /= elements.data[i];
                }
            }
        }
    }
}

// Basic groubBy operation done manually
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);
    string str1 = df.display(true, 200);

    Group!(int, int, int, int) gp;
    gp.createGroup!([2])(df, [0, 1]);

    assert(gp.groups.length == 3);
    assert(gp.groups == [["Hello", "Hi", "1"], ["Hi", "Hello", "2"], ["Hey", "Hey", "3"]]);
    assert(gp.elementCountTill == [0, 1, 2, 3]);
    assert(gp.data[3] == [1, 2, 3]);

    string str2 = df.display(true, 200);
    assert(str1 == str2);
}

// getGroupPosition
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);

    Group!(int, int, int, int) gp;
    gp.createGroup!([2])(df, [0, 1]);
    assert(gp.getGroupPosition(["Hello", "Hi", "1"]) == 0);
    assert(gp.getGroupPosition(["Hi", "Hello", "2"]) == 1);
    assert(gp.getGroupPosition(["Hey", "Hey", "3"]) == 2);
}

// Display
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);
    string str1 = df.display(true, 200);

    Group!(int, int, int, int) gp;
    gp.createGroup!([2])(df, [0, 1]);

    // Full Group
    assert(gp.display(true, 200) == "3    0  1  3  4\n"
        ~ "Hey  0  0  0  1\n\n"
        ~ "3      0  1  3  4\n"
        ~ "Hello  0  0  0  2\n\n"
        ~ "3   0  1  3  4\n"
        ~ "Hi  0  0  0  3\n\n"
    );

    // Single Group
    assert(gp.display(["Hello", "Hi", "1"], true, 200) == "3    0  1  3  4\n"
        ~ "Hey  0  0  0  1\n\n"
    );

    // Array of groups
    assert(gp.display([["Hello", "Hi", "1"], ["Hi", "Hello", "2"]], true, 200) == "3    0  1  3  4\n"
        ~ "Hey  0  0  0  1\n\n"
        ~ "3      0  1  3  4\n"
        ~ "Hello  0  0  0  2\n\n"
    );

    // Array of groups
    assert(gp.display([["Hello", "Hi", "1"], ["Hi", "Hello", "2"], ["Hey", "Hey", "3"]], true, 200) == "3    0  1  3  4\n"
        ~ "Hey  0  0  0  1\n\n"
        ~ "3      0  1  3  4\n"
        ~ "Hello  0  0  0  2\n\n"
        ~ "3   0  1  3  4\n"
        ~ "Hi  0  0  0  3\n\n"
    );

    // Single Group - integer indexes
    assert(gp.display([0], true, 200) == "3    0  1  3  4\n"
        ~ "Hey  0  0  0  1\n\n"
    );

    // Array of groups - integer indexes
    assert(gp.display([0, 1], true, 200) == "3    0  1  3  4\n"
        ~ "Hey  0  0  0  1\n\n"
        ~ "3      0  1  3  4\n"
        ~ "Hello  0  0  0  2\n\n"
    );

    // Array of groups - integer indexes
    assert(gp.display([0, 1, 2], true, 200) == "3    0  1  3  4\n"
        ~ "Hey  0  0  0  1\n\n"
        ~ "3      0  1  3  4\n"
        ~ "Hello  0  0  0  2\n\n"
        ~ "3   0  1  3  4\n"
        ~ "Hi  0  0  0  3\n\n"
    );
}

// Pandas groupBy Example
unittest
{
    DataFrame!(double) df;
    Index inx;
    inx.constructFromLevels!(0)([["Falcon", "Parrot"], ["Captive", "Wild"]], ["Animal", "Type"]);
    inx.constructFromLevels!(1)([["Max-Speed"]]);
    df.setFrameIndex(inx);
    df.assign!1(0, [380.0, 370.0, 24.0, 26.0]);
    // df.display();

    Group!(double) gp;
    gp.createGroup(df, [0]);
    assert(gp.groups.length == 2);
    assert(gp.groups == [["Falcon"], ["Parrot"]]);
    assert(gp.elementCountTill == [0, 2, 4]);
    assert(gp.data[0] == [380.0, 370.0, 24.0, 26.0]);

    // Display full
    assert(gp.display(true, 200) == "Type     Max-Speed\n"
        ~ "Captive  380      \n"
        ~ "Wild     370      \n\n"
        ~ "Type     Max-Speed\n"
        ~ "Captive  24       \n"
        ~ "Wild     26       \n\n"
    );

    // Display Falcon
    assert(gp.display(["Falcon"], true, 200) == "Type     Max-Speed\n"
        ~ "Captive  380      \n"
        ~ "Wild     370      \n\n"
    );

    // Display Parrot
    assert(gp.display(["Parrot"], true, 200) == "Type     Max-Speed\n"
        ~ "Captive  24       \n"
        ~ "Wild     26       \n\n"
    );

    // Display Falcon - Integer Index
    assert(gp.display([0], true, 200) == "Type     Max-Speed\n"
        ~ "Captive  380      \n"
        ~ "Wild     370      \n\n"
    );

    // Display Parrot - Integer Index
    assert(gp.display([1], true, 200) == "Type     Max-Speed\n"
        ~ "Captive  24       \n"
        ~ "Wild     26       \n\n"
    );
}

// getGroups
unittest
{
    DataFrame!(double) df1;
    Index inx1;
    inx1.constructFromLevels!(0)([["Falcon", "Parrot"], ["Captive", "Wild"]], ["Animal", "Type"]);
    inx1.constructFromLevels!(1)([["Max-Speed"]]);
    df1.setFrameIndex(inx1);
    df1.assign!1(0, [380.0, 370.0, 24.0, 26.0]);
    // df.display();

    Group!(double) gp1;
    gp1.createGroup(df1, [0]);
    assert(gp1.getGroups == [["Falcon"], ["Parrot"]]);

    DataFrame!(int, 5) df2;
    Index inx2;
    inx2.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df2.setFrameIndex(inx2);
    df2.assign!1(2, [1,2,3]);
    df2.assign!1(4, [1,2,3]);

    Group!(int, int, int, int) gp2;
    gp2.createGroup!([2])(df2, [0, 1]);
    assert(gp2.getGroups == [["Hello", "Hi", "1"], ["Hi", "Hello", "2"], ["Hey", "Hey", "3"]]);
}

// Combine groups to get a DataFrame
unittest
{
    DataFrame!(double) df1;
    Index inx1;
    inx1.constructFromLevels!(0)([["Falcon", "Parrot"], ["Captive", "Wild"]], ["Animal", "Type"]);
    inx1.constructFromLevels!(1)([["Max-Speed"]]);
    df1.setFrameIndex(inx1);
    df1.assign!1(0, [380.0, 370.0, 24.0, 26.0]);
    // df.display();

    Group!(double) gp1;
    gp1.createGroup(df1, [0]);
    // gp1.display();

    // Combine one group
    assert(gp1.combine([0]).display(true, 200) == "GroupL1  Type     Max-Speed\n"
        ~ "Falcon   Captive  380      \n"
        ~ "Falcon   Wild     370      \n"
    );

    // Combine both the groups
    assert(gp1.combine([0, 1]).display(true, 200) == "GroupL1  Type     Max-Speed\n"
        ~ "Falcon   Captive  380      \n"
        ~ "Falcon   Wild     370      \n"
        ~ "Parrot   Captive  24       \n"
        ~ "Parrot   Wild     26       \n"
    );

    // Cobine all - overload
    assert(gp1.combine().display(true, 200) == "GroupL1  Type     Max-Speed\n"
        ~ "Falcon   Captive  380      \n"
        ~ "Falcon   Wild     370      \n"
        ~ "Parrot   Captive  24       \n"
        ~ "Parrot   Wild     26       \n"
    );

    assert(gp1.combine(["Falcon"]).display(true, 200) == "GroupL1  Type     Max-Speed\n"
        ~ "Falcon   Captive  380      \n"
        ~ "Falcon   Wild     370      \n"
    );

    // Combine both the groups
    assert(gp1.combine([["Falcon"], ["Parrot"]]).display(true, 200) == "GroupL1  Type     Max-Speed\n"
        ~ "Falcon   Captive  380      \n"
        ~ "Falcon   Wild     370      \n"
        ~ "Parrot   Captive  24       \n"
        ~ "Parrot   Wild     26       \n"
    );

    // Just Parrot
    assert(gp1.combine(["Parrot"]).display(true, 200) == "GroupL1  Type     Max-Speed\n"
        ~ "Parrot   Captive  24       \n"
        ~ "Parrot   Wild     26       \n"
    );
}

unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    Group!(int, int, int, int) gp;
    gp.createGroup!([2])(df, [0, 1]);

    // Complete Group
    assert(gp.combine().display(true, 200) == "GroupL1  GroupL2  GroupL3  3      0  1  3  4\n"
        ~ "Hello    Hi       1        Hey    0  0  0  1\n"
        ~ "Hi       Hello    2        Hello  0  0  0  2\n"
        ~ "Hey      Hey      3        Hi     0  0  0  3\n"
    );

    // Single Group
    assert(gp.combine([0]).display(true, 200) == "GroupL1  GroupL2  GroupL3  3    0  1  3  4\n"
        ~ "Hello    Hi       1        Hey  0  0  0  1\n"
    );

    // 2 Groups combine
    assert(gp.combine([0, 1]).display(true, 200) == "GroupL1  GroupL2  GroupL3  3      0  1  3  4\n"
        ~ "Hello    Hi       1        Hey    0  0  0  1\n"
        ~ "Hi       Hello    2        Hello  0  0  0  2\n"
    );

    // Complete group again
    assert(gp.combine([0, 1, 2]).display(true, 200) == "GroupL1  GroupL2  GroupL3  3      0  1  3  4\n"
        ~ "Hello    Hi       1        Hey    0  0  0  1\n"
        ~ "Hi       Hello    2        Hello  0  0  0  2\n"
        ~ "Hey      Hey      3        Hi     0  0  0  3\n"
    );

    // Groups using string array
    assert(gp.combine(["Hello", "Hi", "1"]).display(true, 200) == "GroupL1  GroupL2  GroupL3  3    0  1  3  4\n"
        ~ "Hello    Hi       1        Hey  0  0  0  1\n"
    );

    // Groups using 2D string array
    assert(gp.combine([["Hello", "Hi", "1"], ["Hi", "Hello", "2"]]).display(true, 200) == "GroupL1  GroupL2  GroupL3  3      0  1  3  4\n"
        ~ "Hello    Hi       1        Hey    0  0  0  1\n"
        ~ "Hi       Hello    2        Hello  0  0  0  2\n"
    );
}

// Index operation
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    Group!(int, int, int, int) gp;
    gp.createGroup!([2])(df, [0, 1]);

    assert(gp[["Hello", "Hi", "1"], ["4"]].data == [1]);
    assert(gp[["Hello", "Hi", "1"], ["3"]].data == [0]);

    assert(gp[["Hi", "Hello", "2"], ["4"]].data == [2]);
    assert(gp[["Hi", "Hello", "2"], ["3"]].data == [0]);

    assert(gp[["Hey", "Hey", "3"], ["4"]].data == [3]);
    assert(gp[["Hey", "Hey", "3"], ["3"]].data == [0]);

    assert(gp[["Hello", "Hi", "1"], ["Hey"], 0].data.length == 4);
    assert(gp[["Hello", "Hi", "1"], ["Hey"], 0].data[3] == 1);

    assert(gp[["Hi", "Hello", "2"], ["Hello"], 0].data.length == 4);
    assert(gp[["Hi", "Hello", "2"], ["Hello"], 0].data[3] == 2);

    assert(gp[["Hey", "Hey", "3"], ["Hi"], 0].data.length == 4);
    assert(gp[["Hey", "Hey", "3"], ["Hi"], 0].data[3] == 3);
}

// Index operation
unittest
{
    DataFrame!(double) df1;
    Index inx1;
    inx1.constructFromLevels!(0)([["Falcon", "Parrot"], ["Captive", "Wild"]], ["Animal", "Type"]);
    inx1.constructFromLevels!(1)([["Max-Speed"]]);
    df1.setFrameIndex(inx1);
    df1.assign!1(0, [380.0, 370.0, 24.0, 26.0]);
    // df.display();

    Group!(double) gp;
    gp.createGroup(df1, [0]);
    assert(gp[["Falcon"], ["Max-Speed"]].data == [380.0, 370.0]);
    assert(gp[["Parrot"], ["Max-Speed"]].data == [24.0, 26.0]);

    assert(gp[["Falcon"], ["Captive"], 0].data[0] == 380);
    assert(gp[["Falcon"], ["Wild"], 0].data[0] == 370);
    assert(gp[["Parrot"], ["Captive"], 0].data[0] == 24);
    assert(gp[["Parrot"], ["Wild"], 0].data[0] == 26);

    // No need to pass array if the level of indexing or grouping is 1
    assert(gp["Falcon", "Max-Speed"].data == [380.0, 370.0]);
    assert(gp["Parrot", "Max-Speed"].data == [24.0, 26.0]);

    assert(gp["Falcon", "Captive", 0].data[0] == 380);
    assert(gp["Falcon", "Wild", 0].data[0] == 370);
    assert(gp["Parrot", "Captive", 0].data[0] == 24);
    assert(gp["Parrot", "Wild", 0].data[0] == 26);
}

// Element access using indexes
unittest
{
    DataFrame!(double) df1;
    Index inx1;
    inx1.constructFromLevels!(0)([["Falcon", "Parrot"], ["Captive", "Wild"]], ["Animal", "Type"]);
    inx1.constructFromLevels!(1)([["Max-Speed"]]);
    df1.setFrameIndex(inx1);
    df1.assign!1(0, [380.0, 370.0, 24.0, 26.0]);
    // df.display();

    Group!(double) gp;
    gp.createGroup(df1, [0]);

    assert(gp["Falcon", "Captive", "Max-Speed"] == 380);
    assert(gp[0, 0, 0] == 380);
    assert(gp["Falcon", "Wild", "Max-Speed"] == 370);
    assert(gp[0, 1, 0] == 370);

    assert(gp["Parrot", "Captive", "Max-Speed"] == 24);
    assert(gp[1, 0, 0] == 24);
    assert(gp["Parrot", "Wild", "Max-Speed"] == 26);
    assert(gp[1, 1, 0] == 26);
}

// Element access using index
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    Group!(int, int, int, int) gp;
    gp.createGroup!([2])(df, [0, 1]);

    assert(gp[["Hello", "Hi", "1"], ["Hey"], ["4"]] == 1);
    assert(gp[0, 0, 3] == 1);
    assert(gp[["Hello", "Hi", "1"], ["Hey"], ["3"]] == 0);
    assert(gp[0, 0, 2] == 0);

    assert(gp[["Hi", "Hello", "2"], ["Hello"], ["4"]] == 2);
    assert(gp[1, 0, 3] == 2);
    assert(gp[["Hi", "Hello", "2"], ["Hello"], ["3"]] == 0);
    assert(gp[1, 0, 2] == 0);

    assert(gp[["Hey", "Hey", "3"], ["Hi"], ["4"]] == 3);
    assert(gp[2, 0, 3] == 3);
    assert(gp[["Hey", "Hey", "3"], ["Hi"], ["3"]] == 0);
    assert(gp[2, 0, 2] == 0);

    // Checking different variations
    assert(gp[["Hi", "Hello", "2"], "Hello", ["3"]] == 0);
    assert(gp[["Hi", "Hello", "2"], ["Hello"], "3"] == 0);
    assert(gp[["Hi", "Hello", "2"], "Hello", "3"] == 0);
}

// Assignment operation on rows
unittest
{
    DataFrame!(double) df1;
    Index inx1;
    inx1.constructFromLevels!(0)([["Falcon", "Parrot"], ["Captive", "Wild"]], ["Animal", "Type"]);
    inx1.constructFromLevels!(1)([["Max-Speed"]]);
    df1.setFrameIndex(inx1);
    df1.assign!1(0, [380.0, 370.0, 24.0, 26.0]);
    // df.display();

    Group!(double) gp;
    gp.createGroup(df1, [0]);

    // Row assignment within the same group
    gp[["Falcon"], ["Captive"], 0] = gp[["Falcon"], ["Wild"], 0];
    assert(gp[0, 0, 0] == gp[0, 1, 0]);

    // Row assignment within the same group
    gp[["Parrot"], ["Captive"], 0] = gp[["Parrot"], ["Wild"], 0];
    assert(gp[1, 0, 0] == gp[1, 1, 0]);

    // Row assignment cross group
    gp[["Parrot"], ["Captive"], 0] = gp[["Falcon"], ["Wild"], 0];
    assert(gp[0, 0, 0] == gp[1, 0, 0]);
}

unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    Group!(int, int, int, int) gp;
    gp.createGroup!([2])(df, [0, 1]);

    gp[["Hello", "Hi", "1"], ["3"]] = gp[["Hello", "Hi", "1"], ["4"]];
    assert(gp[["Hello", "Hi", "1"], ["3"]].data == [1]);

    gp[["Hello", "Hi", "1"], ["3"]] = gp[["Hello", "Hi", "1"], ["0"]] + gp[["Hello", "Hi", "1"], ["3"]] + gp[["Hello", "Hi", "1"], ["4"]];
    assert(gp[["Hello", "Hi", "1"], ["3"]].data == [2]);

    gp[["Hello", "Hi", "1"], ["3"]] = gp[["Hello", "Hi", "1"], ["4"]] + gp[["Hi", "Hello", "2"], ["4"]];
    assert(gp[["Hello", "Hi", "1"], ["3"]].data == [3]);
}

// Short hand operations
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    Group!(int, int, int, int) gp;
    gp.createGroup!([2])(df, [0, 1]);

    gp[["Hello", "Hi", "1"], ["3"]] += gp[["Hello", "Hi", "1"], ["4"]];
    assert(gp[["Hello", "Hi", "1"], ["3"]].data == [1]);

    gp[["Hello", "Hi", "1"], ["3"]] -= gp[["Hello", "Hi", "1"], ["0"]] + gp[["Hello", "Hi", "1"], ["3"]] + gp[["Hello", "Hi", "1"], ["4"]];
    assert(gp[["Hello", "Hi", "1"], ["3"]].data == [-1]);

    gp[["Hello", "Hi", "1"], ["3"]] *= gp[["Hello", "Hi", "1"], ["4"]] + gp[["Hi", "Hello", "2"], ["4"]];
    assert(gp[["Hello", "Hi", "1"], ["3"]].data == [-3]);

    gp[["Hello", "Hi", "1"], ["3"]] /= gp[["Hello", "Hi", "1"], ["3"]];
    assert(gp[["Hello", "Hi", "1"], ["3"]].data == [1]);
}
