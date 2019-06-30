module magpie.group;

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
        if(groupIndex.length == 0)
        {
            pos.length = groups.length;
            foreach(i; 0 .. groups.length)
                pos[i] = cast(int)i;
        }
        else static if(is(T == int))
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
    string[][] getGroups() @property
    Description: Get the generated string array containing all the groups
    +/
    string[][] getGroups() @property
    {
        return groups;
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
    assert(gp.display([], true, 200) == "3    0  1  3  4  \n"
        ~ "Hey  0  0  0  1  \n\n"
        ~ "3      0  1  3  4  \n"
        ~ "Hello  0  0  0  2  \n\n"
        ~ "3   0  1  3  4  \n"
        ~ "Hi  0  0  0  3  \n\n"
    );

    // Single Group
    assert(gp.display(["Hello", "Hi", "1"], true, 200) == "3    0  1  3  4  \n"
        ~ "Hey  0  0  0  1  \n\n"
    );

    // Array of groups
    assert(gp.display([["Hello", "Hi", "1"], ["Hi", "Hello", "2"]], true, 200) == "3    0  1  3  4  \n"
        ~ "Hey  0  0  0  1  \n\n"
        ~ "3      0  1  3  4  \n"
        ~ "Hello  0  0  0  2  \n\n"
    );

    // Array of groups
    assert(gp.display([["Hello", "Hi", "1"], ["Hi", "Hello", "2"], ["Hey", "Hey", "3"]], true, 200) == "3    0  1  3  4  \n"
        ~ "Hey  0  0  0  1  \n\n"
        ~ "3      0  1  3  4  \n"
        ~ "Hello  0  0  0  2  \n\n"
        ~ "3   0  1  3  4  \n"
        ~ "Hi  0  0  0  3  \n\n"
    );

    // Single Group - integer indexes
    assert(gp.display([0], true, 200) == "3    0  1  3  4  \n"
        ~ "Hey  0  0  0  1  \n\n"
    );

    // Array of groups - integer indexes
    assert(gp.display([0, 1], true, 200) == "3    0  1  3  4  \n"
        ~ "Hey  0  0  0  1  \n\n"
        ~ "3      0  1  3  4  \n"
        ~ "Hello  0  0  0  2  \n\n"
    );

    // Array of groups - integer indexes
    assert(gp.display([0, 1, 2], true, 200) == "3    0  1  3  4  \n"
        ~ "Hey  0  0  0  1  \n\n"
        ~ "3      0  1  3  4  \n"
        ~ "Hello  0  0  0  2  \n\n"
        ~ "3   0  1  3  4  \n"
        ~ "Hi  0  0  0  3  \n\n"
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
    assert(gp.display([], true, 200) == "Type     Max-Speed  \n"
        ~ "Captive  380        \n"
        ~ "Wild     370        \n\n"
        ~ "Type     Max-Speed  \n"
        ~ "Captive  24         \n"
        ~ "Wild     26         \n\n"
    );

    // Display Falcon
    assert(gp.display(["Falcon"], true, 200) == "Type     Max-Speed  \n"
        ~ "Captive  380        \n"
        ~ "Wild     370        \n\n"
    );

    // Display Parrot
    assert(gp.display(["Parrot"], true, 200) == "Type     Max-Speed  \n"
        ~ "Captive  24         \n"
        ~ "Wild     26         \n\n"
    );

    // Display Falcon - Integer Index
    assert(gp.display([0], true, 200) == "Type     Max-Speed  \n"
        ~ "Captive  380        \n"
        ~ "Wild     370        \n\n"
    );

    // Display Parrot - Integer Index
    assert(gp.display([1], true, 200) == "Type     Max-Speed  \n"
        ~ "Captive  24         \n"
        ~ "Wild     26         \n\n"
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
