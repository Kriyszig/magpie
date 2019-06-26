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
    void createGroup(int[] dataLevels, T...)(DataFrame!T df, int[] indexLevels)
    {
        import std.algorithm: reduce, max, min, sort;
        assert(indexLevels.reduce!max < df.indx.row.index.length);
        assert(indexLevels.reduce!min > -1);
        assert(dataLevels.reduce!max < df.RowType.length);
        assert(dataLevels.reduce!min > -1);

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
        
        // This si required as dropper!(dataLevels, df.data) won't work
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

        elementCountTill.length = codes[0];
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
    assert(gp.elementCountTill == [0, 1, 2]);
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