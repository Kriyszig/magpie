module magpie.dataframe;

import magpie.index: Index;

import std.meta: AliasSeq, Repeat, staticMap;
import std.array: appender;
import std.traits: isType, isBoolean, isArray;

// Template to convert DataFrame template args to RowType
private template getArgsList(args...)
{
    static if(args.length)
    {
        alias arg = args[0];
        import std.traits: isType;
        static if(args.length == 1)
        {
            alias getArgsList = AliasSeq!(arg);
        }
        else
        {
            static if(isType!(args[1]))
            {
                alias getArgsList = AliasSeq!(arg, getArgsList!(args[1 .. $]));
            }
            else
            {
                alias getArgsList = AliasSeq!(Repeat!(args[1],arg), getArgsList!(args[2 .. $]));
            }
        }
    }
    else
        alias getArgsList = AliasSeq!();
}

// Template to get array from type
private alias toArr(T) = T[];

/++
The DataFrame Structure
+/
struct DataFrame(FrameFields...)
    if(FrameFields.length > 0
    && ((!isType!(FrameFields[0]) && is(typeof(FrameFields[0]) == bool) && FrameFields[0] == true && FrameFields.length > 1)
    || isType!(FrameFields[0])))
{
    static if(!isType!(FrameFields[0]) && is(typeof(FrameFields[0]) == bool) && FrameFields[0] == true)
        alias RowType = FrameFields[1 .. $];
    else
        alias RowType = getArgsList!(FrameFields);
    
    alias FrameType = staticMap!(toArr, RowType);

    ///
    size_t rows = 0;
    ///
    size_t cols = RowType.length;

    /// DataFrame indexes
    Index indx;
    /// DataFrame Data
    FrameType data;

private:
    int rowPos(string[] rowindx)
    {
        import std.array: appender;
        import std.algorithm: countUntil;
        import std.conv: to;
        auto codes = appender!(int[]);

        foreach(i; 0 .. cast(int)indx.rcodes.length)
        {
            if(indx.indexes[i].length == 0)
                codes.put(to!int(rowindx[i]));
            else
            {
                int indxpos = cast(int)countUntil(indx.indexes[i], rowindx[i]);
                if(indxpos < 0)
                    return -1;
                codes.put(indxpos);
            }
        }

        foreach(i; 0 .. cast(int)rows)
        {
            bool flag = true;
            foreach(j; 0 .. indx.rcodes.length)
            {
                if(indx.rcodes[j][i] != codes.data[j])
                    flag = false;
            }

            if(flag)
                return i;
        }

        return -1;
    }

    int colPos(string[] colindx)
    {
        import std.array: appender;
        import std.algorithm: countUntil;
        import std.conv: to;
        auto codes = appender!(int[]);

        foreach(i; 0 .. cast(int)indx.ccodes.length)
        {
            if(indx.columns[i].length == 0)
                codes.put(to!int(colindx[i]));
            else
            {
                int indxpos = cast(int)countUntil(indx.columns[i], colindx[i]);
                if(indxpos < 0)
                    return -1;
                codes.put(indxpos);
            }
        }

        foreach(i; 0 .. cast(int)cols)
        {
            bool flag = true;
            foreach(j; 0 .. indx.ccodes.length)
            {
                if(indx.ccodes[j][i] != codes.data[j])
                    flag = false;
            }

            if(flag)
                return i;
        }

        return -1;
    }

public:
    /++
    auto display(bool getStr = false, int maxwidth = 0)
    Description: Converts the given DataFrame into a formatted string to display on the terminal
    @params: getStr - returns the string generated.
    @params: maxwidth - override the width of the terminal.
    +/
    auto display(bool getStr = false, int maxwidth = 0)
    {
        if(rows == 0)
        {
            if(!getStr)
            {
                import std.stdio: writeln;
                writeln("Empty DataFrmae");
            }
            return "";
        }

        const uint terminalw = (maxwidth > 100)? maxwidth: 200;
        const uint maxColSize = 43;
        auto gaps = appender!(size_t[]);

        size_t top, bottom;
        const size_t totalHeight = rows + indx.columns.length +
            ((indx.rtitles.length > 0 && indx.ctitles.length > 0)? 1: 0);
        const size_t totalWidth = cols + indx.indexes.length;

        if(totalHeight > 50)
        {
            top = 25;
            bottom = 25;
        }
        else
        {
            top = totalHeight;
            bottom = 0;
        }

        import std.algorithm: map, reduce, max;
        import std.conv: to;
        size_t dataIndex = 0;
        foreach(i; 0 .. totalWidth)
        {
            int extra = (indx.rtitles.length > 0 && indx.ctitles.length > 0)? 1: 0;
            if(i < indx.indexes.length)
            {
                size_t thisGap = (i < indx.rtitles.length && top > indx.columns.length + extra)? indx.rtitles[i].length: 0;
                if(top > indx.columns.length + extra)
                {
                    size_t tmp = 0;
                    if(indx.rcodes[i].length == 0)
                        tmp = indx.indexes[i][0 .. top - indx.columns.length - extra].map!(e => e.length).reduce!max;
                    else if(indx.indexes[i].length == 0)
                        tmp = indx.rcodes[i][0 .. top - indx.columns.length - extra].map!(e => to!string(e).length).reduce!max;
                    else
                        tmp = indx.rcodes[i][0 .. top - indx.columns.length - extra].map!(e => indx.indexes[i][e].length).reduce!max;
                    
                    if(tmp > thisGap)
                    {
                        thisGap = tmp;
                    }
                }

                if(bottom > 0)
                {
                    if(bottom > indx.indexes[i].length)
                    {
                        size_t tmp = 0;
                        if(indx.rcodes[i].length == 0)
                            tmp = indx.indexes[i].map!(e => e.length).reduce!max;
                        else if(indx.indexes[i].length == 0)
                            tmp = indx.rcodes[i].map!(e => to!string(e).length).reduce!max;
                        else
                            tmp = indx.rcodes[i].map!(e => indx.indexes[i][e].length).reduce!max;
                        
                        if(tmp > thisGap)
                        {
                            thisGap = tmp;
                        }
                    }
                    else
                    {
                        size_t tmp = 0;
                        if(indx.rcodes[i].length == 0)
                            tmp = indx.indexes[i][$ - bottom .. $].map!(e => e.length).reduce!max;
                        else if(indx.indexes[i].length == 0)
                            tmp = indx.rcodes[i][$ - bottom .. $].map!(e => to!string(e).length).reduce!max;
                        else
                            tmp = indx.rcodes[i][$ - bottom .. $].map!(e => indx.indexes[i][e].length).reduce!max;
                        
                        if(tmp > thisGap)
                        {
                            thisGap = tmp;
                        }
                    }
                }

                if(i == indx.indexes.length - 1 && indx.ctitles.length > 0)
                {
                    const auto tmp = (indx.ctitles.length > top)
                        ? indx.ctitles[0 .. top].map!(e => e.length).reduce!max
                        : indx.ctitles.map!(e => e.length).reduce!max;
                    
                    if(tmp > thisGap)
                    {
                        thisGap = tmp;
                    }
                }

                gaps.put((thisGap < maxColSize)? thisGap: maxColSize);
            }
            else
            {
                size_t maxGap = 0;
                foreach(j; 0 .. (top > indx.columns.length)? indx.columns.length: top)
                {
                    size_t lenCol = 0;
                    if(indx.ccodes[j].length == 0)
                        lenCol = indx.columns[j][dataIndex].length;
                    else if(indx.columns[j].length == 0)
                        lenCol = to!string(indx.ccodes[j][dataIndex]).length;
                    else
                        lenCol = indx.columns[j][indx.ccodes[j][dataIndex]].length;
                    
                    maxGap = (maxGap > lenCol)? maxGap: lenCol;
                }

                foreach(j; totalHeight - bottom .. indx.columns.length)
                {
                    size_t lenCol = 0;
                    if(indx.ccodes[j].length == 0)
                        lenCol = indx.columns[j][dataIndex].length;
                    else if(indx.columns[j].length == 0)
                        lenCol = to!string(indx.ccodes[j][dataIndex]).length;
                    else
                        lenCol = indx.columns[j][indx.ccodes[j][dataIndex]].length;
                    
                    maxGap = (maxGap > lenCol)? maxGap: lenCol;
                }

                static foreach(j; 0 .. RowType.length)
                {
                    if(j == dataIndex)
                    {
                        size_t maxsize = data[j].map!(e => to!string(e).length).reduce!max;
                        if(maxsize > maxGap)
                        {
                            maxGap = maxsize;
                        }
                    }
                }

                gaps.put((maxGap < maxColSize)? maxGap: maxColSize);
                ++dataIndex;
            }
        }

        auto cwidth = gaps.data;
        size_t left = 0, right = 0;
        int wOccupied = 0;
        foreach(i; cwidth)
        {
            if(wOccupied + i + 4 < terminalw/2)
            {
                wOccupied += i + 2;
                left++;
            }
            else
                break;
        }

        wOccupied = 0;
        foreach_reverse(i; cwidth)
        {
            if(wOccupied + i + 5 < terminalw/2)
            {
                wOccupied += i + 2;
                right++;
            }
            else
                break;
        }

        if(left + right > cwidth.length)
        {
            left = cwidth.length;
            right = 0;
        }

        auto dispstr = appender!string;
        foreach(ele; [[0, top], [totalHeight - bottom, totalHeight]])
        {
            const int extra = (indx.rtitles.length > 0 && indx.ctitles.length > 0)? 1: 0; 
            if(ele[0] < indx.columns.length + extra)
            {
                dataIndex = 0;
            }
            else
            {
                dataIndex = ele[0] - (indx.columns.length + extra);
            }
            foreach(i; ele[0] .. ele[1])
            {
                bool skipIndex = true;
                foreach(lim; [[0, left], [totalWidth - right, totalWidth]])
                {
                    foreach(j; lim[0] .. lim[1])
                    {
                        if(i < indx.columns.length)
                        {
                            if(j < indx.indexes.length)
                            {
                                if(j == indx.indexes.length - 1 
                                    && indx.ctitles.length != 0)
                                {
                                    if(indx.ctitles[i].length > maxColSize)
                                    {
                                        dispstr.put(indx.ctitles[i][0 .. maxColSize]);
                                        dispstr.put("  ");
                                    }
                                    else
                                    {
                                        dispstr.put(indx.ctitles[i]);
                                        foreach(k;indx.ctitles[i].length .. cwidth[j] + 2)
                                        {
                                            dispstr.put(" ");
                                        }
                                    }
                                }
                                else if(i == indx.columns.length - 1
                                    && indx.ctitles.length == 0)
                                {
                                    if(indx.rtitles[j].length > maxColSize)
                                    {
                                        dispstr.put(indx.rtitles[j][0 .. maxColSize]);
                                        dispstr.put("  ");
                                    }
                                    else
                                    {
                                        dispstr.put(indx.rtitles[j]);
                                        foreach(k; indx.rtitles[j].length .. cwidth[j] + 2)
                                        {
                                            dispstr.put(" ");
                                        }
                                    }
                                }
                                else
                                {
                                    foreach(k; 0 .. cwidth[j] + 2)
                                    {
                                        dispstr.put(" ");
                                    }
                                }
                            }
                            else
                            {
                                string colindx ="";
                                if(indx.ccodes[i].length == 0)
                                    colindx = indx.columns[i][j - indx.indexes.length];
                                else if(indx.columns[i].length == 0)
                                    colindx = to!string(indx.ccodes[i][j - indx.indexes.length]);
                                else
                                    colindx = indx.columns[i][indx.ccodes[i][j - indx.indexes.length]];

                                if(colindx.length > maxColSize)
                                {
                                    dispstr.put(colindx[0 .. maxColSize]);
                                    dispstr.put("  ");
                                }
                                else
                                {
                                    dispstr.put(colindx);
                                    foreach(k; colindx.length .. cwidth[j] + 2)
                                    {
                                        dispstr.put(" ");
                                    }
                                }
                            }
                        }
                        else if(i == indx.columns.length && indx.ctitles.length != 0)
                        {
                            if(j < indx.rtitles.length)
                            {
                                if(indx.rtitles[j].length > maxColSize)
                                {
                                    dispstr.put(indx.rtitles[j][0 .. maxColSize]);
                                    dispstr.put("  ");
                                }
                                else
                                {
                                    dispstr.put(indx.rtitles[j]);
                                    foreach(k; indx.rtitles[j].length .. cwidth[j] + 2)
                                    {
                                        dispstr.put(" ");
                                    }
                                }
                            }
                        }
                        else
                        {
                            if(j < indx.indexes.length)
                            {
                                string idx = "";
                                if(indx.rcodes[j].length == 0)
                                    idx = indx.indexes[j][dataIndex];
                                else if(indx.indexes[j].length == 0)
                                    idx = to!string(indx.rcodes[j][dataIndex]);
                                else if(dataIndex > 0 && j < indx.indexes.length
                                    && indx.rcodes[j][dataIndex] == indx.rcodes[j][dataIndex - 1]
                                    && skipIndex && indx.isMultiIndexed)
                                    idx = "";
                                else
                                {
                                    idx = indx.indexes[j][indx.rcodes[j][dataIndex]];
                                    skipIndex = false;
                                }

                                if(idx.length > maxColSize)
                                {
                                    dispstr.put(idx[0 .. maxColSize]);
                                    dispstr.put("  ");
                                }
                                else
                                {
                                    dispstr.put(idx);
                                    foreach(k; idx.length .. cwidth[j] + 2)
                                    {
                                        dispstr.put(" ");
                                    }
                                }
                            }
                            else
                            {
                                string idx = "";
                                static foreach(k; 0 .. RowType.length)
                                {
                                    if(k == j - indx.indexes.length)
                                        idx = to!string(data[k][dataIndex]);
                                }

                                if(idx.length > maxColSize)
                                {
                                    dispstr.put(idx[0 .. maxColSize]);
                                    dispstr.put("  ");
                                }
                                else
                                {
                                    dispstr.put(idx);
                                    foreach(k; idx.length .. cwidth[j] + 2)
                                    {
                                        dispstr.put(" ");
                                    }
                                }
                            }
                        }
                    }
                    if(right > 0 && lim[0] == 0)
                    {
                        dispstr.put("...  ");
                    }
                }
                dispstr.put("\n");
                if(i >= indx.columns.length + extra)
                    ++dataIndex;
            }
            if(bottom > 0 && ele[0] == 0)
            {
                foreach(i; 0 .. left)
                {
                    foreach(j; 0 .. cwidth[i])
                        dispstr.put(".");
                    dispstr.put("  ");
                }

                if(right > 0)
                {
                    dispstr.put("...  ");
                }

                foreach(i; cwidth.length - right .. cwidth.length)
                {
                    foreach(j; 0 .. cwidth[i])
                        dispstr.put(".");
                    dispstr.put("  ");
                }
                dispstr.put("\n");
            }
        }
        
        import std.stdio: writeln;
        if(!getStr)
        {
            writeln(dispstr.data);
            // writeln(RowType.length);
            // writeln(gaps.data);
            // writeln(left,"\t", right);

            writeln("Dataframe Dimension: [ ", totalHeight," X ", totalWidth, " ]");
            writeln("Data Dimension: [ ", rows," X ", cols, " ]");
        }
        return ((getStr)? dispstr.data: "");
    }

    /++
    void to_csv(string path, bool writeIndex = true, bool writeColumns = true, char sep = ',')
    Description: Writes given DataFrame to a CSV file
    @params: path - path to the output file
    @params: writeIndex - write row indexes to the file
    @params: writeColumns - write column indexes to the file
    @params: sep - data seperator
    +/
    void to_csv(string path, bool writeIndex = true, bool writeColumns = true, char sep = ',')
    {
        import std.array: appender;
        import std.conv: to;

        auto formatter = appender!(string);
        const size_t totalHeight = rows + indx.columns.length +
            ((indx.rtitles.length > 0 && indx.ctitles.length > 0)? 1: 0);
        
        if(rows == 0)
        {
            return;
        }
        
        if(writeColumns)
        {
            foreach(i; 0 .. indx.columns.length)
            {
                if(writeIndex)
                {
                    foreach(j; 0 .. indx.indexes.length)
                    {
                        if(i != indx.columns.length - 1 && j < indx.indexes.length - 1)
                        {
                            formatter.put(sep);
                        }
                        else if(i == indx.columns.length - 1 && indx.ctitles.length == 0)
                        {
                            formatter.put(indx.rtitles[j]);
                            formatter.put(sep);
                        }
                        else if(j == indx.indexes.length - 1 && indx.ctitles.length != 0)
                        {
                            formatter.put(indx.ctitles[i]);
                            formatter.put(sep);
                        }
                        else
                        {
                            formatter.put(sep);
                        }
                    }
                }

                foreach(j; 0 .. cols)
                {
                    string colindx ="";
                    if(indx.ccodes[i].length == 0)
                        colindx = indx.columns[i][j];
                    else if(indx.columns[i].length == 0)
                        colindx = to!string(indx.ccodes[i][j]);
                    else
                        colindx = indx.columns[i][indx.ccodes[i][j]];
                    
                    formatter.put(colindx);
                    if(j < cols - 1)
                        formatter.put(sep);
                }

                formatter.put("\n");
            }
            if(indx.ctitles.length != 0 && writeIndex)
            {
                formatter.put(indx.rtitles[0]);
                foreach(j; 1 .. indx.indexes.length)
                {
                    formatter.put(sep);
                    formatter.put(indx.rtitles[j]);
                }
                formatter.put("\n");
            }
        }

        foreach(i; 0 .. rows)
        {
            if(writeIndex)
            {
                bool skipIndex = true;
                foreach(j; 0 .. indx.indexes.length)
                {
                    string idx = "";
                    if(indx.rcodes[j].length == 0)
                        idx = indx.indexes[j][i];
                    else if(indx.indexes[j].length == 0)
                        idx = to!string(indx.rcodes[j][i]);
                    else if(i > 0 && j < indx.indexes.length
                        && indx.rcodes[j][i] == indx.rcodes[j][i - 1]
                        && skipIndex && indx.isMultiIndexed)
                        idx = "";
                    else
                    {
                        idx = indx.indexes[j][indx.rcodes[j][i]];
                        skipIndex = false;
                    }

                    formatter.put(idx);
                    formatter.put(sep);
                }
            }
            formatter.put(to!string(data[0][i]));
            static foreach(j; 1 .. RowType.length)
            {
                formatter.put(sep);
                formatter.put(to!string(data[j][i]));
            }

            formatter.put("\n");
        }

        import std.stdio: File;
        File outfile = File(path, "w");
        outfile.write(formatter.data);
        outfile.close();
    }

    /++
    void from_csv(string path, size_t indexDepth, size_t columnDepth, size_t[] columns = [], char sep = ',')
    Description: Parsing of DataFrame from a CSV file
    @params: path - File path of csv file
    @params: indexDepth - Number of columns row indexes span
    @params: columnDepth - Number of rows column indexes span
    @params: columns - Integer indexes of column to selectively parse
    @params: sep - Data seperator in the file 
    +/
    void from_csv(string path, size_t indexDepth, size_t columnDepth, size_t[] columns = [], char sep = ',')
    {
        import std.array: appender, split;
        import std.stdio: File;
        import std.string: chomp;

        if(columns.length == 0)
        {
            auto all = appender!(size_t[]);
            foreach(i; 0 .. cols)
                all.put(i);
            columns = all.data;
        }
        
        assert(columns.length == cols, "The dimension of columns[ ] must be same as dimension of the DataFrame");

        File csvfile = File(path, "r");
        bool bothTitle = false;
        size_t line = 0;
        indx = Index();

        foreach(i; 0 .. indexDepth)
        {
            indx.rcodes ~= [[]];
            indx.indexes ~= [[]];
        }
        
        size_t dataIndex = 0;
        while(!csvfile.eof())
        {
            string[] fields = chomp(csvfile.readln()).split(sep);

            if(line < columnDepth)
            {
                if(indexDepth > 0 && line == columnDepth - 1 && fields[0].length > 0)
                {
                    indx.rtitles = fields[0 .. indexDepth];
                }
                else if(indexDepth > 0 && fields[indexDepth - 1].length > 0)
                {
                    indx.ctitles ~= fields[indexDepth - 1];
                    bothTitle = true;
                } 

                indx.columns ~= [[]];
                indx.ccodes ~= [[]];
                foreach(i; 0 .. cols)
                {
                    size_t pos = columns[i];
                    string colindx = fields[indexDepth + pos];
                    
                    if(i > 0 && colindx.length == 0)
                    {
                        indx.ccodes[line] ~= indx.ccodes[line][$ - 1];
                    }
                    else
                    {
                        import std.algorithm: countUntil;
                        int idxpos = cast(int)countUntil(indx.columns[line], colindx);
                        
                        if(idxpos > -1)
                        {
                            indx.ccodes[line] ~= cast(int)idxpos;
                        }
                        else
                        {
                            indx.columns[line] ~= colindx;
                            indx.ccodes[line] ~= cast(uint)indx.columns[line].length - 1;
                        }
                    }
                }
            }
            else if(line == columnDepth && bothTitle)
            {
                indx.rtitles = fields[0 .. indexDepth];
            }
            else
            {
                if(indexDepth == 1 && columnDepth == 1 && line == columnDepth && fields.length == 1)
                {
                    bothTitle = true;
                    indx.rtitles = fields;
                }
                else if(fields.length > 0)
                {
                    foreach(i; 0 .. indexDepth)
                    {
                        import std.algorithm: countUntil;
                        int indxpos = cast(int)countUntil(indx.indexes[i], fields[i]);
                        if(fields[i].length == 0 && dataIndex > 0)
                        {
                            indx.rcodes[i] ~= indx.rcodes[$ - 1];
                        }
                        else if(indxpos > -1)
                        {
                            indx.rcodes[i] ~= cast(uint)indxpos;
                        }
                        else
                        {
                            indx.indexes[i] ~= fields[i];
                            indx.rcodes[i] ~= cast(uint)indx.indexes[i].length - 1;
                        }
                    }

                    static foreach(i; 0 .. RowType.length)
                    {
                        
                        if(fields.length > (columns[i] + indexDepth))
                        {
                            import std.conv: to, ConvException;
                            
                            try
                            {
                                data[i] ~= to!(RowType[i])(fields[columns[i] + indexDepth]);
                            }
                            catch(ConvException e)
                            {
                                data[i] ~= RowType[i].init;
                            }
                        }
                        else
                        {
                            data[i] ~= RowType[i].init;
                        }
                    }
                }
            }

            if(fields.length > 0)
                ++line;
        }
        csvfile.close();

        rows = line - columnDepth - ((bothTitle)?1: 0);

        if(indexDepth == 0)
        {
            indx.rtitles ~= "Index";
            indx.indexes = [[]];
            indx.rcodes = [[]];
            foreach(i; 0 .. cast(uint)rows)
                indx.rcodes[0] ~= i;
        }

        if(columnDepth == 0)
        {
            indx.columns = [[]];
            indx.ccodes = [[]];
            foreach(i; 0 .. indexDepth)
                indx.rtitles ~= ["Index"];
            foreach(i; 0 .. cast(uint)line)
                indx.ccodes[0] ~= i;
        }

        indx.optimize();
    }

    /++
    RowType[i2] at(size_t i1, size_t i2)()
    Description: Getting the element directly from its index
    @param: ii - Row index
    @param: i2 - Column index
    +/
    RowType[i2] at(size_t i1, size_t i2)() @property
        if(i2 < RowType.length)
    {
        return data[i2][i1];
    }

    /++
    void opIndexAssign(Args...)(Args ele, size_t i1, size_t i2)
    Description: Setting the element at an index
    @param: ii - Row index
    @param: i2 - Column index
    +/
    void opIndexAssign(Args...)(Args ele, size_t i1, size_t i2)
        if(Args.length > 0)
    {
        assert(i1 < rows && i2 < cols, "Index out of bound");
        static foreach(i; 0 .. RowType.length)
        {
            if(i == i2)
            {
                data[i][i1] = ele[0];
            }
        }
    }

    /++
    void opIndexAssign(Args...)(Args ele, string[] rindx, string[] cindx)
    Description: Setting the element at an index
    @param: rindx - Row index
    @param: cindx - Column index
    +/
    void opIndexAssign(Ele)(Ele ele, string[] rindx, string[] cindx)
    {
        assert(rindx.length == indx.rcodes.length, "Size of indexes don't match the levels of row indexes");
        assert(cindx.length == indx.ccodes.length, "Size of indexes don't match the levels of column indexes");

        int i1 = rowPos(rindx);
        int i2 = colPos(cindx);

        assert(i1 > -1 && i2 > -1, "Given headers don't match DataFrame Headers");
        static foreach(i; 0 .. RowType.length)
        {
            if(i == i2)
            {
                data[i][i1] = ele;
            }
        }
    }

    /++
    int getRowPos(string[] indexes)
    Description: Get integer index of the given row headers
    Defaults to -1 if headers don't match
    @params: indexes - Array of indexes
    +/
    int getRowPosition(string[] indexes)
    {
        assert(indexes.length == indx.rcodes.length, "Size of indexes don't match the levels of row indexes");
        return rowPos(indexes);
    }

    /++
    int getColPos(string[] indexes)
    Description: Get integer index of the given column headers
    Defaults to -1 if headers don't match
    @params: indexes - Array of indexes
    +/
    int getColumnPosition(string[] indexes)
    {
        assert(indexes.length == indx.rcodes.length, "Size of indexes don't match the levels of column indexes");
        return colPos(indexes);
    }

    /++
    void setFrameIndex(Index index)
    Description: Sets the frame indexes.
    [Please use setIndex method of Index to set the index]
    @params: index - Index structure to replace indexing
    +/
    void setFrameIndex(Index index)
    {
        assert(index.rcodes.length > 0 || index.ccodes.length > 0
            || index.rtitles.length > 0 || index.ctitles.length > 0,
            "Cannot set empty index to DataFrame");
        
        bool needsPadding = false;
        if(index.ccodes.length > 0 && index.ccodes[0].length == cols)
        {
            indx.ccodes = index.ccodes;
            indx.columns = index.columns;
            indx.ctitles = index.ctitles;
        }

        if(index.ccodes.length == 0 && index.ctitles.length == indx.ccodes.length)
        {
            indx.ctitles = index.ctitles;
        }

        if(index.rcodes.length > 0 && index.rcodes[0].length >= rows)
        {
            indx.rcodes = index.rcodes;
            indx.indexes = index.indexes;
            indx.rtitles = index.rtitles;

            if(index.rcodes[0].length != rows)
                needsPadding = true;
            
            rows = index.rcodes[0].length;
        }

        if(index.rcodes.length == 0 && index.rtitles.length == indx.rcodes.length)
        {
            indx.rtitles = index.rtitles;
        }

        if(indx.ccodes.length == 0)
        {
            indx.columns = [[]];
            indx.ccodes = [[]];
            foreach(i; 0 .. cast(int)cols)
                indx.ccodes[0] ~= i;
        }

        if(needsPadding)
        {
            static foreach(i; 0 .. RowType.length)
            {
                foreach(j; data[i].length .. rows)
                    data[i] ~= RowType[i].init;
            }
        }
    }

    void opAssign(T)(T input)
        if(isArray!(T))
    {
        import std.algorithm: map, reduce, max;
        size_t l1 = input.length;
        size_t l2 = input.map!(e => e.length).reduce!max;
        assert(l1 > 0 && l2 > 0, "Cannot assign empty array to DataFrame");
        assert(l1 <= rows, "Cannot implicitly assign values of length larger than that of the DataFrame");
        assert(l2 <= cols, "Cannot implicitly assign values of dimension larger than that of the DataFrame");

        foreach(i; 0 .. l1)
        {
            static foreach(j; 0 .. RowType.length)
            {
                if(input[i].length > j)
                    data[j][i] = cast(RowType[j])input[i][j];
                else
                    data[j][i] = RowType[j].init;
            }
        }

        foreach(i; l1 .. rows)
        {
            static foreach(j; 0 .. RowType.length)
            {
                data[j][i] = RowType[j].init;
            }
        }
    }

    /++
    void assign(int axis, T, U...)(T index, U values)
    Description: Assign values to rows or columns
    @params: axis - 0 for rows, 1 for columns
    @params: index - string[] or integer index of the location to assign
    @params: values - values to assign
    +/
    void assign(int axis, T, U...)(T index, U values)
    {
        static if(axis == 0)
        {
            int pos;
            static if(is(T == int))
                pos = index;
            else
                pos = rowPos(index);
            
            assert(pos > -1 && pos < rows, "Index out of bound");

            static foreach(i; 0 .. (RowType.length > U.length)? U.length: RowType.length)
                data[i][pos] = values[i];
        }
        else
        {
            static assert(isArray!(U[0]), "Assignable data needs to be a 1D array");
            int pos;
            static if(is(T == int))
                pos = index;
            else
                pos = colPos(index);

            import std.stdio;
            assert(pos > -1 && pos < cols, "Index out of bound");
            assert(values[0].length <= rows, "Assignable data larger than DataFrame");

            static foreach(i; 0 .. FrameType.length)
            {
                if(i == pos)
                {
                    static if(is(U[0] == FrameType[i]))
                    {
                        foreach(j; 0 .. values[0].length)
                            data[i][j] = values[0][j];
                    }
                }
            }
        }
    }
}

// Testing DataFrame Definition - O(n + log(n))
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));
}

// O(log(n)) init
unittest
{
    DataFrame!(true, double, double, double) df;
    assert(is(typeof(df.data) == Repeat!(3, double[])));
}

// Initialize from struct - O(log(n))
unittest
{
    struct Example
    {
        int x;
        double y;
    }

    import std.traits: Fields;
    DataFrame!(true, Fields!Example) df;
    assert(is(typeof(df.data) == AliasSeq!(int[], double[])));
}

// Getting element from it's index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1"];
    df.indx.indexes = [["Hello", "Hi"]];
    df.indx.rcodes = [[]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"]];
    df.indx.ccodes = [[]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];

    assert(df.at!(0,0) == 1);
    assert(df.at!(0,1) == 1);
    assert(df.at!(1,0) == 2);
    assert(df.at!(1,1) == 2);
}

// Setting element at an index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1"];
    df.indx.indexes = [["Hello", "Hi"]];
    df.indx.rcodes = [[]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"]];
    df.indx.ccodes = [[]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];

    df[0, 0] = 42;
    assert(df.data[0] == [42, 2]);
}

// Assignment based on string headers
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1, 24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[0,1],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];

    df[["Hello", "Hello", "1"], ["Hello", "1", "Hello"]] = 48;
    assert(df.data[0] == [48, 2]);
    df[["Hi", "Hello", "24"], ["Hello", "1", "Hello"]] = 29;
    assert(df.data[0] == [48, 29]);
    df[["Hello", "Hello", "1"], ["Hi", "2", "Hello"]] = 96;
    assert(df.data[1] == [96, 2]);
    df[["Hi", "Hello", "24"], ["Hi", "2", "Hello"]] = 43;
    assert(df.data[1] == [96, 43]);
}


// getiing integer position of the given row indexes
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];

    assert(df.getRowPosition(["Hello", "Hello", "1"]) == 0);
    assert(df.getRowPosition(["Hi", "Hello", "24"]) == 1);
    assert(df.getRowPosition(["Hi", "Hello", "54"]) == -1);
    assert(df.getRowPosition(["Hi", "Helo", "24"]) == -1);
    assert(df.getRowPosition(["H", "Hello", "54"]) == -1);
}

// getiing integer position of the given column indexes
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[0,1],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];

    assert(df.getColumnPosition(["Hello", "1", "Hello"]) == 0);
    assert(df.getColumnPosition(["Hi", "2", "Hello"]) == 1);
    assert(df.getColumnPosition(["Hello", "1", "Hell"]) == -1);
    assert(df.getColumnPosition(["Hello", "45", "Hello"]) == -1);
}

// Setting index to DataFrame
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([1,2,3,4,5], ["Index"]);
    df.setFrameIndex(inx);
    string ret = df.display(true, 200);
    assert(ret == "Index  0    1  \n"
        ~ "1      nan  0  \n"
        ~ "2      nan  0  \n"
        ~ "3      nan  0  \n"
        ~ "4      nan  0  \n"
        ~ "5      nan  0  \n"
    );
}

// Checking if setting index works as intended when assigning using the set index
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([1,2,3,4,5], ["Index"], [1, 2], ["Index"]);
    df.setFrameIndex(inx);
    df[["1"], ["2"]] = 42;
    assert(df.data[1] == [42,0,0,0,0]);
}

// Checking if setting index works as intended when assigning using the set index
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [1, 2], ["Index"]);
    df.setFrameIndex(inx);
    df[["Hello", "Hi"], ["2"]] = 42;
    assert(df.data[1] == [42,0]);
}

// Checking if setting index works as intended when assigning using the set index
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [["Hello", "Hi"], ["Hi", "Hello"]]);
    df.setFrameIndex(inx);
    df[["Hello", "Hi"], ["Hi", "Hello"]] = 42;
    assert(df.data[1] == [42,0]);
}

// Basic Assignment operation
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [["Hello", "Hi"], ["Hi", "Hello"]]);
    df.setFrameIndex(inx);

    // Assignment
    df = [[1, 2], [3, 4]];
    // df.display();
    assert(df.data[0] == [1,3]);
    assert(df.data[1] == [2,4]);

    // Assignment that needs apdding
    df = [[1], [2,3]];
    // df.display();
    assert(df.data[0] == [1,2]);
    assert(df.data[1] == [0,3]);

    // Checking casting
    df = [[1.2, 1], [4.6, 7]];
    // df.display();
    assert(df.data[0] == [1.2, 4.6]);
    assert(df.data[1] == [1, 7]);
}

// Assigning an entire column & row to DataFrame
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [["Hello", "Hi"], ["Hi", "Hello"]]);
    df.setFrameIndex(inx);
    df.RowType ele;
    ele[0] = 1.77;
    ele[1] = 4;

    // Using RowType alais
    df.assign!0(["Hi", "Hello"], ele);
    assert(df.data[0][1] == 1.77);
    assert(df.data[1][1] == 4);

    // Without RowType
    df.assign!0(["Hi", "Hello"], 1.688, 6);
    assert(df.data[0][1] == 1.688);
    assert(df.data[1][1] == 6);
    
    // Assigning usig direct index
    df.assign!0(1, 1.588, 6);
    assert(df.data[0][1] == 1.588);
    assert(df.data[1][1] == 6);
    
    // Assigning column
    df.assign!1(["Hello", "Hi"], [1.2, 3.6]);
    assert(df.data[0] == [1.2, 3.6]);
    
    // Assigning columns using direct index
    df.assign!1(0, [1.26, 4.6]);
    assert(df.data[0] == [1.26, 4.6]);
    
    // Partial Assignment - rows
    df.assign!0(1, 3.588);
    assert(df.data[0][1] == 3.588);
    assert(df.data[1][1] == 6);

    // Partial Assignment - columns
    df.assign!1(0, [2.26]);
    assert(df.data[0] == [2.26, 3.588]);
}

// Simple Data Frame
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1"];
    df.indx.indexes = [["Hello", "Hi"]];
    df.indx.rcodes = [[]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"]];
    df.indx.ccodes = [[]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);
    assert(ret == "Index1  Hello  Hi  \n"
        ~ "Hello   1      1   \n"
        ~ "Hi      2      2   \n"
    );
}

// Simple DataFrame with both row and column index title
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1"];
    df.indx.indexes = [["Hello", "Hi"]];
    df.indx.rcodes = [[]];
    df.indx.ctitles = ["Also Index"];
    df.indx.columns = [["Hello","Hi"]];
    df.indx.ccodes = [[]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);

    assert(ret == "Also Index  Hello  Hi  \n"
        ~ "Index1      \n"
        ~ "Hello       1      1   \n"
        ~ "Hi          2      2   \n"
    );
}

// Multi-Indexed rows
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2"];
    df.indx.indexes = [["Hello", "Hi"], ["Hello", "Hi"]];
    df.indx.rcodes = [[],[]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"]];
    df.indx.ccodes = [[]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);
    
    assert(ret == "Index1  Index2  Hello  Hi  \n"
        ~ "Hello   Hello   1      1   \n"
        ~ "Hi      Hi      2      2   \n"
    );
}
// Multi Indexed Columns
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1"];
    df.indx.indexes = [["Hello", "Hi"]];
    df.indx.rcodes = [[]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"], ["Hello","Hi"]];
    df.indx.ccodes = [[],[]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);
    
    assert(ret == "        Hello  Hi  \n"
        ~ "Index1  Hello  Hi  \n"
        ~ "Hello   1      1   \n"
        ~ "Hi      2      2   \n"
    );
}

// Multi Indexed Columns with titles
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1"];
    df.indx.indexes = [["Hello", "Hi"]];
    df.indx.rcodes = [[]];
    df.indx.ctitles = ["CIndex1", "CIndex2"];
    df.indx.columns = [["Hello","Hi"], ["Hello","Hi"]];
    df.indx.ccodes = [[],[]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);
    
    assert(ret == "CIndex1  Hello  Hi  \n"
        ~ "CIndex2  Hello  Hi  \n"
        ~ "Index1   \n"
        ~ "Hello    1      1   \n"
        ~ "Hi       2      2   \n"
    );
}

// Wide DataFrame
unittest
{
    DataFrame!(int, 20) df;
    assert(is(typeof(df.data) == Repeat!(20, int[])));

    df.indx.rtitles = ["Index1"];
    df.indx.indexes = [["Hello", "Hi"]];
    df.indx.rcodes = [[]];
    df.indx.ctitles = [];
    df.indx.columns = [[]];
    df.indx.ccodes = [[]];
    df.rows = 2;
    int[] arr = [12_222_222, 12_222_222];

    static foreach(i; 0 .. 20)
    {
        import std.conv: to;
        df.indx.columns[0] ~= to!string(i);
        df.data[i] = arr;
    }

    string ret = df.display(true, 200);
    assert(ret == "Index1  0         1         2         3         4         5         6         7         ...  11        12        13        14        15        16        17        18        19        \n"
        ~ "Hello   12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  ...  12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  \n"
        ~ "Hi      12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  ...  12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  \n"
    );
}

// Daddy Long Legs DataFrame
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1"];
    df.indx.indexes = [[]];
    df.indx.rcodes = [[]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"]];
    df.indx.ccodes = [[]];
    df.rows = 100;
    int[] arr = [];
    foreach(i; 0 .. 100)
    {
        arr ~= i;
        import std.conv: to;
        df.indx.indexes[0] ~= to!string(i);
    }
    df.data[0] = arr;
    df.data[1] = arr;
    string ret = df.display(true, 200);

    assert(ret == "Index1  Hello  Hi  \n"
        ~ "0       0      0   \n"
        ~ "1       1      1   \n"
        ~ "2       2      2   \n"
        ~ "3       3      3   \n"
        ~ "4       4      4   \n"
        ~ "5       5      5   \n"
        ~ "6       6      6   \n"
        ~ "7       7      7   \n"
        ~ "8       8      8   \n"
        ~ "9       9      9   \n"
        ~ "10      10     10  \n"
        ~ "11      11     11  \n"
        ~ "12      12     12  \n"
        ~ "13      13     13  \n"
        ~ "14      14     14  \n"
        ~ "15      15     15  \n"
        ~ "16      16     16  \n"
        ~ "17      17     17  \n"
        ~ "18      18     18  \n"
        ~ "19      19     19  \n"
        ~ "20      20     20  \n"
        ~ "21      21     21  \n"
        ~ "22      22     22  \n"
        ~ "23      23     23  \n"
        ~ "......  .....  ..  \n"
        ~ "75      75     75  \n"
        ~ "76      76     76  \n"
        ~ "77      77     77  \n"
        ~ "78      78     78  \n"
        ~ "79      79     79  \n"
        ~ "80      80     80  \n"
        ~ "81      81     81  \n"
        ~ "82      82     82  \n"
        ~ "83      83     83  \n"
        ~ "84      84     84  \n"
        ~ "85      85     85  \n"
        ~ "86      86     86  \n"
        ~ "87      87     87  \n"
        ~ "88      88     88  \n"
        ~ "89      89     89  \n"
        ~ "90      90     90  \n"
        ~ "91      91     91  \n"
        ~ "92      92     92  \n"
        ~ "93      93     93  \n"
        ~ "94      94     94  \n"
        ~ "95      95     95  \n"
        ~ "96      96     96  \n"
        ~ "97      97     97  \n"
        ~ "98      98     98  \n"
        ~ "99      99     99  \n"
    );
}

// Multi Indexed DataFrame
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);
    assert(ret == "                        Hello  Hi     \n"
        ~ "                        1      2      \n"
        ~ "Index1  Index2  Index3  Hello  Hello  \n"
        ~ "Hello   Hello   1       1      1      \n"
        ~ "Hi      Hello   24      2      2      \n"
    );
}

// Multi Indexed DataFrame with both row and column titles
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);
    assert(ret == "                Hey     Hello  Hi     \n"
        ~ "                Hey     1      2      \n"
        ~ "                Hey     Hello  Hello  \n"
        ~ "Index1  Index2  Index3  \n"
        ~ "Hello   Hello   1       1      1      \n"
        ~ "Hi      Hello   24      2      2      \n"
    );

    // df.to_csv("");
}

// Multi-Indexed with skipping row index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[1, 1], [0, 0], [1,24]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.indx.isMultiIndexed = true;
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);
    assert(ret == "                        Hello  Hi     \n"
        ~ "                        1      2      \n"
        ~ "Index1  Index2  Index3  Hello  Hello  \n"
        ~ "Hi      Hello   1       1      1      \n"
        ~ "                24      2      2      \n"
    );
}

// Middle Indexes won't skip if the outer indexes aren't skipped
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[1, 0], [0, 0], [1,24]];
    df.indx.ctitles = [];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.indx.isMultiIndexed = true;
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    string ret = df.display(true, 200);
    
    assert(ret == "                        Hello  Hi     \n"
        ~ "                        1      2      \n"
        ~ "Index1  Index2  Index3  Hello  Hello  \n"
        ~ "Hi      Hello   1       1      1      \n"
        ~ "Hello   Hello   24      2      2      \n"
    );

    // df.to_csv("", true, false);
}

// Writing entire dataframe to csv
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    
    df.to_csv("./test/tocsv/ex1p1.csv");
    string[] data = [
        ",,Hey,Hello,Hi",
        ",,Hey,1,2",
        ",,Hey,Hello,Hello",
        "Index1,Index2,Index3",
        "Hello,Hello,1,1,1",
        "Hi,Hello,24,2,2",
        ""
    ];

    import std.stdio: File;
    import std.string: chomp;
    int line = 0;
    File csv = File("./test/tocsv/ex1p1.csv");
    while(!csv.eof())
    {
        assert(chomp(csv.readln()) == data[line]);
        ++line;
    }
    csv.close();
}

// Writing only row index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    
    df.to_csv("./test/tocsv/ex1p2.csv", true, false);
    string[] data = [
        "Hello,Hello,1,1,1",
        "Hi,Hello,24,2,2",
        ""
    ];

    import std.stdio: File;
    import std.string: chomp;
    int line = 0;
    File csv = File("./test/tocsv/ex1p2.csv");
    while(!csv.eof())
    {
        assert(chomp(csv.readln()) == data[line]);
        ++line;
    }
    csv.close();
}

// Writing only column index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    
    df.to_csv("./test/tocsv/ex1p3.csv", false, true);
    string[] data = [
        "Hello,Hi",
        "1,2",
        "Hello,Hello",
        "1,1",
        "2,2",
        ""
    ];

    import std.stdio: File;
    import std.string: chomp;
    int line = 0;
    File csv = File("./test/tocsv/ex1p3.csv");
    while(!csv.eof())
    {
        assert(chomp(csv.readln()) == data[line]);
        ++line;
    }
    csv.close();
}

// Writing only data to the csv
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    
    df.to_csv("./test/tocsv/ex1p4.csv", false, false);
    string[] data = [
        "1,1",
        "2,2",
        ""
    ];

    import std.stdio: File;
    import std.string: chomp;
    int line = 0;
    File csv = File("./test/tocsv/ex1p4.csv");
    while(!csv.eof())
    {
        assert(chomp(csv.readln()) == data[line]);
        ++line;
    }
    csv.close();
}

// Changing seperator
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == Repeat!(2, int[])));

    df.indx.rtitles = ["Index1", "Index2", "Index3"];
    df.indx.indexes = [["Hello", "Hi"],["Hello"], []];
    df.indx.rcodes = [[0, 1], [0, 0], [1,24]];
    df.indx.ctitles = ["Hey","Hey","Hey"];
    df.indx.columns = [["Hello","Hi"],[],["Hello"]];
    df.indx.ccodes = [[],[1,2],[0,0]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];
    
    df.to_csv("./test/tocsv/ex1p5.csv", false, false, '|');
    string[] data = [
        "1|1",
        "2|2",
        ""
    ];

    import std.stdio: File;
    import std.string: chomp;
    int line = 0;
    File csv = File("./test/tocsv/ex1p5.csv");
    while(!csv.eof())
    {
        assert(chomp(csv.readln()) == data[line]);
        ++line;
    }
    csv.close();
}

// Parsing a CSV
unittest
{
    DataFrame!(int, 2) df;
    df.from_csv("./test/tocsv/ex1p1.csv", 3, 3);
    // df.display();
    df.to_csv("./test/tocsv/ex2p1.csv");

    import std.stdio: File;
    File f1 = File("./test/tocsv/ex1p1.csv", "r");
    File f2 = File("./test/tocsv/ex2p1.csv", "r");

    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// PArsing CSV without column headers
unittest
{
    DataFrame!(int, 2) df;
    df.from_csv("./test/tocsv/ex1p2.csv", 3, 0);
    // df.display();
    df.to_csv("./test/tocsv/ex2p2.csv", true, false);

    import std.stdio: File;
    File f1 = File("./test/tocsv/ex1p2.csv", "r");
    File f2 = File("./test/tocsv/ex2p2.csv", "r");

    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// Parsing CSV without row indexes
unittest
{
    DataFrame!(int, 2) df;
    df.from_csv("./test/tocsv/ex1p3.csv", 0, 3);
    // df.display();
    df.to_csv("./test/tocsv/ex2p3.csv", false, true);

    import std.stdio: File;
    File f1 = File("./test/tocsv/ex1p3.csv", "r");
    File f2 = File("./test/tocsv/ex2p3.csv", "r");

    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// Parsing CSV without any indexes
unittest
{
    DataFrame!(int, 2) df;
    df.from_csv("./test/tocsv/ex1p4.csv", 0, 0);
    // df.display();
    df.to_csv("./test/tocsv/ex2p4.csv", false, false);

    import std.stdio: File;
    File f1 = File("./test/tocsv/ex1p4.csv", "r");
    File f2 = File("./test/tocsv/ex2p4.csv", "r");

    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// Different Types
unittest
{
    DataFrame!(int, long) df;
    df.from_csv("./test/tocsv/ex1p4.csv", 0, 0);
    static assert(is(typeof(df.data[1]) == long[]));
    // df.display();
    df.to_csv("./test/tocsv/ex2p5.csv", false, false);

    import std.stdio: File;
    File f1 = File("./test/tocsv/ex1p4.csv", "r");
    File f2 = File("./test/tocsv/ex2p5.csv", "r");

    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// Partial Parsing of data
unittest
{
    DataFrame!(int) df;
    df.from_csv("./test/tocsv/ex1p4.csv", 0, 0, [0]);
    // df.display();
    assert(df.data[0] == [1,2]);
}

// Partial parsing - second column
unittest
{
    DataFrame!(int) df;
    df.from_csv("./test/tocsv/ex2p6.csv", 0, 0, [1]);
    // df.display();
    assert(df.data[0] == [1,24]);
}

// Parsing by mentioning all the columns
unittest
{
    DataFrame!(int, int) df;
    df.from_csv("./test/tocsv/ex2p6.csv", 0, 0, [0, 1]);
    // df.display();
    df.to_csv("./test/tocsv/ex2p7.csv", false, false);

    import std.stdio: File;
    File f1 = File("./test/tocsv/ex2p6.csv", "r");
    File f2 = File("./test/tocsv/ex2p7.csv", "r");

    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// Partial Parsing with column headers
unittest
{
    DataFrame!(int) df;
    df.from_csv("./test/tocsv/ex1p1.csv", 3, 3, [0]);
    assert(df.data[0] == [1,2]);
    df.to_csv("./test/tocsv/ex2p8.csv");
    string[] data = [
        ",,Hey,Hello",
        ",,Hey,1",
        ",,Hey,Hello",
        "Index1,Index2,Index3",
        "Hello,Hello,1,1",
        "Hi,Hello,24,2",
        ""
    ];

    import std.stdio: File;
    import std.string: chomp;
    int line = 0;
    File csv = File("./test/tocsv/ex2p8.csv");
    while(!csv.eof())
    {
        assert(chomp(csv.readln()) == data[line]);
        ++line;
    }
    csv.close();
}

// Partial Parsing with column headers - second column
unittest
{
    DataFrame!(int) df;
    df.from_csv("./test/tocsv/ex1p1.csv", 3, 3, [1]);
    assert(df.data[0] == [1,2]);
    df.to_csv("./test/tocsv/ex2p9.csv");
    string[] data = [
        ",,Hey,Hi",
        ",,Hey,2",
        ",,Hey,Hello",
        "Index1,Index2,Index3",
        "Hello,Hello,1,1",
        "Hi,Hello,24,2",
        ""
    ];

    import std.stdio: File;
    import std.string: chomp;
    int line = 0;
    File csv = File("./test/tocsv/ex2p9.csv");
    while(!csv.eof())
    {
        assert(chomp(csv.readln()) == data[line]);
        ++line;
    }
    csv.close();
}

// PArsing of dataset 1
unittest
{
    DataFrame!(int, 9, double, int, 4) df;
    df.from_csv("./test/fromcsv/dataset1.csv", 0, 1);
    // df.display();
    df.to_csv("./test/tocsv/ex3p1.csv", false);

    import std.stdio: File;
    File f1 = File("./test/fromcsv/dataset1.csv", "r");
    File f2 = File("./test/tocsv/ex3p1.csv", "r");

    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// Parsing dataset 1 considering first column as index
unittest
{
    DataFrame!(int, 8, double, int, 4) df;
    df.from_csv("./test/fromcsv/dataset1.csv", 1, 1);
    // df.display();
    df.to_csv("./test/tocsv/ex3p2.csv");

    import std.stdio: File;
    File f1 = File("./test/fromcsv/dataset1.csv", "r");
    File f2 = File("./test/tocsv/ex3p2.csv", "r");

    while(!f1.eof())
    {
        assert(f1.readln() == f2.readln());
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// Parsing dataset 2 with gaps in data
unittest
{
    DataFrame!(double, 23) df;
    df.from_csv("./test/fromcsv/dataset2.csv", 2, 1);
    //df.display();
    df.to_csv("./test/tocsv/ex4p1.csv");

    import std.stdio: File;
    import std.string: chomp;
    import std.array: split;

    File f1 = File("./test/fromcsv/dataset2.csv", "r");
    File f2 = File("./test/tocsv/ex4p1.csv", "r");

    while(!f1.eof())
    {
        auto lf1 = chomp(f1.readln()).split(",");
        auto lf2 = chomp(f2.readln()).split(",");
        if(lf1.length > 0)
            assert(lf1[0 .. 3] == lf2[0 .. 3]);
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// PArtially parsing the complete columns of dataset2
unittest
{
    DataFrame!(int, 1) df;
    df.from_csv("./test/fromcsv/dataset2.csv", 2, 1, [0]);
    // df.display();
    df.to_csv("./test/tocsv/ex4p2.csv");

    import std.stdio: File;
    import std.string: chomp;
    import std.array: split;

    File f1 = File("./test/fromcsv/dataset2.csv", "r");
    File f2 = File("./test/tocsv/ex4p2.csv", "r");

    while(!f1.eof())
    {
        auto lf1 = chomp(f1.readln()).split(",");
        auto lf2 = chomp(f2.readln()).split(",");
        if(lf1.length > 0)
            assert(lf1[0 .. 3] == lf2);
    }
    assert(f1.eof() && f2.eof());

    f1.close();
    f2.close();
}

// Unittest for example in README.md
unittest
{
    DataFrame!(int, 2, double, 1) df;
    Index index;
    index.setIndex([0,1,2,3,4,5], ["Row Index"], [0,1,2], ["Column Index"]);
    df.setFrameIndex(index);
    // df.display();

    df.assign!1(2, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    // df.display();

    df.assign!1(1, [1, 2, 3]);
    // df.display();

    df.assign!0(0, 4, 5, 1.6);
    // df.display();

    index.extend!0([6]);
    df.setFrameIndex(index);
    // df.display();
}

unittest
{
    Index inx;
    DataFrame!(int, 2) df;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["RL1", "RL2"],
                [["Hello", "Hi"], ["Hi", "Hello"]], ["CL1", "CL2"]);
    df.setFrameIndex(inx);
    // df.display();

    DataFrame!(int, 3) df2;
    inx.extend!0(["Hey", "Hey"]);
    inx.extend!1(["Yo", "Yo"]);
    df2.setFrameIndex(inx);
    // df2.display();
}

unittest
{
    Index inx;
    inx.setIndex([1, 2, 3],["rindex"]);

    DataFrame!(int, 2, double) df;
    df.setFrameIndex(inx);
    // df.display();
    df = [[1.0], [1.0, 2.0], [1.0, 2.0, 3.5]];
    df[0, 0] = 42;
    df[["2"], ["1"]] = 17;
    // df.display();
}