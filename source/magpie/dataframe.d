module magpie.dataframe;

import magpie.axis: Axis, DataType;
import magpie.index: Index;
import magpie.helper: getArgsList, toArr, isHomogeneous, suitableType, auxDispatch;

import std.meta: AliasSeq, Repeat, staticMap;
import std.array: appender;
import std.traits: isType, isBoolean, isArray;

import mir.ndslice;

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

    /// Flag to know if the DataFrame is homogeneous
    enum bool isHomogeneousType = isHomogeneous!(RowType);

    static if(!isHomogeneousType)
        alias FrameType = staticMap!(toArr, RowType);
    else
        alias FrameType = toArr!(RowType[0])[RowType.length];

    ///
    size_t rows = 0;
    ///
    size_t cols = RowType.length;

    /// DataFrame indexing
    Index indx;
    /// DataFrame Data
    FrameType data;

private:
    ptrdiff_t getPosition(int axis)(string[] index)
    {
        return indx.getPosition!(axis)(index);
    }

    template resolverInternal(T, Ops...)
    {
        import std.traits: ReturnType;
        static if(Ops.length)
        {
            alias Op = Ops[0];
            static if(__traits(compiles, ReturnType!(Op)))
                alias resolverInternal = AliasSeq!(ReturnType!(Op), resolverInternal!(T, Ops[1 .. $]));
            else static if(__traits(compiles, ReturnType!(Op!T)))
                alias resolverInternal = AliasSeq!(ReturnType!(Op!T), resolverInternal!(T, Ops[1 .. $]));
            else static if(__traits(compiles, ReturnType!(Op!(toArr!T))))
                alias resolverInternal = AliasSeq!(ReturnType!(Op!(toArr!T)), resolverInternal!(T, Ops[1 .. $]));
            else
                alias resolverInternal = AliasSeq!(ReturnType!(Op!(T,T)), resolverInternal!(T, Ops[1 .. $]));
        }
        else 
            alias resolverInternal = AliasSeq!();
    }

    template aggregateType(Ops...)
    {
        alias fwdType = suitableType!(RowType);
        alias Resolved = resolverInternal!(fwdType, Ops);
        alias aggregateType = Resolved;
    }

    auto dropperRuntimeInternal(int[] positions)
    {
        import magpie.helper: dropper;
        import std.algorithm: max, min, reduce;
        assert(positions.reduce!min > -1 && positions.reduce!max < rows, "Index out of bound");

        DataFrame!(true, RowType) ret;
        ret.indx = Index();
        ret.indx.indexing[1] = indx.indexing[1];
        ret.indx.row.titles = indx.row.titles;

        import std.range: lockstep;
        ret.indx.row.index.length = indx.row.index.length;
        ret.indx.row.codes.length = indx.row.codes.length;
        foreach(i, a, b; lockstep(indx.row.index, indx.row.codes))
        {
            ret.indx.row.index[i] = a;
            ret.indx.row.codes[i] = dropper(positions, b);
        }

        static foreach(i; 0 .. RowType.length)
            ret.data[i] = dropper(positions, data[i]);

        ret.rows = rows - positions.length;
        return ret;
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
        import std.algorithm: map, reduce, max;
        import std.conv: to;

        auto gapCalc(T)(T[] arr)
        {
            static if(is(T == string))
                return arr.map!(e => e.length).reduce!max;
            else
                return arr.map!(e => to!string(e).length).reduce!max;
        }

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
        const size_t totalHeight = rows + indx.column.index.length +
            ((indx.row.titles.length > 0 && indx.column.titles.length > 0)? 1: 0);
        const size_t totalWidth = cols + indx.row.index.length;

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

        size_t dataIndex = 0;
        foreach(i; 0 .. totalWidth)
        {
            int extra = (indx.row.titles.length > 0 && indx.column.titles.length > 0)? 1: 0;
            if(i < indx.row.index.length)
            {
                size_t thisGap = (i < indx.row.titles.length && top > indx.column.index.length + extra)? indx.row.titles[i].length: 0;
                if(top > indx.column.index.length + extra)
                {
                    size_t tmp = 0;
                    if(indx.row.codes[i].length == 0)
                        tmp = gapCalc(indx.row.index[i][0 .. top - indx.column.index.length - extra]);
                    else if(indx.row.index[i].length == 0)
                        tmp = gapCalc(indx.row.codes[i][0 .. top - indx.column.index.length - extra]);
                    else
                        tmp = indx.row.codes[i][0 .. top - indx.column.index.length - extra].map!(e => indx.row.index[i][e].length).reduce!max;

                    thisGap = max(thisGap, tmp);
                }

                if(bottom > 0)
                {
                    if(bottom > indx.row.index[i].length)
                    {
                        size_t tmp = 0;
                        if(indx.row.codes[i].length == 0)
                            tmp = gapCalc(indx.row.index[i]);
                        else if(indx.row.index[i].length == 0)
                            tmp = gapCalc(indx.row.codes[i]);
                        else
                            tmp = indx.row.codes[i].map!(e => indx.row.index[i][e].length).reduce!max;

                        thisGap = max(thisGap, tmp);
                    }
                    else
                    {
                        size_t tmp = 0;
                        if(indx.row.codes[i].length == 0)
                            tmp = gapCalc(indx.row.index[i][$ - bottom .. $]);
                        else if(indx.row.index[i].length == 0)
                            tmp = gapCalc(indx.row.codes[i][$ - bottom .. $]);
                        else
                            tmp = indx.row.codes[i][$ - bottom .. $].map!(e => indx.row.index[i][e].length).reduce!max;

                        thisGap = max(thisGap, tmp);
                    }
                }

                if(i == indx.row.index.length - 1 && indx.column.titles.length > 0)
                {
                    const auto tmp = (indx.column.titles.length > top)
                        ? gapCalc(indx.column.titles[0 .. top])
                        : gapCalc(indx.column.titles);

                    thisGap = max(thisGap, tmp);
                }

                gaps.put((thisGap < maxColSize)? thisGap: maxColSize);
            }
            else
            {
                size_t maxGap = 0;
                foreach(j; 0 .. (top > indx.column.index.length)? indx.column.index.length: top)
                {
                    size_t lenCol = 0;
                    if(indx.column.codes[j].length == 0)
                        lenCol = indx.column.index[j][dataIndex].length;
                    else if(indx.column.index[j].length == 0)
                        lenCol = to!string(indx.column.codes[j][dataIndex]).length;
                    else
                        lenCol = indx.column.index[j][indx.column.codes[j][dataIndex]].length;

                    maxGap = max(maxGap, lenCol);
                }

                foreach(j; totalHeight - bottom .. indx.column.index.length)
                {
                    size_t lenCol = 0;
                    if(indx.column.codes[j].length == 0)
                        lenCol = indx.column.index[j][dataIndex].length;
                    else if(indx.column.index[j].length == 0)
                        lenCol = to!string(indx.column.codes[j][dataIndex]).length;
                    else
                        lenCol = indx.column.index[j][indx.column.codes[j][dataIndex]].length;

                    maxGap = max(maxGap, lenCol);
                }

                size_t maxsize1 = 0, maxsize2 = 0;

                void maxGapCalc(ptrdiff_t si = -1)(size_t ri = 0) @property
                {
                    static if(si > -1)
                        alias j = si;
                    else
                        size_t j = ri;

                    if(top > indx.column.index.length + extra)
                        maxsize1 = gapCalc(data[j][0 .. top - indx.column.index.length - extra]);
                    if(bottom > data[j].length)
                        maxsize2 = gapCalc(data[j]);
                    else if(bottom > 0)
                        maxsize2 = gapCalc(data[j][$ - bottom .. $]);
                }

                mixin auxDispatch!(maxGapCalc, isHomogeneousType, RowType);
                auxDispatch(dataIndex);

                maxGap = max(maxGap, maxsize1, maxsize2);
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
            const int extra = (indx.row.titles.length > 0 && indx.column.titles.length > 0)? 1: 0;
            if(ele[0] < indx.column.index.length + extra)
            {
                dataIndex = 0;
            }
            else
            {
                dataIndex = ele[0] - (indx.column.index.length + extra);
            }
            foreach(i; ele[0] .. ele[1])
            {
                bool skipIndex = true;
                foreach(lim; [[0, left], [totalWidth - right, totalWidth]])
                {
                    foreach(j; lim[0] .. lim[1])
                    {
                        if(i < indx.column.index.length)
                        {
                            if(j < indx.row.index.length)
                            {
                                if(j == indx.row.index.length - 1
                                    && indx.column.titles.length != 0)
                                {
                                    if(indx.column.titles[i].length > maxColSize)
                                    {
                                        dispstr.put(indx.column.titles[i][0 .. maxColSize]);
                                        dispstr.put("  ");
                                    }
                                    else
                                    {
                                        dispstr.put(indx.column.titles[i]);
                                        foreach(k;indx.column.titles[i].length .. cwidth[j] + 2)
                                        {
                                            dispstr.put(" ");
                                        }
                                    }
                                }
                                else if(i == indx.column.index.length - 1
                                    && indx.column.titles.length == 0)
                                {
                                    if(indx.row.titles[j].length > maxColSize)
                                    {
                                        dispstr.put(indx.row.titles[j][0 .. maxColSize]);
                                        dispstr.put("  ");
                                    }
                                    else
                                    {
                                        dispstr.put(indx.row.titles[j]);
                                        foreach(k; indx.row.titles[j].length .. cwidth[j] + 2)
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
                                if(indx.column.codes[i].length == 0)
                                    colindx = indx.column.index[i][j - indx.row.index.length];
                                else if(indx.column.index[i].length == 0)
                                    colindx = to!string(indx.column.codes[i][j - indx.row.index.length]);
                                else
                                    colindx = indx.column.index[i][indx.column.codes[i][j - indx.row.index.length]];

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
                        else if(i == indx.column.index.length && indx.column.titles.length != 0)
                        {
                            if(j < indx.row.titles.length)
                            {
                                if(indx.row.titles[j].length > maxColSize)
                                {
                                    dispstr.put(indx.row.titles[j][0 .. maxColSize]);
                                    dispstr.put("  ");
                                }
                                else
                                {
                                    dispstr.put(indx.row.titles[j]);
                                    foreach(k; indx.row.titles[j].length .. cwidth[j] + 2)
                                    {
                                        dispstr.put(" ");
                                    }
                                }
                            }
                        }
                        else
                        {
                            if(j < indx.row.index.length)
                            {
                                string idx = "";
                                if(indx.row.codes[j].length == 0)
                                    idx = indx.row.index[j][dataIndex];
                                else if(indx.row.index[j].length == 0)
                                    idx = to!string(indx.row.codes[j][dataIndex]);
                                else if(dataIndex > 0 && j < indx.row.index.length
                                    && indx.row.codes[j][dataIndex] == indx.row.codes[j][dataIndex - 1]
                                    && skipIndex && indx.isMultiIndexed)
                                    idx = "";
                                else
                                {
                                    idx = indx.row.index[j][indx.row.codes[j][dataIndex]];
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

                                string toStringAux(ptrdiff_t si = -1)(size_t ri = 0) @property
                                {
                                    static if(si > -1)
                                        alias i = si;
                                    else
                                        size_t i = ri;

                                    return to!string(data[i][dataIndex]);
                                }

                                mixin auxDispatch!(toStringAux, isHomogeneousType, RowType);
                                idx = auxDispatch(j - indx.row.index.length);
                                
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
                if(i >= indx.column.index.length + extra)
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
        import std.array: replace;
        if(!getStr)
        {
            writeln(dispstr.data.replace("  \n", "\n"));
            // writeln(RowType.length);
            // writeln(gaps.data);
            // writeln(left,"\t", right);

            writeln("Dataframe Dimension: [ ", totalHeight," X ", totalWidth, " ]");
            writeln("Data Dimension: [ ", rows," X ", cols, " ]");
        }
        return ((getStr)? dispstr.data.replace("  \n", "\n"): "");
    }

    /++
    void to_csv(string path, bool writeIndex = true, bool writecolumn.index = true, char sep = ',')
    Description: Writes given DataFrame to a CSV file
    @params: path - path to the output file
    @params: writeIndex - write row index to the file
    @params: writecolumn.index - write column index to the file
    @params: sep - data seperator
    +/
    void to_csv(int precision = 0)(string path, bool writeIndex = true, bool writeColumn = true, char sep = ',')
    {
        import std.array: appender;
        import std.conv: to;
        import std.format: format;

        string formatData(T)(T ele)
        {
            static if(__traits(isIntegral, T))
                return format!"%d"(ele);
            else static if(__traits(isFloating, T) && precision)
                return format!("%." ~ to!string(precision) ~"f")(ele);
            else
                return to!string(ele);
        }

        auto formatter = appender!(string);
        const size_t totalHeight = rows + indx.column.index.length +
            ((indx.row.titles.length > 0 && indx.column.titles.length > 0)? 1: 0);

        if(rows == 0)
        {
            return;
        }

        if(writeColumn)
        {
            foreach(i; 0 .. indx.column.index.length)
            {
                if(writeIndex)
                {
                    foreach(j; 0 .. indx.row.index.length)
                    {
                        if(i != indx.column.index.length - 1 && j < indx.row.index.length - 1)
                        {
                            formatter.put(sep);
                        }
                        else if(i == indx.column.index.length - 1 && indx.column.titles.length == 0)
                        {
                            formatter.put(indx.row.titles[j]);
                            formatter.put(sep);
                        }
                        else if(j == indx.row.index.length - 1 && indx.column.titles.length != 0)
                        {
                            formatter.put(indx.column.titles[i]);
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
                    if(indx.column.codes[i].length == 0)
                        colindx = indx.column.index[i][j];
                    else if(indx.column.index[i].length == 0)
                        colindx = to!string(indx.column.codes[i][j]);
                    else
                        colindx = indx.column.index[i][indx.column.codes[i][j]];

                    formatter.put(colindx);
                    if(j < cols - 1)
                        formatter.put(sep);
                }

                formatter.put("\n");
            }
            if(indx.column.titles.length != 0 && writeIndex)
            {
                formatter.put(indx.row.titles[0]);
                foreach(j; 1 .. indx.row.index.length)
                {
                    formatter.put(sep);
                    formatter.put(indx.row.titles[j]);
                }
                formatter.put("\n");
            }
        }

        foreach(i; 0 .. rows)
        {
            if(writeIndex)
            {
                bool skipIndex = true;
                foreach(j; 0 .. indx.row.index.length)
                {
                    string idx = "";
                    if(indx.row.codes[j].length == 0)
                        idx = indx.row.index[j][i];
                    else if(indx.row.index[j].length == 0)
                        idx = to!string(indx.row.codes[j][i]);
                    else if(i > 0 && j < indx.row.index.length
                        && indx.row.codes[j][i] == indx.row.codes[j][i - 1]
                        && skipIndex && indx.isMultiIndexed)
                        idx = "";
                    else
                    {
                        idx = indx.row.index[j][indx.row.codes[j][i]];
                        skipIndex = false;
                    }

                    formatter.put(idx);
                    formatter.put(sep);
                }
            }
            formatter.put(formatData(data[0][i]));
            static foreach(j; 1 .. RowType.length)
            {
                formatter.put(sep);
                formatter.put(formatData(data[j][i]));
            }

            formatter.put("\n");
        }

        import std.stdio: File;
        File outfile = File(path, "w");
        outfile.write(formatter.data);
        outfile.close();
    }

    /++
    void from_csv(string path, size_t indexDepth, size_t columnDepth, size_t[] column.index = [], char sep = ',')
    Description: Parsing of DataFrame from a CSV file
    @params: path - File path of csv file
    @params: indexDepth - Number of column row index span
    @params: columnDepth - Number of rows column index span
    @params: column.index - Integer row.index of column to selectively parse
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

        assert(columns.length == cols, "The dimension of columns must be same as dimension of the DataFrame");

        File csvfile = File(path, "r");
        bool bothTitle = false;
        size_t line = 0;
        indx = Index();

        indx.row.codes.length = indexDepth;
        indx.row.index.length = indexDepth;

        size_t dataIndex = 0;
        while(!csvfile.eof())
        {
            string[] fields = chomp(csvfile.readln()).split(sep);

            if(line < columnDepth)
            {
                if(indexDepth > 0 && line == columnDepth - 1 && fields[0].length > 0)
                {
                    indx.row.titles = fields[0 .. indexDepth];
                }
                else if(indexDepth > 0 && fields[indexDepth - 1].length > 0)
                {
                    indx.column.titles ~= fields[indexDepth - 1];
                    bothTitle = true;
                }

                indx.column.index ~= [[]];
                indx.column.codes ~= [[]];
                foreach(i; 0 .. cols)
                {
                    size_t pos = columns[i];
                    string colindx = fields[indexDepth + pos];

                    if(i > 0 && colindx.length == 0)
                    {
                        indx.column.codes[line] ~= indx.column.codes[line][$ - 1];
                    }
                    else
                    {
                        import std.algorithm: countUntil;
                        int idxpos = cast(int)countUntil(indx.column.index[line], colindx);

                        if(idxpos > -1)
                        {
                            indx.column.codes[line] ~= cast(int)idxpos;
                        }
                        else
                        {
                            indx.column.index[line] ~= colindx;
                            indx.column.codes[line] ~= cast(uint)indx.column.index[line].length - 1;
                        }
                    }
                }
            }
            else if(line == columnDepth && bothTitle)
            {
                indx.row.titles = fields[0 .. indexDepth];
            }
            else
            {
                if(indexDepth == 1 && columnDepth == 1 && line == columnDepth && fields.length == 1)
                {
                    bothTitle = true;
                    indx.row.titles = fields;
                }
                else if(fields.length > 0)
                {
                    foreach(i; 0 .. indexDepth)
                    {
                        import std.algorithm: countUntil;
                        int indxpos = cast(int)countUntil(indx.row.index[i], fields[i]);
                        if(fields[i].length == 0 && dataIndex > 0)
                        {
                            indx.row.codes[i] ~= indx.row.codes[$ - 1];
                        }
                        else if(indxpos > -1)
                        {
                            indx.row.codes[i] ~= cast(uint)indxpos;
                        }
                        else
                        {
                            indx.row.index[i] ~= fields[i];
                            indx.row.codes[i] ~= cast(uint)indx.row.index[i].length - 1;
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
            indx.row.titles ~= "Index";
            indx.row.index = [[]];
            indx.row.codes = [[]];
            foreach(i; 0 .. rows)
                indx.row.codes[0] ~= cast(uint)i;
        }

        if(columnDepth == 0)
        {
            indx.column.index = [[]];
            indx.column.codes = [[]];
            foreach(i; 0 .. indexDepth)
                indx.row.titles ~= ["Index"];
            foreach(i; 0 .. line)
                indx.column.codes[0] ~= cast(uint)i;
        }

        indx.optimize();
    }

    /++
    from_csv rebuild for faster read
    +/
    void fastCSV(string path, size_t indexDepth, size_t columnDepth, char sep = ',')
    {
        import std.array: array, split;
        import std.algorithm: map;
        import std.stdio: File;
        import std.string: chomp;

        File csvfile = File(path, "r");
        string[][] lines = csvfile.byLineCopy().map!(chomp).map!(e => e.split(",")).array();
        int totalLines = cast(int)lines.length;
        csvfile.close();

        indx.row.titles.length = indexDepth;
        indx.row.index.length = indexDepth;
        indx.column.index.length = columnDepth;
        indx.row.codes.length = indexDepth;
        indx.column.codes.length = columnDepth;
        indx.row.titles = lines[columnDepth - 1][0 .. indexDepth];

        foreach(i, ele; lines[0 .. columnDepth])
            indx.column.index[i] = ele[indexDepth .. $];

        foreach(i, ele; lines[columnDepth .. $])
        {
            if(ele.length == 0)
                continue;

            foreach(j; 0 .. indexDepth)
            {
                ++indx.row.index[j].length;
                indx.row.index[j][i] = ele[j];
            }

            static foreach(j; 0 .. RowType.length)
            {
                import std.conv: to;
                ++data[j].length;
                if(ele[indexDepth + j].length == 0)
                    data[j][i] =  RowType[j].init;
                else
                    data[j][i] = to!(RowType[j])(ele[indexDepth + j]);
            }
        }

        rows = totalLines - columnDepth;
        if(indx.row.index.length == 0)
        {
            indx.row.index.length = 1;
            indx.row.codes.length = 1;
            indx.row.codes[0].length = rows;
            foreach(i; 0 .. rows)
                indx.row.codes[0][i] = cast(int)i;

            indx.row.titles = ["Index"];
        }

        if(indx.column.index.length == 0)
        {
            indx.column.index.length = 1;
            indx.column.codes.length = 1;
            indx.column.codes[0].length = rows;
            foreach(i; 0 .. rows)
                indx.column.codes[0][i] = cast(int)i;

            indx.row.titles.length = indx.row.codes.length;
            foreach(i, ref ele;indx.row.titles)
            {
                import std.conv: to;
                ele = "Index" ~ to!(string)(i + 1);
            }
        }

        indx.generateCodes();
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

    RowType[i2] at(size_t i2)(size_t i1) @property
        if(i2 < RowType.length)
    {
        return data[i2][i1];
    }

    auto at(size_t i1, size_t i2) @property
    {
        assert(i2 < cols && i1 < rows, "Index out of bound");

        auto returnAux(ptrdiff_t si = -1)(size_t ri = 0) @property
        {
            static if(si > -1)
                alias i = si;
            else
                size_t i = ri;

            return data[i][i1];
        }
        
        mixin auxDispatch!(returnAux, isHomogeneousType, RowType);
        return auxDispatch(i2);
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

        void assignAux(ptrdiff_t si = -1)(size_t ri = 0) @property
        {
            static if(si > -1)
                alias i = si;
            else
                size_t i = ri;

            data[i][i1] = ele[0];
        }

        mixin auxDispatch!(assignAux, isHomogeneousType, RowType);
        auxDispatch(i2);
    }

    /++
    void opIndexAssign(Args...)(Args ele, string[] rindx, string[] cindx)
    Description: Setting the element at an index
    @param: rindx - Row index
    @param: cindx - Column index
    +/
    void opIndexAssign(Ele)(Ele ele, string[] rindx, string[] cindx)
    {
        assert(rindx.length == indx.row.codes.length, "Size of row index don't match the index depth");
        assert(cindx.length == indx.column.codes.length, "Size of column index don't match the index depth");

        ptrdiff_t i1 = getPosition!0(rindx);
        ptrdiff_t i2 = getPosition!1(cindx);

        assert(i1 > -1 && i2 > -1, "Given headers don't match DataFrame Headers");

        void assignAux(ptrdiff_t si = -1)(size_t ri = 0) @property
        {
            static if(si > -1)
                alias i = si;
            else
                size_t i = ri;

            data[i][i1] = ele;
        }

        mixin auxDispatch!(assignAux, isHomogeneousType, RowType);
        auxDispatch(i2);
    }

    /++
    int getRowPos(string[] row.index)
    Description: Get integer index of the given row headers
    Defaults to -1 if headers don't match
    @params: row.index - Array of row.index
    +/
    ptrdiff_t getRowPosition(string[] indexes)
    {
        assert(indexes.length == indx.row.codes.length, "Size of row index don't match the indexing depth");
        return getPosition!0(indexes);
    }

    /++
    int getColPos(string[] row.index)
    Description: Get integer index of the given column headers
    Defaults to -1 if headers don't match
    @params: row.index - Array of row.index
    +/
    ptrdiff_t getColumnPosition(string[] indexes)
    {
        assert(indexes.length == indx.column.codes.length, "Size of column index doesn't match indexing depth");
        return getPosition!1(indexes);
    }

    /++
    void setFrameIndex(Index index)
    Description: Sets the frame row.index.
    [Please use setIndex method of Index to set the index]
    @params: index - Index structure to replace indexing
    +/
    void setFrameIndex(Index index)
    {
        assert(index.row.codes.length > 0 || index.column.codes.length > 0
            || index.row.titles.length > 0 || index.column.titles.length > 0,
            "Cannot set empty index to DataFrame");

        bool needsPadding = false;

        foreach(i; 0 .. 2)
        {
            if(index.indexing[i].codes.length > 0 && index.indexing[i].codes[0].length >= ((i)? cols: rows))
            {
                indx.indexing[i].codes = index.indexing[i].codes;
                indx.indexing[i].index = index.indexing[i].index;
                indx.indexing[i].titles = index.indexing[i].titles;
            }

            if(index.indexing[i].codes.length == 0 && index.indexing[i].titles.length == indx.indexing[i].codes.length)
            {
                indx.indexing[i].titles = index.indexing[i].titles;
            }

            if(i == 0 && index.row.codes[0].length != rows)
            {
                needsPadding = true;
                rows = index.row.codes[0].length;
            }
        }

        if(indx.column.codes.length == 0)
        {
            indx.column.index = [[]];
            indx.column.codes = [[]];
            indx.column.codes[0].length = cols;
            foreach(i; 0 .. cols)
                indx.column.codes[0][i] = cast(int)i;
        }

        if(needsPadding)
        {
            static foreach(i; 0 .. RowType.length)
            {
                data[i].length += rows - data[i].length;
                foreach(j; data[i].length .. rows)
                    data[i][j] = RowType[i].init;
            }
        }
    }

    /++
    Direct assignment of a 2D array to DataFrame
    Usage: df = [[1,2], [3,4]];
    +/
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

    /// Assign Slice to DataFrame
    void opAssign(T, SliceKind k)(Slice!(T*, 2, k) input)
    {
        assert(input.shape[0] <= rows && input.shape[1] <= cols, "Given Slice is larger than DataFrame");
        static foreach(i; 0 .. RowType.length)
            if(i < input.shape[1])
            {

                foreach(j; 0 .. input.shape[0])
                    static if(__traits(isArithmetic, T, RowType[i]))
                        data[i][j] = cast(RowType[i])input[j][i];
                    else
                    {
                        import std.conv: to;
                        data[i][j] = to!(RowType[i])(input[j][i]);
                    }
            }
            
    }

    /++
    void assign(int axis, T, U...)(T index, U values)
    Description: Assign values to rows or column.index
    @params: axis - 0 for rows, 1 for column.index
    @params: index - string[] or integer index of the location to assign
    @params: values - values to assign
    +/
    void assign(int axis, T, U...)(T index, U values)
        if(U.length > 0)
    {

        ptrdiff_t pos;
        static if(is(T == int))
            pos = index;
        else
            pos = getPosition!(axis)(index);

        assert(pos > -1 && pos < (axis)? rows: cols, "Index out of bound");

        static if(axis == 0)
        {
            static foreach(i; 0 .. (RowType.length > U.length)? U.length: RowType.length)
                data[i][pos] = values[i];
        }
        else
        {
            void assignAux(ptrdiff_t si = -1)(size_t ri = 0) @property
            {
                static if(si > -1)
                {
                    alias i = si;
                    enum size_t typepos = i;
                }
                else
                {
                    size_t i = ri;
                    enum size_t typepos = 0;
                }

                static if(is(toArr!(RowType[typepos]) == U[0]))
                {
                    foreach(j; 0 .. (rows > values[0].length)? values[0].length: rows)
                        data[i][j] = values[0][j];
                }
            }
            
            mixin auxDispatch!(assignAux, isHomogeneousType, RowType);
            auxDispatch(pos);
        }
    }

    /++
    Element access based on integral index
    Usage: df[0, 0]
    +/
    auto opIndex(size_t i1, size_t i2)
    {
        assert(i1 <= rows, "Row index out of bound");
        assert(i2 <= cols, "Column index out of bound");

        auto returnAux(ptrdiff_t si = -1)(size_t ri = 0) @property
        {
            static if(si > -1)
                alias i = si;
            else
                size_t i = ri;

            return data[i][i1];
        }

        mixin auxDispatch!(returnAux, isHomogeneousType, RowType);
        return auxDispatch(i2);
    }

    /++
    Element access based on integral index
    Usage: df[["Index1"], ["Index2"]]
    +/
    auto opIndex(T, U)(T rindx, U cindx)
        if((is(T == string) || is(T == string[]))
        && (is(U == string) || is(U == string[])))
    {
        ptrdiff_t i1 = -1;// getPosition!0(rindx);
        ptrdiff_t i2 = -1;//getPosition!1(cindx);

        static if(is(T == string))
            i1 = getPosition!0([rindx]);
        else
            i1 = getPosition!0(rindx);

        static if(is(U == string))
            i2 = getPosition!1([cindx]);
        else
            i2 = getPosition!1(cindx);

        assert(i1 > -1 && i2 > -1, "Given headers don't match DataFrame Headers");
        
        auto returnAux(ptrdiff_t si = -1)(size_t ri = 0) @property
        {
            static if(si > -1)
                alias i = si;
            else
                size_t i = ri;

            return data[i][i1];
        }

        mixin auxDispatch!(returnAux, isHomogeneousType, RowType);
        return auxDispatch(i2);
    }

    /++
    Get an entire row or column using index
    For column: df[["Index"]]
    For row: df[["Index"], 0]
    +/
    auto opIndex(Args...)(string[] index, Args args)
        if(Args.length == 0 || (Args.length == 1 && is(Args[0] == int)))
    {
        const int axis = 1 - Args.length;
        ptrdiff_t pos = -1;
        if(axis == 0)
            pos = getPosition!0(index);
        else
            pos = getPosition!1(index);

        assert(pos > -1, "Index not found");

        static if(axis == 1)
        {
            import magpie.axis: Axis, DataType;
            Axis!void retcol;

            void axisAssignAux(ptrdiff_t si = -1)(size_t ri = 0) @property
            {
                static if(si > -1)
                    alias i = si;
                else
                    size_t i = ri;
                    
                retcol.data.length = data[i].length;
                foreach(j, ele; data[i])
                    retcol.data[j] = DataType(ele);
            }

            mixin auxDispatch!(axisAssignAux, isHomogeneousType, RowType);
            auxDispatch(pos);
            return retcol;
        }
        else
        {
            import magpie.axis: Axis;
            Axis!(RowType) retrow;
            static foreach(i; 0 .. RowType.length)
                retrow.data[i] = data[i][pos];

            return retrow;
        }

        assert(0);
    }

    /++
    Column/Row binary operations
    df[["CIndex1"]] = df[["CIndex2"]] + df[["CIndex3"]];
    df[["RIndex1"], 0] = df[["RIndex2"], 0] + df[["RIndex3"], 0];
    +/
    void opIndexAssign(T...)(Axis!T elements, string[] index, int axis = 1)
        if(T.length == RowType.length || T.length == 1)
    {
        opIndexOpAssign!("")(elements, index, axis);
    }

    /// Short Hand operations
    void opIndexOpAssign(string op, T...)(Axis!T elements, string[] index, int axis = 1)
        if(T.length == RowType.length || T.length == 1)
    {
        import std.traits: isArray;
        static if(is(T[0] == void) || (T.length == 1 && isArray!(T[0])))
        {
            assert(elements.data.length == rows, "Length of Axis.data is not equal to number of rows");
            ptrdiff_t pos = getPosition!1(index);
            assert(pos > -1, "Index not found");

            void mixinAux(ptrdiff_t si = -1)(size_t ri = 0) @property
            {
                static if(si > -1)
                {
                    alias i = si;
                    enum size_t typepos = si;
                }
                else
                {
                    size_t i = ri;
                    enum size_t typepos = 0;
                }

                static if(is(T[0] == void))
                    enum string append = ".get!(RowType[typepos]);";
                else
                    enum string append = ";";

                foreach(j; 0 .. elements.data.length)
                    mixin("data[i][j] " ~ op ~"= elements.data[j]" ~ append);
            }
            
            mixin auxDispatch!(mixinAux, isHomogeneousType, RowType);
            auxDispatch(pos);
        }
        else
        {
            assert(elements.data.length == RowType.length, "Length of Axis.data is less than the number of columns");
            ptrdiff_t pos = getPosition!0(index);
            assert(pos > -1, "Index not found");
            static foreach(i; 0 .. RowType.length)
                mixin("data[i][pos] " ~ op ~ "= elements.data[i];");
        }
    }

    /// Slice assignment operation
    void opIndexAssign(T, SliceKind kind)(Slice!(T*, 1, kind) sl, string[] index, int axis = 1)
    {
        opIndexOpAssign!("")(sl, index, axis);
    }

    /// Slice assignment operation
    void opIndexOpAssign(string op, T, SliceKind kind)(Slice!(T*, 1, kind) sl, string[] index, int axis = 1)
    {
        if(axis)
        {
            Axis!void fwd;
            fwd.data.length = sl.shape[0];
            foreach(i; 0 .. sl.shape[0])
                fwd.data[i] = DataType(sl[i]);

            opIndexOpAssign!(op)(fwd, index, axis);
        }
        else
        {
            Axis!(RowType) fwd;
            static foreach(i; 0 .. RowType.length)
                static if(__traits(isArithmetic, RowType[i], T))
                    fwd.data[i] = cast(RowType[i])sl[i];
                else
                {
                    import std.conv: to;
                    fwd.data[i] = to!(RowType[i])(sl[i]);
                }

            opIndexOpAssign!(op)(fwd, index, axis);
        }
    }

    /++
    void apply(alias Fn, int axis, T)(T index)
    Description: Applies a function on a particular row or column
    @params: Fn - Function
    @params: axis - 0 for rows, 1 for columns
    @params: index - integer or string indexes of rows
    +/
    void apply(alias Fn, int axis, T)(T index)
        if(is(T == int[]) || is(T == string[][]))
    {
        if(index.length == 0)
            return;

        static if(is(T == string[][]))
            foreach(i; index)
                assert(i.length == indx.indexing[axis].codes.length, "Index level mismatch");

        ptrdiff_t[] pos;
        static if(is(T == int[]))
        {
            pos.length = index.length;
            foreach(i, ele; index)
                pos[i] = ele;
        }
        else
        {
            pos.length = index.length;
            foreach(i, ele; index)
                pos[i] = getPosition!(axis)(ele);
        }

        static if(axis == 0)
        {
            static foreach(i; 0 .. RowType.length)
            {
                foreach(j; pos)
                    data[i][j] = cast(RowType[i])Fn(data[i][j]);
            }
        }
        else
        {
            static foreach(i; 0 .. RowType.length)
            {
                import std.algorithm: countUntil;
                if(countUntil(pos, i) > -1)
                    foreach(j; 0 .. rows)
                        data[i][j] = cast(RowType[i])Fn(data[i][j]);
            }
        }
    }

    /++
    void apply(alias Fn, int axis, T)(T index)
    Description: apply overload - apply to all data
    @params: Fn - Function
    @params: axis - 0 for rows, 1 for columns
    +/
    void apply(alias Fn)()
    {
        static foreach(i; 0 .. RowType.length)
            foreach(j; 0 .. rows)
                data[i][j] = cast(RowType[i])Fn(data[i][j]);
    }

    /++
    auto drop(int axis, int[] positions)() @property
    Description: Drop a row/column from DataFrame
    @params: axis - 0 for dropping row, 1 for dropping columns
    @params: positions - integer array of positions of all the rows/colmns to be dropped
    +/
    auto drop(int axis, int[] positions)() @property
    {
        import magpie.helper: dropper;
        import std.algorithm: reduce, max, min;

        static if(positions.length == 0)
        {
            return this;
        }
        else static if(axis == 0)
            return dropperRuntimeInternal(positions);
        else
        {
            assert(positions.reduce!min > -1 && positions.reduce!max < cols, "Index out of bound");
            DataFrame!(true, dropper!(positions, RowType)) ret;
            ret.indx = indx;
            ret.indx = Index();
            ret.indx.indexing[0] = indx.indexing[0];
            ret.indx.column.titles = indx.column.titles;

            ret.indx.column.index.length = indx.column.index.length;
            ret.indx.column.codes.length = indx.column.index.length;

            import std.range: lockstep;
            foreach(pos, a, b; lockstep(indx.column.index, indx.column.codes))
            {
                ret.indx.column.index[pos] = a;
                ret.indx.column.codes[pos] = dropper(positions, b);
            }

            static if(isHomogeneousType)
                auto retdata = dropper(positions, data);
            else
                auto retdata = dropper!(positions, data);
            
            static foreach(i; 0 .. ret.RowType.length)
                ret.data[i] = retdata[i];

            ret.rows = rows;
            return ret;
        }
    }

    /++
    auto columnToIndex(int position)() @property
    Description: Converts a column into row index level
    @params: position - integral index of column to be converted to index
    +/
    auto columnToIndex(int position)() @property
    {
        assert(position > -1 && position < cols, "Index out of bound");

        import std.conv: to;
        auto ret = this.drop!(1, [position]);
        static if(is(RowType[position] == string))
        {
            ret.indx.row.index ~= data[position];
            ret.indx.row.codes ~= [[]];
        }
        else static if(is(RowType[position] == int))
        {
            ret.indx.row.index ~= [[]];
            ret.indx.row.codes ~= data[position];
        }
        else
        {
            ret.indx.row.index ~= to!(string[])(data[position]);
            ret.indx.row.codes ~= [[]];
        }

        if(indx.column.index[$ - 1].length == 0)
            ret.indx.row.titles ~= to!(string)(indx.column.codes[$ - 1][position]);
        else
            ret.indx.row.titles ~= indx.column.index[$ - 1][indx.column.codes[$ - 1][position]];

        ret.indx.generateCodes();

        return ret;
    }

    /++
    auto indexToData(int position, Dtype...)(int indexLevel, string[] dataIndex)
    Description: Convers a level of indexing to data
    @params: position - position to insert data at
    @params: DataType - Data type for the new data column
    @params: indexLevel - Index level to convert to data
    @params: dataIndex - Index for the new data column
    +/
    auto indexToData(int position, DataType)(int indexLevel, string[] dataIndex)
        if(position <= RowType.length)
    {
        DataFrame!(true, RowType[0 .. position], DataType, RowType[position .. $]) ret;
        ret.indx = indx;
        ret.indx.extend!1(dataIndex, position);

        ret.indx.row.index = ret.indx.row.index[0 .. indexLevel] ~ ret.indx.row.index[indexLevel + 1 .. $];
        ret.indx.row.codes = ret.indx.row.codes[0 .. indexLevel] ~ ret.indx.row.codes[indexLevel + 1 .. $];
        ret.indx.row.titles = ret.indx.row.titles[0 .. indexLevel] ~ ret.indx.row.titles[indexLevel + 1 .. $];

        ret.indx.optimize();

        static foreach(i; 0 .. RowType.length + 1)
        {
            static if(i < position)
                ret.data[i] = data[i];
            else static if(i == position)
            {
                import std.conv: to;
                import std.array: array;
                import std.algorithm: map;

                toArr!DataType newData;
                if(indx.row.index[indexLevel].length == 0)
                    newData = to!(toArr!DataType)(indx.row.codes[indexLevel]);
                else
                    newData = to!(toArr!DataType)(indx.row.codes[indexLevel].map!(e => indx.row.index[indexLevel][e]).array());

                ret.data[i] = newData;
            }
            else
                ret.data[i] = data[i - 1];
        }

        ret.rows = rows;
        return ret;
    }

    /++
    Group DataFrame rows based on column values
    Params:
        dataLevels: Integer position of data columns to consider for groupBy
        indexLevels: Levels of row index to consider for grouping
    Returns:
        A Group object
    +/
    auto groupBy(int[] dataLevels = [])(int[] indexLevels = [])
    {
        import magpie.group: Group;
        import magpie.helper: dropper;

        Group!(dropper!(dataLevels, RowType)) grp;
        grp.createGroup!(dataLevels)(this, indexLevels);

        return grp;
    }

    /++
    Get DataFrame data as a Slice
    Params:
        Type: Iterator type for the resultant slice
        Kind: SliceKind for the resultant Slice
    Returns:
        A Slice of type Slice!(Type*, 2, kind)
    +/
    auto asSlice(Type, SliceKind kind)() @property
    {
        static if(__traits(isArithmetic, Type))
            alias RetType = Type;
        else
            alias RetType = string;

        static if(kind == Universal)
            Slice!(RetType*, 2, kind) ret = slice!(RetType)(rows, cols).universal;
        else static if(kind == Canonical)
            Slice!(RetType*, 2, kind) ret = slice!(RetType)(rows, cols).canonical;
        else
            Slice!(RetType*, 2, kind) ret = slice!(RetType)(rows, cols);

        static foreach(i; 0 .. RowType.length)
        {
            static if(__traits(isArithmetic, RowType[i], RetType) || is(RetType == RowType[i]))
            {
                foreach(j; 0 .. rows)
                    ret[j][i] = cast(RetType)data[i][j];
            }
            else static if(is(RetType == string))
            {
                import std.conv: to;
                foreach(j; 0 .. rows)
                    ret[j][i] = to!(string)(data[i][j]);
            }
        }

        return ret;
    }

    auto asSlice(SliceKind kind, Type = string, int axis = 0, T)(T index)
        if(is(T == string[]) || is(T == int))
    {
        ptrdiff_t pos;
        static if(is(T == string[]))
            pos = getPosition!(axis)(index);
        else
            pos = index;

        assert(pos > -1, "Index not found");
        static if(axis)
        {
            static if(kind == Universal)
                Slice!(Type*, 1, kind) ret = slice!(Type)(rows).universal;
            else static if(kind == Canonical)
                Slice!(Type*, 1, kind) ret = slice!(Type)(rows).canonical;
            else
                Slice!(Type*, 1, kind) ret = slice!(Type)(rows);

            void sliceAssignAux(ptrdiff_t si = -1)(size_t ri = 0) @property
            {
                static if(si > -1)
                {
                    alias i = si;
                    enum size_t typepos = si;
                }
                else
                {
                    size_t i = ri;
                    enum size_t typepos = 0;
                }

                static if(__traits(isArithmetic, Type, RowType[typepos]) || is(Type == RowType[typepos]))
                    foreach(j; 0 .. rows)
                        ret[j] = cast(Type)data[i][j];
            }
            
            mixin auxDispatch!(sliceAssignAux, isHomogeneousType, RowType);
            auxDispatch(pos);
            return ret;
        }
        else
        {
            import std.conv: to;
            Slice!(string*, 1, kind) ret;
            static if(kind == Universal)
                ret = slice!(string)(RowType.length).universal;
            else static if(kind == Canonical)
                ret = slice!(string)(RowType.length).canonical;
            else
                ret = slice!(string)(RowType.length);

            static foreach(i; 0 .. RowType.length)
                ret[i] = to!string(data[i][pos]);

            return ret;
        }
    }

    /++
    Applies mathematical operations on DataFrame row/column
    Params:
        axis: 0 to compute row wise, 1 to compute column wise
        Ops: Mathematical operations to apply
    Returns:
        DataFrame with computed operations  
    +/
    auto aggregate(int axis, Ops...)() @property
    {
        void init(int axis, T)(ref T args)
        {
            enum int perpendicular = ((axis)? 0: 1);
            
            args.indx.indexing[axis] = indx.indexing[axis];
            args.indx.indexing[perpendicular].index.length = 1;
            args.indx.indexing[perpendicular].index[0].length = Ops.length;
            args.indx.indexing[perpendicular].codes.length = 1;

            static foreach(i; 0 .. Ops.length)
                ret.indx.indexing[perpendicular].index[0][i] = __traits(identifier, Ops[0]);
            
            static if(axis)
            {
                args.indx.row.titles = ["Operation"];
                args.rows = Ops.length;

                static foreach(i; 0 .. RowType.length)
                    ret.data[i].length = Ops.length;
            }
            else
            {
                args.rows = rows;
                static foreach(i; 0 .. Ops.length)
                    ret.data[i].length = rows;
            }
        }

        static if(axis)
        {
            import std.meta: staticMap;

            alias Resolve(T) = suitableType!(resolverInternal!(T, Ops));
            DataFrame!(true, staticMap!(Resolve, RowType)) ret;
            init!(axis)(ret);

            static foreach(i; 0 .. RowType.length)
                static if(__traits(isArithmetic, RowType[i]))
                    static foreach(j; 0 .. Ops.length)
                    {
                        static if(__traits(compiles, Ops[j](data[i])))
                            ret.data[i][j] = Ops[j](data[i]);
                        else
                        {
                            import std.algorithm: map, reduce;
                            if(__traits(compiles, data[i].map!(e => e).reduce!(Ops[j])))
                                ret.data[i][j] = cast(ret.RowType[i])data[i].map!(e => e).reduce!(Ops[j]);
                        }
                    }
        }
        else
        {
            DataFrame!(true, aggregateType!(Ops)) ret;
            init!(axis)(ret);

            suitableType!(RowType)[RowType.length] oparr;
            size_t k;

            foreach(i; 0 .. rows)
            {
                k = 0;
                static foreach(j; 0 .. RowType.length)
                    static if(__traits(isArithmetic, RowType[j]))
                    {
                        oparr[k] = data[j][i];
                        ++k;
                    }
                
                static foreach(j; 0 .. Ops.length)
                {
                    static if(__traits(compiles, Ops[j](oparr[0 .. k])))
                    {
                        if(k > 0)
                            ret.data[j][i] = Ops[j](oparr[0 .. k]);
                    }
                    else
                    {
                        import std.algorithm: map, reduce;
                        if(k > 0 && __traits(compiles, oparr[0 .. k].map!(e => e).reduce!(Ops[j])))
                            ret.data[j][i] = cast(ret.RowType[j])oparr[0 .. k].map!(e => e).reduce!(Ops[j]);
                    }
                }
            }
        }

        ret.indx.generateCodes();
        return ret;
    }

    /++
    Filter the DataFrame based on the result of callback
    Params:
        filterFunc: Callback function based on whose result, the DataFrame is filtered
    Returns:
        Filtered DataFrame
    +/
    auto filter(alias filterFunc)() @property
    {
        auto applier(alias filterFunc, T...)(T element)
        {
            static if(isHomogeneousType)
                toArr!(RowType[0]) rowArray = element[0][0];
            else
                alias rowArray = element;

            return filterFunc(rowArray);
        }

        int[] pos;
        int k, count;
        pos.length = rows;
        
        static if(isHomogeneousType)
        {
            import magpie.helper: transposed;
            RowType[0][][] operableData = transposed(data);
        }
        else
            alias operableData = data;

        import std.range: zip;
        auto zipped = zip(operableData);
        foreach(ele; zipped)
        {
            if(!applier!filterFunc(ele))
            {
                pos[k] = count;
                ++k;
            }
            ++count;
        }

        return dropperRuntimeInternal(pos[0 .. k]);
    }

    auto pivot(size_t col_size)(int[] index, int[] columns, int[] values)
    {
        import std.conv: to;
        import std.algorithm: countUntil;

        DataFrame!(suitableType!RowType, col_size) ret;
        Index inx;
        string[][][2] indices;
        string[] titles;

        indices[0].length = index.length;
        titles.length = index.length;
        indices[1].length = columns.length;

        static foreach(k; 0 .. 2)
        {
            foreach(pos, i; ((k == 0) ? index : columns))
            {
                string[] indxdata;
                string[] unique;
                int end;
                
                static if(isHomogeneousType)
                    indxdata = to!(string[])(data[i]);
                else
                    static foreach(j; 0 .. RowType.length)
                        if(i == j)
                            indxdata = to!(string[])(data[j]);

                unique.length = rows;
                // Getting Unique indexes in order to prevent index collision
                foreach(j; indxdata)
                {
                    if(countUntil(unique, j) == -1)
                    {
                        unique[end] = j;
                        ++end;
                    }
                }
                
                indices[k][pos] = unique[0 .. end];
                if(!k)
                    titles[pos] = "Index" ~ to!string(pos);
            }
        }

        const size_t level_size = indices[0][$ - 1].length;
        inx.constructFromLevels!0(indices[0], titles);
        inx.constructFromLevels!1(indices[1]);
        ret.setFrameIndex(inx);
        ret.rows = ret.indx.indexing[0].codes[0].length;

        foreach(i, ele; values)
        {
            if(i > col_size / level_size - 1)
                break;

            toArr!(ret.RowType[0]) dfval;
            static if(isHomogeneousType)
                dfval = to!(toArr!(ret.RowType[0]))(data[ele]);
            else
                static foreach(j; 0 .. RowType.length)
                {
                    if(j == ele)
                        dfval = to!(toArr!(ret.RowType[0]))(data[j]);
                }

            size_t start;
            while((start < dfval.length) && (start / level_size < ret.rows))
            {
                foreach(j; 0 .. level_size)
                {
                    if(start > dfval.length - 1)
                        break;
                    
                    ret.data[i * level_size + j][start / level_size] = dfval[start];
                    ++start;
                }
            }
        }

        return ret;
    }
}

// Testing DataFrame Definition - O(n + log(n))
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));
}

// O(log(n)) init
unittest
{
    DataFrame!(true, double, double, double) df;
    assert(is(typeof(df.data) == double[][3]));
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

// Community suggested way to declare a DataFrame
unittest
{
    DataFrame!(int[3], double) df;
    assert(is(df.RowType == AliasSeq!(int, int, int, double)));
}

// Community suggested way to declare a DataFrame - from struct
unittest
{
    struct Example
    {
        int[2] x;
        double[2] y;
    }

    import std.traits: Fields;
    DataFrame!(Fields!Example) df;
    assert(is(df.RowType == AliasSeq!(int, int, double, double)));
}

// Getting element from it's index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1"];
    df.indx.row.index = [["Hello", "Hi"]];
    df.indx.row.codes = [[]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello", "Hi"]];
    df.indx.column.codes = [[]];
    df.rows = 2;
    df.data[0] = [1,2];
    df.data[1] = [1,2];

    assert(df.at!(0,0) == 1);
    assert(df.at!(0,1) == 1);
    assert(df.at!(1,0) == 2);
    assert(df.at!(1,1) == 2);

    assert(df[0, 0] == 1);
    assert(df[0, 1] == 1);
    assert(df[1, 0] == 2);
    assert(df[1, 1] == 2);
}

// Setting element at an index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1"];
    df.indx.row.index = [["Hello", "Hi"]];
    df.indx.row.codes = [[]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello", "Hi"]];
    df.indx.column.codes = [[]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

    df[0, 0] = 42;
    assert(df.data[0] == [42, 2]);
}

// Assignment based on string headers
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[0, 1], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

    df[["Hello", "Hello", "1"], ["Hello", "1", "Hello"]] = 48;
    assert(df.data[0] == [48, 2]);
    assert(df[["Hello", "Hello", "1"], ["Hello", "1", "Hello"]] == 48);

    df[["Hi", "Hello", "24"], ["Hello", "1", "Hello"]] = 29;
    assert(df.data[0] == [48, 29]);
    assert(df[["Hi", "Hello", "24"], ["Hello", "1", "Hello"]] == 29);

    df[["Hello", "Hello", "1"], ["Hi", "2", "Hello"]] = 96;
    assert(df.data[1] == [96, 2]);
    assert(df[["Hello", "Hello", "1"], ["Hi", "2", "Hello"]] == 96);

    df[["Hi", "Hello", "24"], ["Hi", "2", "Hello"]] = 43;
    assert(df.data[1] == [96, 43]);
    assert(df[["Hi", "Hello", "24"], ["Hi", "2", "Hello"]] == 43);
}


// getiing integer position of the given row row.index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1,24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

    assert(df.getRowPosition(["Hello", "Hello", "1"]) == 0);
    assert(df.getRowPosition(["Hi", "Hello", "24"]) == 1);
    assert(df.getRowPosition(["Hi", "Hello", "54"]) == -1);
    assert(df.getRowPosition(["Hi", "Helo", "24"]) == -1);
    assert(df.getRowPosition(["H", "Hello", "54"]) == -1);
}

// getiing integer position of the given column row.index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[0, 1], [1, 2], [0, 0]];
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
    inx.setIndex([1, 2, 3, 4, 5], ["Index"]);
    df.setFrameIndex(inx);
    string ret = df.display(true, 200);
    assert(ret == "Index  0    1\n"
        ~ "1      nan  0\n"
        ~ "2      nan  0\n"
        ~ "3      nan  0\n"
        ~ "4      nan  0\n"
        ~ "5      nan  0\n"
    );
}

// Checking if setting index works as intended when assigning using the set index
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([1, 2, 3, 4, 5], ["Index"], [1, 2], ["Index"]);
    df.setFrameIndex(inx);
    df[["1"], ["2"]] = 42;
    assert(df.data[1] == [42, 0, 0, 0, 0]);
}

// Checking if setting index works as intended when assigning using the set index
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [1, 2], ["Index"]);
    df.setFrameIndex(inx);
    df[["Hello", "Hi"], ["2"]] = 42;
    assert(df.data[1] == [42, 0]);
}

// Checking if setting index works as intended when assigning using the set index
unittest
{
    DataFrame!(double, int) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [["Hello", "Hi"], ["Hi", "Hello"]]);
    df.setFrameIndex(inx);
    df[["Hello", "Hi"], ["Hi", "Hello"]] = 42;
    assert(df.data[1] == [42, 0]);
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
    assert(df.data[0] == [1, 3]);
    assert(df.data[1] == [2, 4]);
    assert(df[["Hello", "Hi"], ["Hello", "Hi"]] == 1);
    assert(df[["Hello", "Hi"], ["Hi", "Hello"]] == 2);
    assert(df[["Hi", "Hello"], ["Hello", "Hi"]] == 3);
    assert(df[["Hi", "Hello"], ["Hi", "Hello"]] == 4);

    // Assignment that needs apdding
    df = [[1], [2, 3]];
    // df.display();
    assert(df.data[0] == [1, 2]);
    assert(df.data[1] == [0, 3]);
    assert(df[["Hello", "Hi"], ["Hello", "Hi"]] == 1);
    assert(df[["Hello", "Hi"], ["Hi", "Hello"]] == 0);
    assert(df[["Hi", "Hello"], ["Hello", "Hi"]] == 2);
    assert(df[["Hi", "Hello"], ["Hi", "Hello"]] == 3);

    // Checking casting
    df = [[1.2, 1], [4.6, 7]];
    // df.display();
    assert(df.data[0] == [1.2, 4.6]);
    assert(df.data[1] == [1, 7]);
    assert(df[["Hello", "Hi"], ["Hello", "Hi"]] == 1.2);
    assert(df[["Hello", "Hi"], ["Hi", "Hello"]] == 1);
    assert(df[["Hi", "Hello"], ["Hello", "Hi"]] == 4.6);
    assert(df[["Hi", "Hello"], ["Hi", "Hello"]] == 7);
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

    // Assigning column.index using direct index
    df.assign!1(0, [1.26, 4.6]);
    assert(df.data[0] == [1.26, 4.6]);

    // Partial Assignment - rows
    df.assign!0(1, 3.588);
    assert(df.data[0][1] == 3.588);
    assert(df.data[1][1] == 6);

    // Partial Assignment - column.index
    df.assign!1(0, [2.26]);
    assert(df.data[0] == [2.26, 3.588]);
}

// Getting an entire row/column from the DataFrame
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"],["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[0, 1],[1, 2],[0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

    assert(df[["Hello", "1", "Hello"]].data == [1, 2]);
    assert(df[["Hi", "2", "Hello"]].data == [1, 2]);
    assert(df[["Hello", "Hello", "1"], 0].data[0] == 1);
    assert(df[["Hi", "Hello", "24"], 0].data[0] == 2);
    assert(df[["Hello", "Hello", "1"], 0].data[1] == 1);
    assert(df[["Hi", "Hello", "24"], 0].data[1] == 2);
}

// Column binary Operation
unittest
{
    DataFrame!(int, 3) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1, 4]);
    df.assign!1(1, [1, 6]);
    df.assign!1(2, [1, 8]);
    // df.display();

    df[["0"]] = df[["1"]] + df[["2"]];
    assert(df.data[0] == [2, 14]);
    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0];
    assert(df.data[0][0] == 14 && df.data[1][0] == 6 && df.data[2][0] == 8);
    // df.display();

    df[["0"]] = df[["1"]] - df[["2"]];
    assert(df.data[0] == [-2, -2]);
    // df.display();

    df[["0"]] = df[["1"]] * df[["2"]];
    assert(df.data[0] == [48, 48]);
    // df.display();

    df[["0"]] = df[["1"]] / df[["2"]];
    assert(df.data[0] == [0, 0]);
    // df.display();

    df[["0"]] = df[["1"]];
    assert(df.data[0] == [6, 6]);
}

// Row binary operations
unittest
{
    DataFrame!(int, 3) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"]], ["Index", "Index", "Index"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1, 4, 8]);
    df.assign!1(1, [1, 6, 9]);
    df.assign!1(2, [1, 8, 17]);

    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0] + df[["Hey", "Hey"], 0];
    assert(df.data[0][0] == 12 && df.data[1][0] == 15 && df.data[2][0] == 25);

    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0] - df[["Hey", "Hey"], 0];
    assert(df.data[0][0] == -4 && df.data[1][0] == -3 && df.data[2][0] == -9);

    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0] * df[["Hey", "Hey"], 0];
    assert(df.data[0][0] == 32 && df.data[1][0] == 54 && df.data[2][0] == 136);

    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0] / df[["Hey", "Hey"], 0];
    assert(df.data[0][0] == 0 && df.data[1][0] == 0 && df.data[2][0] == 0);

    // df.display();
}

// Column Binary Operation - Heterogeneous DataFrame
unittest
{
    DataFrame!(int, 2, double, 2) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1, 4]);
    df.assign!1(1, [1, 6]);
    df.assign!1(2, [1.9, 8.4]);
    df.assign!1(3, [9.2, 4.6]);
    // df.display();

    df[["0"]] = df[["1"]];
    assert(df.data[0] == [1, 6]);

    // If for some reason float is assigned to int, it gets explicitly converted
    // Supported only for floating point -> Integral kind
    df[["0"]] = df[["2"]].convertTo!(int[]);
    assert(df.data[0] == [1, 8]);

    df[["1"]] = (df[["2"]] + df[["3"]]).convertTo!(int[]);
    assert(df.data[1] == [11, 13]);

    df[["0"]] = (df[["1"]] + df[["2"]] + df[["3"]]).convertTo!(int[]);
    assert(df.data[0] == [22, 26]);

    df[["3"]] = df[["0"]];
    assert(df.data[3] == [22, 26]);

    df[["0"]] = (df[["1"]] + df[["2"]] * df[["3"]]).convertTo!(int[]);
    foreach(i; 0 .. 2)
        assert(df.data[0][i] == cast(int)(df.data[1][i] + df.data[2][i] * df.data[3][i]));

    df[["0"]] = (df[["1"]] - df[["2"]] / df[["3"]]).convertTo!(int[]);
    foreach(i; 0 .. 2)
        assert(df.data[0][i] == cast(int)(df.data[1][i] - df.data[2][i] / df.data[3][i]));

    import std.math: approxEqual;
    df[["2"]] = df[["0"]] * df[["1"]] / df[["3"]];
    foreach(i; 0 .. 2)
        assert(approxEqual(df.data[2][i], df.data[0][i] * df.data[1][i] / df.data[3][i], 1e-3));

    df[["3"]] = df[["2"]] / df[["1"]] / df[["0"]];
    foreach(i; 0 .. 2)
        assert(approxEqual(df.data[3][i], df.data[2][i] / df.data[1][i] / df.data[0][i], 1e-3));

    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0];
    static foreach(i; 0 .. 4)
        assert(approxEqual(df.data[i][0], df.data[i][1], 1e-3));
}

// Row binary Operation on Heterogeneous DataFrame
unittest
{
    DataFrame!(int, 2, double, 2) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey", "Ahoy"], ["Hi", "Hello", "Ahoy", "Hey"]], ["Index", "Index"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1, 4, 7, 8]);
    df.assign!1(1, [1, 6, 13, 45]);
    df.assign!1(2, [1.9, 8.4, 17.2, 34.3]);
    df.assign!1(3, [9.2, 4.6, 19.6, 44.3]);
    // df.display();

    import std.math: approxEqual;
    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0] + df[["Hey", "Ahoy"], 0] * df[["Ahoy", "Hey"], 0];
    static foreach(i; 0 .. 4)
        assert(approxEqual(df.data[i][0], df.data[i][1] + df.data[i][2] * df.data[i][3], 1e-1));

    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0] * df[["Hey", "Ahoy"], 0] / df[["Ahoy", "Hey"], 0];
    static foreach(i; 0 .. 4)
        assert(approxEqual(df.data[i][0], df.data[i][1] * df.data[i][2] / df.data[i][3], 1e-1));

    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0] * df[["Hey", "Ahoy"], 0] * df[["Ahoy", "Hey"], 0];
    static foreach(i; 0 .. 4)
        assert(approxEqual(df.data[i][0], df.data[i][1] * df.data[i][2] * df.data[i][3], 1e-1));

    df[["Hello", "Hi"], 0] = df[["Hi", "Hello"], 0] / df[["Hey", "Ahoy"], 0] / df[["Ahoy", "Hey"], 0];
    static foreach(i; 0 .. 4)
        assert(approxEqual(df.data[i][0], df.data[i][1] / df.data[i][2] / df.data[i][3], 1e-1));

    df[["Hello", "Hi"], 0] = (df[["Hi", "Hello"], 0] + df[["Hey", "Ahoy"], 0]) * df[["Ahoy", "Hey"], 0];
    static foreach(i; 0 .. 4)
        assert(approxEqual(df.data[i][0], (df.data[i][1] + df.data[i][2]) * df.data[i][3], 1e-1));

    df[["Hi", "Hello"], 0] = (df[["Hello", "Hi"], 0] + df[["Hey", "Ahoy"], 0]) * df[["Ahoy", "Hey"], 0];
    static foreach(i; 0 .. 4)
        assert(approxEqual(df.data[i][1], (df.data[i][0] + df.data[i][2]) * df.data[i][3], 1e-1));

    df[["Hi", "Hello"], 0] = (df[["Hello", "Hi"], 0] - df[["Hey", "Ahoy"], 0]) / df[["Ahoy", "Hey"], 0];
    static foreach(i; 0 .. 4)
        assert(approxEqual(df.data[i][1], (df.data[i][0] - df.data[i][2]) / df.data[i][3], 1e-1));
}

// Checking if pattern matching for steIndex works - If an index isn't assignable, it's overlooked
unittest
{
    DataFrame!(double, 2, int) df;
    Index inx;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["Index", "Index"], [["Hello", "Hi"], ["Hi", "Hello"]]);
    df.setFrameIndex(inx);

    assert(df.indx.column.index == [[]]);
    assert(df.indx.column.codes == [[0, 1, 2]]);
    assert(df.indx.row.index == [["Hello", "Hi"], ["Hello", "Hi"]]);
    assert(df.indx.row.codes == [[0, 1], [1, 0]]);
}

// Simple Data Frame
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1"];
    df.indx.row.index = [["Hello", "Hi"]];
    df.indx.row.codes = [[]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello", "Hi"]];
    df.indx.column.codes = [[]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);
    assert(ret == "Index1  Hello  Hi\n"
        ~ "Hello   1      1 \n"
        ~ "Hi      2      2 \n"
    );
}

// Simple DataFrame with both row and column index title
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1"];
    df.indx.row.index = [["Hello", "Hi"]];
    df.indx.row.codes = [[]];
    df.indx.column.titles = ["Also Index"];
    df.indx.column.index = [["Hello", "Hi"]];
    df.indx.column.codes = [[]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);

    assert(ret == "Also Index  Hello  Hi\n"
        ~ "Index1    \n"
        ~ "Hello       1      1 \n"
        ~ "Hi          2      2 \n"
    );
}

// Multi-Indexed rows
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello", "Hi"]];
    df.indx.row.codes = [[],[]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello", "Hi"]];
    df.indx.column.codes = [[]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);

    assert(ret == "Index1  Index2  Hello  Hi\n"
        ~ "Hello   Hello   1      1 \n"
        ~ "Hi      Hi      2      2 \n"
    );
}
// Multi Indexed column.index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1"];
    df.indx.row.index = [["Hello", "Hi"]];
    df.indx.row.codes = [[]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello", "Hi"], ["Hello", "Hi"]];
    df.indx.column.codes = [[], []];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);

    assert(ret == "        Hello  Hi\n"
        ~ "Index1  Hello  Hi\n"
        ~ "Hello   1      1 \n"
        ~ "Hi      2      2 \n"
    );
}

// Multi Indexed column.index with titles
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1"];
    df.indx.row.index = [["Hello", "Hi"]];
    df.indx.row.codes = [[]];
    df.indx.column.titles = ["CIndex1", "CIndex2"];
    df.indx.column.index = [["Hello", "Hi"], ["Hello", "Hi"]];
    df.indx.column.codes = [[], []];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);

    assert(ret == "CIndex1  Hello  Hi\n"
        ~ "CIndex2  Hello  Hi\n"
        ~ "Index1 \n"
        ~ "Hello    1      1 \n"
        ~ "Hi       2      2 \n"
    );
}

// Wide DataFrame
unittest
{
    DataFrame!(int, 20) df;
    assert(is(typeof(df.data) == int[][20]));

    df.indx.row.titles = ["Index1"];
    df.indx.row.index = [["Hello", "Hi"]];
    df.indx.row.codes = [[]];
    df.indx.column.titles = [];
    df.indx.column.index = [[]];
    df.indx.column.codes = [[]];
    df.rows = 2;
    int[] arr = [12_222_222, 12_222_222];

    static foreach(i; 0 .. 20)
    {
        import std.conv: to;
        df.indx.column.index[0] ~= to!string(i);
        df.data[i] = arr;
    }

    string ret = df.display(true, 200);
    assert(ret == "Index1  0         1         2         3         4         5         6         7         ...  11        12        13        14        15        16        17        18        19      \n"
        ~ "Hello   12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  ...  12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222\n"
        ~ "Hi      12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  ...  12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222  12222222\n"
    );
}

// Daddy Long Legs DataFrame
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1"];
    df.indx.row.index = [[]];
    df.indx.row.codes = [[]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello", "Hi"]];
    df.indx.column.codes = [[]];
    df.rows = 100;
    int[] arr = [];
    foreach(i; 0 .. 100)
    {
        arr ~= i;
        import std.conv: to;
        df.indx.row.index[0] ~= to!string(i);
    }
    df.data[0] = arr;
    df.data[1] = arr;
    string ret = df.display(true, 200);

    assert(ret == "Index1  Hello  Hi\n"
        ~ "0       0      0 \n"
        ~ "1       1      1 \n"
        ~ "2       2      2 \n"
        ~ "3       3      3 \n"
        ~ "4       4      4 \n"
        ~ "5       5      5 \n"
        ~ "6       6      6 \n"
        ~ "7       7      7 \n"
        ~ "8       8      8 \n"
        ~ "9       9      9 \n"
        ~ "10      10     10\n"
        ~ "11      11     11\n"
        ~ "12      12     12\n"
        ~ "13      13     13\n"
        ~ "14      14     14\n"
        ~ "15      15     15\n"
        ~ "16      16     16\n"
        ~ "17      17     17\n"
        ~ "18      18     18\n"
        ~ "19      19     19\n"
        ~ "20      20     20\n"
        ~ "21      21     21\n"
        ~ "22      22     22\n"
        ~ "23      23     23\n"
        ~ "......  .....  ..\n"
        ~ "75      75     75\n"
        ~ "76      76     76\n"
        ~ "77      77     77\n"
        ~ "78      78     78\n"
        ~ "79      79     79\n"
        ~ "80      80     80\n"
        ~ "81      81     81\n"
        ~ "82      82     82\n"
        ~ "83      83     83\n"
        ~ "84      84     84\n"
        ~ "85      85     85\n"
        ~ "86      86     86\n"
        ~ "87      87     87\n"
        ~ "88      88     88\n"
        ~ "89      89     89\n"
        ~ "90      90     90\n"
        ~ "91      91     91\n"
        ~ "92      92     92\n"
        ~ "93      93     93\n"
        ~ "94      94     94\n"
        ~ "95      95     95\n"
        ~ "96      96     96\n"
        ~ "97      97     97\n"
        ~ "98      98     98\n"
        ~ "99      99     99\n"
    );
}

// Multi Indexed DataFrame
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);
    assert(ret == "                        Hello  Hi   \n"
        ~ "                        1      2    \n"
        ~ "Index1  Index2  Index3  Hello  Hello\n"
        ~ "Hello   Hello   1       1      1    \n"
        ~ "Hi      Hello   24      2      2    \n"
    );
}

// Multi Indexed DataFrame with both row and column titles
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);
    assert(ret == "                Hey     Hello  Hi   \n"
        ~ "                Hey     1      2    \n"
        ~ "                Hey     Hello  Hello\n"
        ~ "Index1  Index2  Index3\n"
        ~ "Hello   Hello   1       1      1    \n"
        ~ "Hi      Hello   24      2      2    \n"
    );

    // df.to_csv("");
}

// Multi-Indexed with skipping row index
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[1, 1], [0, 0], [1, 24]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.indx.isMultiIndexed = true;
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);
    assert(ret == "                        Hello  Hi   \n"
        ~ "                        1      2    \n"
        ~ "Index1  Index2  Index3  Hello  Hello\n"
        ~ "Hi      Hello   1       1      1    \n"
        ~ "                24      2      2    \n"
    );
}

// Middle row.index won't skip if the outer row.index aren't skipped
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[1, 0], [0, 0], [1, 24]];
    df.indx.column.titles = [];
    df.indx.column.index = [["Hello","Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.indx.isMultiIndexed = true;
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];
    string ret = df.display(true, 200);

    assert(ret == "                        Hello  Hi   \n"
        ~ "                        1      2    \n"
        ~ "Index1  Index2  Index3  Hello  Hello\n"
        ~ "Hi      Hello   1       1      1    \n"
        ~ "Hello   Hello   24      2      2    \n"
    );

    // df.to_csv("", true, false);
}

// Writing entire dataframe to csv
unittest
{
    DataFrame!(int, 2) df;
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

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
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

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
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"],["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

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
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"],["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

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
    assert(is(typeof(df.data) == int[][2]));

    df.indx.row.titles = ["Index1", "Index2", "Index3"];
    df.indx.row.index = [["Hello", "Hi"], ["Hello"], []];
    df.indx.row.codes = [[0, 1], [0, 0], [1, 24]];
    df.indx.column.titles = ["Hey", "Hey", "Hey"];
    df.indx.column.index = [["Hello", "Hi"], [], ["Hello"]];
    df.indx.column.codes = [[], [1, 2], [0, 0]];
    df.rows = 2;
    df.data[0] = [1, 2];
    df.data[1] = [1, 2];

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

// Parsing CSV without row row.index
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

// Parsing CSV without any row.index
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
    assert(df.data[0] == [1, 2]);
}

// Partial parsing - second column
unittest
{
    DataFrame!(int) df;
    df.from_csv("./test/tocsv/ex2p6.csv", 0, 0, [1]);
    // df.display();
    assert(df.data[0] == [1, 24]);
}

// Parsing by mentioning all the column.index
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
    assert(df.data[0] == [1, 2]);
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
    assert(df.data[0] == [1, 2]);
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

// DataFrame without row indexes - testt for groupBy
unittest
{
    DataFrame!(int, int) df;
    df.indx.column.index = [["Data1L1", "Data2L1"], ["Data1L2", "Data2L2"]];
    df.indx.column.codes = [[0, 1], [0, 1]];
    df.data[0] = [1, 2, 3];
    df.data[1] = [1, 2, 3];
    df.rows = 3;
    assert(df.display(true, 200) == "Data1L1  Data2L1\n"
        ~ "Data1L2  Data2L2\n"
        ~ "1        1      \n"
        ~ "2        2      \n"
        ~ "3        3      \n"
    );
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

// Parsing of dataset 1
unittest
{
    DataFrame!(int, 9, double, int, 4) df;
    df.fastCSV("./test/fromcsv/dataset1.csv", 0, 1);
    // df.display();
    df.to_csv("./test/tocsv/ex5p1.csv", false);

    import std.stdio: File;
    File f1 = File("./test/fromcsv/dataset1.csv", "r");
    File f2 = File("./test/tocsv/ex5p1.csv", "r");

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

// fastCSV dataset1
unittest
{
    DataFrame!(int, 8, double, int, 4) df;
    df.fastCSV("./test/fromcsv/dataset1.csv", 1, 1);
    // df.display();
    df.to_csv("./test/tocsv/ex5p2.csv");

    import std.stdio: File;
    File f1 = File("./test/fromcsv/dataset1.csv", "r");
    File f2 = File("./test/tocsv/ex5p2.csv", "r");

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
    DataFrame!(int, double, 22) df;
    df.from_csv("./test/fromcsv/dataset2.csv", 2, 1);
    //df.display();
    df.to_csv!(4)("./test/tocsv/ex4p1.csv");

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

// fastCSV dataset2
unittest
{

    DataFrame!(double, 23) df;
    df.fastCSV("./test/fromcsv/dataset2.csv", 2, 1);
    // df.display();
    df.to_csv("./test/tocsv/ex5p3.csv");

    import std.stdio: File;
    import std.string: chomp;
    import std.array: split;

    File f1 = File("./test/fromcsv/dataset2.csv", "r");
    File f2 = File("./test/tocsv/ex5p3.csv", "r");

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

// Partially parsing the complete columns of dataset2
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
    index.setIndex([0, 1, 2, 3, 4, 5], ["Row Index"], [0, 1, 2], ["Column Index"]);
    df.setFrameIndex(index);
    // df.display();

    df.assign!1(2, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    // df.display();
    assert(df.data[2] == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);

    df.assign!1(1, [1, 2, 3]);
    // df.display();
    assert(df.data[1] == [1, 2, 3, 0, 0, 0]);

    df.assign!0(0, 4, 5, 1.6);
    // df.display();
    assert(df.data[0][0] == 4);
    assert(df.data[1][0] == 5);
    assert(df.data[2][0] == 1.6);

    index.extend!0([6]);
    df.setFrameIndex(index);
    // df.display();
    assert(df.rows == 7);
}

// ditto
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

// ditto
unittest
{
    Index inx;
    inx.setIndex([1, 2, 3],["rindex"]);

    DataFrame!(int, 2, double) df;
    df.setFrameIndex(inx);
    // df.display();
    df = [[1.0], [1.0, 2.0], [1.0, 2.0, 3.5]];
    assert(df.data[0] == [1, 1, 1]);
    assert(df.data[1] == [0, 2, 2]);
    assert(df.data[2][2] == 3.5);

    df[0, 0] = 42;
    df[["2"], ["1"]] = 17;
    // df.display();
    assert(df.data[0][0] == 42);
    assert(df.data[1][1] == 17);
}

// Apply
unittest
{
    Index inx;
    DataFrame!(double, 2) df;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["RL1", "RL2"],
                [["Hello", "Hi"], ["Hi", "Hello"]], ["CL1", "CL2"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1.0, 4.0]);
    df.assign!1(1, [16.0, 256.0]);
    // df.display();

    import std.math: sqrt;
    df.apply!(sqrt, 1)([1]);
    assert(df.data[1] == [4, 16]);

    df.apply!(sqrt, 1)([["Hi", "Hello"]]);
    assert(df.data[1] == [2, 4]);

    df.apply!(sqrt, 0)([1]);
    assert(df.data[0][1] == 2 && df.data[1][1] == 2);

    df.assign!0(1, 16.0, 16.0);
    df.apply!(sqrt, 0)([["Hi", "Hello"]]);
    assert(df.data[0][1] == 4 && df.data[1][1] == 4);

    df.assign!1(0, [16.0, 16.0]);
    df.assign!1(1, [16.0, 16.0]);
    // df.display();

    // Apply to the entire DataFrame
    df.apply!(sqrt)();
    assert(df.data[0] == [4, 4] && df.data[1] == [4, 4]);

    df.apply!(sqrt)();
    assert(df.data[0] == [2, 2] && df.data[1] == [2, 2]);
}

// Drop
unittest
{
    Index inx;
    DataFrame!(double, 2) df;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["RL1", "RL2"],
                [["Hello", "Hi"], ["Hi", "Hello"]], ["CL1", "CL2"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1.0, 4.0]);
    df.assign!1(1, [16.0, 256.0]);
    const string str1 = df.display(true, 200);


    // Dropping a row
    auto drow = df.drop!(0, [1]);
    assert(drow.rows == 1);
    assert(drow.data[0][0] == 1 && drow.data[1][0] == 16);
    assert(drow.display(true, 200) == "       CL1  Hello  Hi   \n"
        ~ "       CL2  Hi     Hello\n"
        ~ "RL1    RL2\n"
        ~ "Hello  Hi   1      16   \n"
    );

    // Checking if df is untouched
    assert(df.data[0] == [1.0, 4.0]);
    assert(df.data[1] == [16.0, 256.0]);
    const string str2 = df.display(true, 200);
    assert(str1 == str2);

    drow = df.drop!(0, [0]);
    assert(drow.rows == 1);
    assert(drow.data[0][0] == 4 && drow.data[1][0] == 256);
    assert(drow.display(true, 200) == "     CL1    Hello  Hi   \n"
        ~ "     CL2    Hi     Hello\n"
        ~ "RL1  RL2  \n"
        ~ "Hi   Hello  4      256  \n"
    );

    assert(df.data[0] == [1.0, 4.0]);
    assert(df.data[1] == [16.0, 256.0]);
    const string str3 = df.display(true, 200);
    assert(str1 == str3);

    auto dcol = df.drop!(1, [0]);
    assert(dcol.cols == 1);
    assert(dcol.data[0] == [16, 256]);
    assert(dcol.display(true, 200) == "       CL1    Hi   \n"
        ~ "       CL2    Hello\n"
        ~ "RL1    RL2  \n"
        ~ "Hello  Hi     16   \n"
        ~ "Hi     Hello  256  \n"
    );

    assert(df.data[0] == [1.0, 4.0]);
    assert(df.data[1] == [16.0, 256.0]);
    const string str4 = df.display(true, 200);
    assert(str1 == str4);

    dcol = df.drop!(1, [1]);
    assert(dcol.cols == 1);
    assert(dcol.data[0] == [1, 4]);
    assert(dcol.display(true, 200) == "       CL1    Hello\n"
        ~ "       CL2    Hi   \n"
        ~ "RL1    RL2  \n"
        ~ "Hello  Hi     1    \n"
        ~ "Hi     Hello  4    \n"
    );

    assert(df.data[0] == [1.0, 4.0]);
    assert(df.data[1] == [16.0, 256.0]);
    const string str5 = df.display(true, 200);
    assert(str1 == str5);
}

// Drop - multiple with heterogeneous
unittest
{
    Index inx;
    DataFrame!(int, double[2]) df;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"]], ["RL1", "RL2"],
                [["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"]], ["CL1", "CL2"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1, 4, 16]);
    df.assign!1(1, [16.0, 256.0, 225.0]);
    df.assign!1(2, [1.0, 4.0, 16.0]);
    const string str1 = df.display(true, 200);

    auto drows = df.drop!(0, [1, 2]);
    assert(drows.rows == 1);
    assert(drows.data[0][0] == 1 && drows.data[1][0] == 16 && drows.data[2][0] == 1);
    assert(drows.display(true, 200) == "       CL1  Hello  Hi     Hey\n"
        ~ "       CL2  Hi     Hello  Hey\n"
        ~ "RL1    RL2\n"
        ~ "Hello  Hi   1      16     1  \n"
    );

    const string str2 = df.display(true, 200);
    assert(str1 == str2);

    drows = df.drop!(0, [0, 2]);
    assert(drows.rows == 1);
    assert(drows.data[0][0] == 4 && drows.data[1][0] == 256 && drows.data[2][0] == 4);
    assert(drows.display(true, 200) == "     CL1    Hello  Hi     Hey\n"
        ~ "     CL2    Hi     Hello  Hey\n"
        ~ "RL1  RL2  \n"
        ~ "Hi   Hello  4      256    4  \n"
    );

    const string str3 = df.display(true, 200);
    assert(str1 == str3);

    auto dcols = df.drop!(1, [1, 2]);
    assert(dcols.cols == 1);
    assert(dcols.data[0] == [1, 4, 16]);
    assert(dcols.display(true, 200) == "       CL1    Hello\n"
        ~ "       CL2    Hi   \n"
        ~ "RL1    RL2  \n"
        ~ "Hello  Hi     1    \n"
        ~ "Hi     Hello  4    \n"
        ~ "Hey    Hey    16   \n"
    );

    const string str4 = df.display(true, 200);
    assert(str1 == str4);

    auto dcols2 = df.drop!(1, [0, 1]);
    assert(dcols2.cols == 1);
    assert(dcols2.data[0] == [1, 4, 16]);
    assert(dcols2.display(true, 200) == "       CL1    Hey\n"
        ~ "       CL2    Hey\n"
        ~ "RL1    RL2  \n"
        ~ "Hello  Hi     1  \n"
        ~ "Hi     Hello  4  \n"
        ~ "Hey    Hey    16 \n"
    );

    const string str5 = df.display(true, 200);
    assert(str1 == str5);
}

// columntoIndex
unittest
{
    Index inx;
    DataFrame!(double, 2) df;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["RL1", "RL2"],
                [["Hello", "Hi"], ["Hi", "Hello"]], ["CL1", "CL2"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1.0, 4.0]);
    df.assign!1(1, [16.0, 256.0]);
    const string str1 = df.display(true, 200);

    auto extended = df.columnToIndex!(0);
    assert(extended.indx.row.codes.length == 3);
    assert(extended.indx.row.index[2] == []);
    assert(extended.indx.row.codes[2] == [1, 4]);
    assert(extended.cols == 1);
    assert(extended.data[0] == [16, 256]);

    assert(extended.display(true, 200) == "              CL1  Hi   \n"
        ~ "              CL2  Hello\n"
        ~ "RL1    RL2    Hi \n"
        ~ "Hello  Hi     1    16   \n"
        ~ "Hi     Hello  4    256  \n"
    );

    const string str2 = df.display(true, 200);
    assert(str1 == str2);

    extended = df.columnToIndex!(1);
    assert(extended.indx.row.codes.length == 3);
    assert(extended.indx.row.index[2] == []);
    assert(extended.indx.row.codes[2] == [16, 256]);
    assert(extended.cols == 1);
    assert(extended.data[0] == [1, 4]);

    assert(extended.display(true, 200) == "              CL1    Hello\n"
        ~ "              CL2    Hi   \n"
        ~ "RL1    RL2    Hello\n"
        ~ "Hello  Hi     16     1    \n"
        ~ "Hi     Hello  256    4    \n"
    );

    const string str3 = df.display(true, 200);
    assert(str1 == str3);
}

// Converting a level of index to a Data level
unittest
{
    Index inx;
    DataFrame!(double, 2) df;
    inx.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["RL1", "RL2"],
                [["Hello", "Hi"], ["Hi", "Hello"]], ["CL1", "CL2"]);
    df.setFrameIndex(inx);
    // df.display();

    df.assign!1(0, [1.0, 4.0]);
    df.assign!1(1, [16.0, 256.0]);
    const string str1 = df.display(true, 200);

    auto extended = df.columnToIndex!(0);
    // Putting it back in place should give same DataFrame back
    const string str2 = extended.indexToData!(0, double)(2, ["Hello", "Hi"]).display(true, 200);

    // The final DataFrame should be same as the original one
    assert(str1 == str2);
}

// Gropby on DataFrame Struct - Part of xample ported over from group.d
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);
    // string str1 = df.display(true, 200);

    auto grp = df.groupBy!([2])([0, 1]);

    import magpie.group: Group;
    Group!(int, int, int, int) grpBy;
    grpBy.createGroup!([2])(df, [0, 1]);

    assert(grp.display(true, 200) == grpBy.display(true, 200));
}

// As a universal Slice
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    auto dfslice = df.asSlice!(int, Universal);
    assert(dfslice == [[0, 0, 1, 0, 1], [0, 0, 2, 0, 2], [0, 0, 3, 0, 3]]);
}

// As a canonical slice
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    auto dfslice = df.asSlice!(int, Canonical);
    assert(dfslice == [[0, 0, 1, 0, 1], [0, 0, 2, 0, 2], [0, 0, 3, 0, 3]]);
}

// As a contiguous slice
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    auto dfslice = df.asSlice!(int, Contiguous);
    assert(dfslice == [[0, 0, 1, 0, 1], [0, 0, 2, 0, 2], [0, 0, 3, 0, 3]]);
}

// Slice assignment
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    auto dfslice = df.asSlice!(int, Contiguous);
    
    DataFrame!(int, 5) df2;
    df2.setFrameIndex(inx);

    df2 = dfslice;
    assert(df.display(true, 200) == df2.display(true, 200));
}

// PArtial Slice Assignment
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    
    auto assn = slice!(int)(1, 1);
    assn[0][0] = 42;
    df = assn;

    assert(df.data[0][0] == 42);
}

// asSlice oveload
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    assert(df.asSlice!(Universal, int, 1)(["4"]) == [1, 2, 3]);
    assert(df.asSlice!(Universal)(["Hello", "Hi", "Hey"]) == ["0", "0", "1", "0", "1"]);
}

// Slice opIndexOpAssign
unittest
{
    DataFrame!(int, 5) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1,2,3]);

    df[["3"]] = df.asSlice!(Universal, int, 1)(["4"]);
    assert(df.asSlice!(Universal, int, 1)(["3"]) == [1, 2, 3]);

    df[["Hello", "Hi", "Hey"], 0] = df.asSlice!(Universal)(["Hi", "Hello", "Hello"]);
    assert(df.asSlice!(Universal)(["Hello", "Hi", "Hey"]) == ["0", "0", "2", "2", "2"]);
}

// Slice opIndexOpAssign - heterogeneous DataFrame
unittest
{
    DataFrame!(int, 3, double, 2) df;
    Index inx;
    inx.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hello", "Hi"]], ["1", "2", "3"]);
    df.setFrameIndex(inx);
    df.assign!1(2, [1,2,3]);
    df.assign!1(4, [1.0, 2.0, 3.0]);

    df[["1"]] = df.asSlice!(Universal, int, 1)(["4"]);
    assert(df.asSlice!(Universal, int, 1)(["1"]) == [1, 2, 3]);

    df[["Hello", "Hi", "Hey"], 0] = df.asSlice!(Universal)(["Hi", "Hello", "Hello"]);
    assert(df.asSlice!(Universal)(["Hello", "Hi", "Hey"]) == ["0", "2", "2", "nan", "2"]);
}

// Aggregate Operation on DataFrame Columns
unittest
{
    DataFrame!(int, 2, double, 2) df;
    Index inx;

    inx[0] = ["Row1", "Row2"];
    inx[1] = ["Col1", "Col2", "Col3", "Col4"];

    df.setFrameIndex(inx);
    df = [[1, 2, 3.4, 5.6], [2, 8, 7.9, 5.6]];
    // df.display();

    import std.algorithm: max, min;
    assert(df.aggregate!(1, max).display(true, 200) == "Operation  Col1  Col2  Col3  Col4\n"
        ~ "max        2     8     7.9   5.6 \n"
    );

    auto mindf = df.aggregate!(1, min);
    import std.math: approxEqual;
    static assert(is(mindf.RowType == AliasSeq!(int, int, double, double)));
    assert(mindf.data[0][0] == 1 && mindf.data[1][0] == 2);
    assert(approxEqual(mindf.data[2][0], 3.4, 1e-8) && approxEqual(mindf.data[3][0], 5.6, 1e-8));

    auto doubledf = df.aggregate!(1, max, min);
    static assert(is(doubledf.RowType == AliasSeq!(int, int, double, double)));
    assert(doubledf.data[0][0] == 2 && doubledf.data[1][0] == 8);
    assert(approxEqual(doubledf.data[2][0], 7.9, 1e-8) && approxEqual(doubledf.data[3][0], 5.6, 1e-8));
    assert(doubledf.data[0][1] == 1 && doubledf.data[1][1] == 2);
    assert(approxEqual(doubledf.data[2][1], 3.4, 1e-8) && approxEqual(doubledf.data[3][1], 5.6, 1e-8));
}

// Aggregate Operation on DataFrame Columns
unittest
{
    DataFrame!(int, 2, double, 2) df;
    Index inx;

    inx[0] = ["Row1", "Row2"];
    inx[1] = ["Col1", "Col2", "Col3", "Col4"];

    df.setFrameIndex(inx);
    df = [[1, 2, 3.4, 5.6], [2, 8, 7.9, 5.6]];
    // df.display();

    import std.algorithm: max, min;
    import std.math: approxEqual;

    auto maxdf = df.aggregate!(0, max);
    assert(approxEqual(maxdf.data[0][0], 5.6, 1e-8) && approxEqual(maxdf.data[0][1], 8, 1e-8));

    auto mindf = df.aggregate!(0, min);
    assert(approxEqual(mindf.data[0][0], 1, 1e-8) && approxEqual(mindf.data[0][1], 2, 1e-8));

    auto doubledf = df.aggregate!(0, min, max);
    assert(approxEqual(doubledf.data[0][0], 1, 1e-8) && approxEqual(doubledf.data[0][1], 2, 1e-8));
    assert(approxEqual(doubledf.data[1][0], 5.6, 1e-8) && approxEqual(doubledf.data[1][1], 8, 1e-8));
}

// Custom function passed to aggregate
unittest
{
    static auto customFunc(T)(T[] arr)
    {
        T res = 0;
        foreach(i, ele; arr)
            res += i * ele;
        
        return res;
    }

    DataFrame!(int, 2, double, 2) df;
    Index inx;

    inx[0] = ["Row1", "Row2"];
    inx[1] = ["Col1", "Col2", "Col3", "Col4"];

    df.setFrameIndex(inx);
    df = [[1, 2, 3.4, 5.6], [2, 8, 7.9, 5.6]];
    // df.display();

    import std.math: approxEqual;
    auto customdf = df.aggregate!(1, customFunc);
    assert(customdf.data[0][0] == 2 && customdf.data[1][0] == 8);
    assert(approxEqual(customdf.data[2][0], 7.9, 1e-8) && approxEqual(customdf.data[3][0], 5.6, 1e-8)); 
}

// Return Type optimization for Aggregate
unittest
{
    DataFrame!(int, 4) df;
    Index inx;

    inx[0] = ["Row1", "Row2"];
    inx[1] = ["Col1", "Col2", "Col3", "Col4"];

    df.setFrameIndex(inx);
    df = [[1, 2, 3, 5], [2, 8, 7, 6]];
    // df.display();

    import std.algorithm: max, min;
    import std.math: approxEqual;

    auto maxdf = df.aggregate!(0, max);
    static assert(is(maxdf.FrameType == int[][1]));
    assert(maxdf.data[0][0] == 5 && maxdf.data[0][1] == 8);

    auto maxdfc = df.aggregate!(1, max);
    static assert(is(maxdfc.FrameType == int[][4]));
    assert(maxdfc.data[0][0] == 2 && maxdfc.data[1][0] == 8 && maxdfc.data[2][0] == 7 && maxdfc.data[3][0] == 6);
}

// Filter on Heterogeneous DataFrame
unittest
{
    DataFrame!(double, float) df;
    Index inx;
    inx[0] = ["Firm1", "Firm2", "Firm3", "Firm4", "Firm5"];
    inx[1] = ["Assets", "Valuation"];
    df.setFrameIndex(inx);

    static bool filterFunc(T)(T ele)
    {
        return (ele[0] > ele[1]);
    }

    df = [[1.2, 2.3], [0.8, 1.2], [4.2, 1.2], [7.2, 9.4], [1.1, 0.5]];
    // df.display();
    assert(df.filter!(filterFunc).display(true, 200) == "       Assets  Valuation\n"
        ~ "Firm3  4.2     1.2      \n"
        ~ "Firm5  1.1     0.5      \n"
    );

    version(DMD)
    {
        assert(df.filter!(e => e[0] > e[1]).display(true, 200) == "       Assets  Valuation\n"
            ~ "Firm3  4.2     1.2      \n"
            ~ "Firm5  1.1     0.5      \n"
        );
    }
}

// Filter on Homogeneous DataFrame
unittest
{
    DataFrame!(float, float) df;
    Index inx;
    inx[0] = ["Firm1", "Firm2", "Firm3", "Firm4", "Firm5"];
    inx[1] = ["Assets", "Valuation"];
    df.setFrameIndex(inx);

    static bool filterFunc(T)(T ele)
    {
        return (ele[0] > ele[1]);
    }

    df = [[1.2, 2.3], [0.8, 1.2], [4.2, 1.2], [7.2, 9.4], [1.1, 0.5]];
    // df.display();
    assert(df.filter!(filterFunc).display(true, 200) == "       Assets  Valuation\n"
        ~ "Firm3  4.2     1.2      \n"
        ~ "Firm5  1.1     0.5      \n"
    );
}

// Pivot Operation
unittest
{
    DataFrame!(float, 3) df;
    Index inx;
    inx[0] = ["0", "1", "2", "3"];
    inx[1] = ["Foo", "Bar", "Baz"];
    df.setFrameIndex(inx);

    df = [[1,3,1],[1,3,2],[2,4,3],[2,4,4]];

    // Single Index
    assert(df.pivot!2([1],[0],[2]).display(true, 200) == "Index0  1  2\n"
        ~ "3       1  2\n"
        ~ "4       3  4\n"
    );

    // Multi-Index
    assert(df.pivot!4([1],[0, 1],[2, 1]).display(true, 200) == "        1  1  2  2\n"
        ~ "Index0  3  4  3  4\n"
        ~ "3       1  2  3  3\n"
        ~ "4       3  4  4  4\n"
    );
}

// Pivot Operation on heterogeneous DataFrame
unittest
{
    DataFrame!(int, double, 2) df;
    Index inx;
    inx[0] = ["0", "1", "2", "3"];
    inx[1] = ["Foo", "Bar", "Baz"];
    df.setFrameIndex(inx);

    df = [[1,3,1],[1,3,2],[2,4,3],[2,4,4]];

    // Single Index
    assert(df.pivot!2([1],[0],[2]).display(true, 200) == "Index0  1  2\n"
        ~ "3       1  2\n"
        ~ "4       3  4\n"
    );

    // Multi-Index
    assert(df.pivot!4([1],[0, 1],[2, 1]).display(true, 200) == "        1  1  2  2\n"
        ~ "Index0  3  4  3  4\n"
        ~ "3       1  2  3  3\n"
        ~ "4       3  4  4  4\n"
    );
}
