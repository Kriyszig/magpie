module magpie.frame;

import mir.ndslice;

struct Index
{
    bool isMultiIndexed = false;
    
    string[] rIndexTitles;
    string[][]  rIndices;
    int[][] rCodes;

    string[] cIndexTitles;
    string[][] cIndices;
    int[][] cCodes;

}

struct DataFrame(T)
{
    Slice!(T*, 2, Universal) data;
    Index frameIndex;

public:
    void display()
    {
        import std.stdio: write, writeln;
        if(data.shape[0] == 0 || data.shape[1] == 0)
        {
            writeln("The DataFrame is empty");
        }

        import std.conv;
        immutable int maxColSize = 45;
        immutable int terminalWidth = 200;
        immutable ulong rindexDepth = frameIndex.rCodes.length;
        immutable ulong cindexDepth = frameIndex.cCodes.length;
        ulong[] colSize;

        ulong totalWidth = 0u; 
        for(int i = 0; i < rindexDepth; ++i)
        {
            ulong maxGap = frameIndex.rIndexTitles[i].length;
            for(int j = 0; j < frameIndex.rCodes[i].length; ++j)
            {
                if(frameIndex.rIndices[i].length == 0)
                {
                    if(maxGap < to!string(frameIndex.rCodes[i][j]).length)
                    {
                        maxGap = to!string(frameIndex.rCodes[i][j]).length;
                    }
                }
                else
                {
                    if(maxGap < frameIndex.rIndices[i][frameIndex.rCodes[i][j]].length)
                    {
                        maxGap = frameIndex.rIndices[i][frameIndex.rCodes[i][j]].length;
                    }
                }

                if(maxGap >= maxColSize)
                {
                    maxGap = maxColSize;
                    break;
                }
            }

            colSize ~= [maxGap];
            totalWidth += maxGap;
        }

        if(frameIndex.cIndexTitles.length != 0)
        {
            ulong maxGap = colSize[colSize.length - 1];
            totalWidth -= maxGap;
            for(int i = 0; i < frameIndex.cIndexTitles.length; ++i)
            {
                if(maxGap < frameIndex.cIndexTitles[i].length)
                {
                    maxGap = frameIndex.cIndexTitles[i].length;
                }

                if(maxGap >= maxColSize)
                {
                    maxGap = maxColSize;
                    break;
                }
            }

            colSize[colSize.length - 1] = maxGap;
            totalWidth += maxGap;
        }

        for(int i = 0; i < data.shape[1]; ++i)
        {
            ulong maxGap = 0;
            for(int j = 0; j < cindexDepth; ++j)
            {
                if(frameIndex.cIndices[j].length == 0)
                {
                    if(maxGap < to!string(frameIndex.cCodes[j][i]).length)
                    {
                        maxGap = to!string(frameIndex.cCodes[j][i]).length;   
                    }
                }
                else
                {
                    if(maxGap < frameIndex.cIndices[j][frameIndex.cCodes[j][i]].length)
                    {
                        maxGap = frameIndex.cIndices[j][frameIndex.cCodes[j][i]].length;
                    }
                }
            }

            if(maxGap >= maxColSize)
            {
                maxGap = maxColSize;
            }

            for(int j = 0 ; j < data.shape[0]; ++j)
            {
                if(maxGap < to!string(data[j][i]).length)
                {
                    maxGap = to!string(data[j][i]).length;
                }

                if(maxGap >= maxColSize)
                {
                    maxGap = maxColSize;
                    break;
                }
            }

            colSize ~= maxGap;
            totalWidth += maxGap;
        }

        int forward = 0;
        int backward = 1;
        int sum = 0;
        for(int i = 0; i < colSize.length; ++i)
        {
            if(sum + colSize[i] + 2 >= terminalWidth / 2)
            {
                break;
            }
            ++forward;
            sum += colSize[i] + 2;
        }

        sum = 0;
        for(int i = to!int(colSize.length) - 1; i > -1; --i)
        {
            if(sum + colSize[i] + 5 >= terminalWidth / 2)
            {
                break;
            }
            ++backward;
            sum += colSize[i] + 2;
        }

        if(backward > 1)
        {
            --backward;
        }

        if(backward + forward > colSize.length)
        {
            forward = to!int(colSize.length);
            backward = 0;
        }

        int dataIndex = 0;
        for(int i = 0; i < (frameIndex.cCodes.length + data.shape[0] + ((frameIndex.cIndexTitles.length > 0)?1: 0)); ++i)
        {
            if(i < frameIndex.cCodes.length)
            {
                for(int j = 0; j < forward; ++j)
                {
                    if(j < frameIndex.rCodes.length - 1)
                    {
                        if(i == frameIndex.cCodes.length - 1 && frameIndex.cIndexTitles.length == 0)
                        {
                            if(frameIndex.rIndexTitles[j].length > colSize[j])
                            {
                                write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3],"...  ");
                            }
                            else
                            {
                                write(frameIndex.rIndexTitles[j]);
                                for(ulong  k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                        else
                        {
                            for(int k = 0; k < colSize[j] + 2; ++k)
                            {
                                write(" ");
                            }
                        }
                    }
                    else if(j == frameIndex.rCodes.length - 1)
                    {
                        if(frameIndex.cIndexTitles.length != 0)
                        {
                            immutable ulong len = frameIndex.cIndexTitles[i].length;
                            if(len > maxColSize)
                            {
                                write(frameIndex.cIndexTitles[i][0 .. maxColSize - 3], "...  ");
                            }
                            else
                            {
                                write(frameIndex.cIndexTitles[i]);
                                for(ulong k = frameIndex.cIndexTitles[i].length; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                        else
                        {
                            if(i == frameIndex.cCodes.length - 1)
                            {
                                if(frameIndex.rIndexTitles[j].length > colSize[j])
                                {
                                    write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3],"...  ");
                                }
                                else
                                {
                                    write(frameIndex.rIndexTitles[j]);
                                    for(ulong  k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                    {
                                        write(" ");
                                    }
                                }
                            }
                            else
                            {
                                for(int k = 0; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                    }
                    else
                    {
                        string index2disp;
                        if(frameIndex.cIndices[i].length == 0)
                        {
                            index2disp = to!string(frameIndex.cCodes[i][j - frameIndex.rCodes.length]);
                        }
                        else
                        {
                            index2disp = frameIndex.cIndices[i][frameIndex.cCodes[i][j - frameIndex.rCodes.length]];
                        }
                        if(index2disp.length > colSize[j])
                        {
                            index2disp = index2disp[0 .. colSize[j] - 3] ~ "...";
                        }
                        write(index2disp);
                        for(ulong k = index2disp.length; k < colSize[j] + 2; ++k)
                        {
                            write(" ");
                        }
                    }

                }

                if(backward > 0)
                {
                    write("...  ");
                }

                for(ulong j = colSize.length - backward; j < colSize.length; ++j)
                {
                   if(j < frameIndex.rCodes.length - 1)
                    {
                        if(i == frameIndex.cCodes.length - 1 && frameIndex.cIndexTitles.length == 0)
                        {
                            if(frameIndex.rIndexTitles[j].length > colSize[j])
                            {
                                write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3],"...  ");
                            }
                            else
                            {
                                write(frameIndex.rIndexTitles[j]);
                                for(ulong  k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                        else
                        {
                            for(int k = 0; k < colSize[j] + 2; ++k)
                            {
                                write(" ");
                            }
                        }
                    }
                    else if(j == frameIndex.rCodes.length - 1)
                    {
                        if(frameIndex.cIndexTitles.length != 0)
                        {
                            immutable ulong len = frameIndex.cIndexTitles[i].length;
                            if(len > maxColSize)
                            {
                                write(frameIndex.cIndexTitles[i][0 .. maxColSize - 3], "...  ");
                            }
                            else
                            {
                                write(frameIndex.cIndexTitles[i]);
                                for(ulong k = frameIndex.cIndexTitles[i].length; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                        else
                        {
                            if(i == frameIndex.cCodes.length - 1)
                            {
                                if(frameIndex.rIndexTitles[j].length > colSize[j])
                                {
                                    write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3],"...  ");
                                }
                                else
                                {
                                    write(frameIndex.rIndexTitles[j]);
                                    for(ulong  k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                                    {
                                        write(" ");
                                    }
                                }
                            }
                            else
                            {
                                for(int k = 0; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                    }
                    else
                    {
                        string index2disp;
                        if(frameIndex.cIndices[i].length == 0)
                        {
                            index2disp = to!string(frameIndex.cCodes[i][j - frameIndex.rCodes.length]);
                        }
                        else
                        {
                            index2disp = frameIndex.cIndices[i][frameIndex.cCodes[i][j - frameIndex.rCodes.length]];
                        }
                        if(index2disp.length > colSize[j])
                        {
                            index2disp = index2disp[0 .. colSize[j] - 3] ~ "...";
                        }
                        write(index2disp);
                        for(ulong k = index2disp.length; k < colSize[j] + 2; ++k)
                        {
                            write(" ");
                        }
                    }
                }
                write("\n");
            }
            else if(i == frameIndex.cCodes.length && frameIndex.cIndexTitles.length != 0)
            {
                for(int j = 0;j < frameIndex.rCodes.length; ++j)
                {
                    if(frameIndex.rIndexTitles[j].length > colSize[j])
                    {
                        write(frameIndex.rIndexTitles[j][0 .. colSize[j] - 3], "...  ");
                    }
                    else
                    {
                        write(frameIndex.rIndexTitles[j]);
                        for(ulong k = frameIndex.rIndexTitles[j].length; k < colSize[j] + 2; ++k)
                        {
                            write(" ");
                        }
                    }
                }
                write("\n");
            }
            else
            {
                for(int j = 0; j < forward; ++j)
                {
                    if(j < frameIndex.rCodes.length)
                    {
                        if(frameIndex.isMultiIndexed == true && dataIndex > 0 && j < frameIndex.rCodes.length - 1
                            && frameIndex.rCodes[j][dataIndex] == frameIndex.rCodes[j][dataIndex - 1])
                        {
                            for(int k = 0; k < colSize[j] + 2; ++k)
                            {
                                write(" ");
                            }
                        }
                        else
                        {
                            string dispstr;
                            if(frameIndex.rIndices[j].length == 0)
                            {
                                dispstr = to!string(frameIndex.rCodes[j][dataIndex]);
                            }
                            else
                            {
                                dispstr = frameIndex.rIndices[j][frameIndex.rCodes[j][dataIndex]];
                            }

                            if(dispstr.length > colSize[j])
                            {
                                write(dispstr[0 .. colSize[j] - 3], "...  ");
                            }
                            else
                            {
                                write(dispstr);
                                for(ulong k = dispstr.length; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                    }
                    else
                    {
                        string dispstr = to!string(data[dataIndex][j - frameIndex.rCodes.length]);
                        if(dispstr.length > colSize[j])
                        {
                            write(dispstr[0 .. colSize[j] - 3], "...  ");
                        }
                        else
                        {
                            write(dispstr);
                            for(ulong k = dispstr.length; k < colSize[j] + 2; ++k)
                            {
                                write(" ");
                            }
                        }
                    }
                }

                if(backward > 0)
                {
                    write("...  ");
                }

                for(ulong j = colSize.length - backward; j < colSize.length; ++j)
                {
                    if(j < frameIndex.rCodes.length)
                    {
                        if(frameIndex.isMultiIndexed == true && dataIndex > 0 && j < frameIndex.rCodes.length - 1
                            && frameIndex.rCodes[j][dataIndex] == frameIndex.rCodes[j][dataIndex - 1])
                        {
                            for(int k = 0; k < colSize[j] + 2; ++k)
                            {
                                write(" ");
                            }
                        }
                        else
                        {
                            string dispstr;
                            if(frameIndex.rIndices[j].length == 0)
                            {
                                dispstr = to!string(dataIndex);
                            }
                            else
                            {
                                dispstr = frameIndex.rIndices[j][frameIndex.rCodes[j][dataIndex]];
                            }

                            if(dispstr.length > colSize[j])
                            {
                                write(dispstr[0 .. colSize[j] - 3], "...  ");
                            }
                            else
                            {
                                write(dispstr);
                                for(ulong k = dispstr.length; k < colSize[j] + 2; ++k)
                                {
                                    write(" ");
                                }
                            }
                        }
                    }
                    else
                    {
                        string dispstr = to!string(data[dataIndex][j - frameIndex.rCodes.length]);
                        if(dispstr.length > colSize[j])
                        {
                            write(dispstr[0 .. colSize[j] - 3], "...  ");
                        }
                        else
                        {
                            write(dispstr);
                            for(ulong k = dispstr.length; k < colSize[j] + 2; ++k)
                            {
                                write(" ");
                            }
                        }
                    }
                }
                ++dataIndex;
                write("\n");
            }
        }
        /*
         * writeln(colSize);
         * writeln(forward, "\t", backward);
         * writeln(data);
         */
    }
}
