import mir.ndslice;
import std.stdio;
import std.conv;

import magpie.frame: DataFrame;

void main()
{
	Slice!(double*, 3, Universal) s = (new double[24]).sliced(2,3,4).universal;
    //s.writeln;
    s = (new double[36]).sliced(3,3,4).universal;
    s[1,1,1] = 4;
    //s.writeln;
    float b;
    //writeln(to!string(b));
    Slice!(char**, 3, Universal) sdf;
    auto a = new double[1_000_000];
    double j = 10.63;
    for(int i = 0; i < 100000; i += 1000)
    {
        a[i] = j;
        j += 100.467;
    }
    Slice!(double*, 2, Universal) k = (a).sliced(1000,1000).universal;
    DataFrame!double d;
    d.frameIndex.rCodes = [[1,2,3,0],[1,2,3,5555555]];
    d.frameIndex.isMultiIndexed = true;
    d.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    d.frameIndex.rIndexTitles = ["Index", "Index2"];
    d.frameIndex.cIndexTitles = ["DangDangg", "DangDangDangDang"];
    d.frameIndex.cCodes = [[0,1,2,3],[0,1,2,3]];
    d.frameIndex.cIndices = [["d", "d lang","d programming lang", "C+++"],
                            ["d", "d lang","d programming lang", "C+++"]];
    d.data = (new double(16)).sliced(4,4).universal;
    d.display();
}
