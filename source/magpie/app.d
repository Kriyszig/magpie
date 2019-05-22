import mir.ndslice;
import std.stdio;
import magpie.frame: DataFrame;

void main()
{
    writeln("Example 1: Empty Dataframe");
    DataFrame!double empty;
    empty.display();

    writeln("\nExample 2: Simple Data Frame");
    DataFrame!double simpleEx;
    simpleEx.frameIndex.rIndexTitles = ["Index"];
    simpleEx.frameIndex.rCodes = [[0,1,2,3]];
    simpleEx.frameIndex.rIndices = [[]];
    simpleEx.frameIndex.cCodes = [[0,1,2,3]];
    simpleEx.frameIndex.cIndices = [[]];
    simpleEx.data = (new double(16)).sliced(4,4).universal;
    simpleEx.display();

    writeln("\nExample 3: Larger than terminal");
    DataFrame!double largeEx;
    largeEx.frameIndex.rIndexTitles = ["Index"];
    largeEx.frameIndex.rCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    largeEx.frameIndex.rIndices = [[]];
    largeEx.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    largeEx.frameIndex.cIndices = [[]];
    largeEx.data = (new double(225)).sliced(15,15).universal;
    largeEx.display();

    writeln("\nExample 4: Dataframe with both row and column index titles");
    DataFrame!double both;
    both.frameIndex.rIndexTitles = ["Index"];
    both.frameIndex.rCodes = [[0,1,2,3]];
    both.frameIndex.rIndices = [[]];
    both.frameIndex.cIndexTitles = ["Column Index:"];
    both.frameIndex.cCodes = [[0,1,2,3]];
    both.frameIndex.cIndices = [[]];
    both.data = (new double(16)).sliced(4,4).universal;
    both.display();


    writeln("\nExample 5: Multi-Indexed Rows");
    DataFrame!double mirows;
    mirows.frameIndex.rIndexTitles = ["Index1", "Index2"];
    mirows.frameIndex.rCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14],[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mirows.frameIndex.rIndices = [[], []];
    mirows.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mirows.frameIndex.cIndices = [[]];
    mirows.data = (new double(225)).sliced(15,15).universal;
    mirows.display();

    writeln("\nExample 6: Multi Indexed Columns");
    DataFrame!double mic;
    mic.frameIndex.rIndexTitles = ["Index1"];
    mic.frameIndex.rCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mic.frameIndex.rIndices = [[], []];
    mic.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14],[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mic.frameIndex.cIndices = [[],[]];
    mic.data = (new double(225)).sliced(15,15).universal;
    mic.display();

    writeln("\nExample 7: Multi Indexed Columns with column index titles");
    DataFrame!double mict;
    mict.frameIndex.rIndexTitles = ["Index1"];
    mict.frameIndex.rCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mict.frameIndex.rIndices = [[], []];
    mict.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14],[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    mict.frameIndex.cIndexTitles = ["CIndex1:", "Cindex2:"];
    mict.frameIndex.cIndices = [[],[]];
    mict.data = (new double(225)).sliced(15,15).universal;
    mict.display();

    writeln("\nExample 8: Maximum Column Size");
    simpleEx.frameIndex.rIndexTitles = ["IndexIndexIndexIndexIndexIndexIndexIndexIndexIndexIndexIndex"];
    simpleEx.frameIndex.rCodes = [[0,1,2,3]];
    simpleEx.frameIndex.rIndices = [[]];
    simpleEx.frameIndex.cCodes = [[0,1,2,3]];
    simpleEx.frameIndex.cIndices = [[]];
    simpleEx.data = (new double(16)).sliced(4,4).universal;
    simpleEx.display();

    writeln("\nExample 9: Multi-Indexed Rows and Columns");
    DataFrame!double ex1;
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,2,3,0],[1,2,3,5_555_555]];
    //ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,1,2,3],[0,1,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    ex1.data = (new double(16)).sliced(4,4).universal;
    ex1.display();

    writeln("\nExample 10: Multi-Indexed Rows and Columns with both row and colun index title");
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,2,3,0],[1,2,3,5_555_555]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,1,2,3],[0,1,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    ex1.data = (new double(16)).sliced(4,4).universal;
    ex1.display();

    writeln("\nExample 11: Multi-Indexed Rows and Columns skipping same index in rows index");
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,1,0,0],[1,2,3,5_555_555]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,1,2,3],[0,1,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    ex1.data = (new double(16)).sliced(4,4).universal;
    ex1.display();

    writeln("\nExample 12: Multi-Indexed Rows and Columns skipping same index in column index");
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,1,0,0],[1,2,3,5_555_555]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,0,2,3],[0,1,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    ex1.data = (new double(16)).sliced(4,4).universal;
    ex1.display();

    writeln("\nExample 13: Multi-Indexed Rows and Columns - skipping doesn't happen for the innermost index");
    // Illegal example - Here two sets of values can be inferred from same pair of index
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["Index", "Index2"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[]];
    ex1.frameIndex.rCodes = [[1,1,0,0],[1,1,3,5_555_555]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,0,2,3],[0,0,2,3]];
    ex1.frameIndex.cIndices = [["d","d lang","d programming lang","C+++"],["d","d lang","d programming lang","C+++"]];
    ex1.data = (new double(16)).sliced(4,4).universal;
    ex1.display();

    writeln("\nExample 14: Long DataFrame");
    largeEx.frameIndex.rIndexTitles = ["Index"];
    largeEx.frameIndex.rCodes = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
    53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63]];
    largeEx.frameIndex.rIndices = [[]];
    largeEx.frameIndex.cCodes = [[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]];
    largeEx.frameIndex.cIndices = [[]];
    largeEx.data = (new double(960)).sliced(64,15).universal;
    largeEx.display();

    writeln("\nExample 15: Unnecessarily long indexing");
    ex1.frameIndex.isMultiIndexed = true;
    ex1.frameIndex.rIndexTitles = ["IndexIndexIndexIndexIndexIndex", "IndexIndexIndexIndexIndexIndexIndex2",
    "IndexIndexIndexIndexIndexIndex3", "IndexIndexIndexIndexIndexIndex4", "Index5"];
    ex1.frameIndex.rIndices = [["yo","yoloy", "danndo", "jjjjjjjjjj"],[],[],[],[]];
    ex1.frameIndex.rCodes = [[1,1,0,0],[1,2,3,5_555_555],[1,2,3,4],[1,2,3,4],[1,2,3,4]];
    ex1.frameIndex.cIndexTitles = ["Language", "Language Again"];
    ex1.frameIndex.cCodes = [[0,0],[0,1]];
    ex1.frameIndex.cIndices = [["d"],["d programming lang","C+++"]];
    ex1.data = (new double(8)).sliced(4,2).universal;
    ex1.display();
}