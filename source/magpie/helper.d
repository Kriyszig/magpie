module magpie.helper;

import magpie.index: Index;

import std.meta: AliasSeq, Repeat;
import std.range.primitives: ElementType;
import std.traits: isStaticArray;

/// Build DataFrame argument list
template getArgsList(args...)
{
    static if(args.length)
    {
        alias arg = args[0];
        import std.traits: isType;
        static if(args.length == 1)
        {
            static if(isStaticArray!(arg))
                alias getArgsList = AliasSeq!(Repeat!(arg.length, ElementType!(arg)));
            else
                alias getArgsList = AliasSeq!(arg);
        }
        else
        {
            static if(isType!(args[1]))
            {
                static if(isStaticArray!(arg))
                    alias getArgsList = AliasSeq!(Repeat!(arg.length, ElementType!(arg)), getArgsList!(args[1 .. $]));
                else
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

private template dropper_internal(int[] pos, int rem, Types...)
{
    static if(pos.length == 0)
    {
        alias dropper_internal = Types;
    }
    else
    {
        alias dropper_internal = AliasSeq!(Types[0 .. pos[0] - rem],
            dropper_internal!(pos[1 .. $], pos[0] + 1 ,Types[pos[0] - rem + 1 .. $]));
    }
}

/// Template to evaluate RowType - removes elements in Types according to the positions in pos. pos must be ascending
template dropper(int[] pos, Types...)
{
    alias dropper = dropper_internal!(pos, 0, Types);
}

/// drops values at a set of position from given array
T[] dropper(T)(int[] pos, T[] values)
{
    T[] dropper_internal(T)(int[] pos, int rem, T[] values)
    {
        if(pos.length == 0)
            return values;
        else
            return values[0 .. pos[0] - rem] ~ dropper_internal(pos[1 .. $], pos[0] + 1, values[pos[0] - rem + 1 .. $]);
    }

    return dropper_internal(pos, 0, values);
}

/// Function to sort indexes in ascending order and switch the code to keep the effective positions same
void sortIndex(string[] index, int[] codes)
{
    foreach(i; 0 .. cast(uint)index.length)
    {
        uint pos = i;
        foreach(j; i + 1 .. cast(uint)index.length)
        {
            if(index[pos] > index[j])
            {
                pos = j;
            }
        }

        if(i == pos)
            continue;

        string tmp = index[i];
        index[i] = index[pos];
        index[pos] = tmp;

        foreach(j; 0 .. codes.length)
        {
            if(codes[j] == pos)
                codes[j] = i;
            else if(codes[j] == i)
                codes[j] = pos;
        }
    }
}

/// Does exactly what generateCode does in Index but for any general ndarray
int[] vectorize(T)(T[] values)
{
    int[T] elementPos;
    int[] pos;
    int totalUnique = 0;

    foreach(i; values)
    {
        ++pos.length;
        pos[$ - 1] = elementPos.require(cast(immutable)i, { return totalUnique++; }());
    }

    return totalUnique ~ pos;
}

/// Transposes an array of integer
T[][] transposed(T)(T[][] data)
{
    T[][] ret;
    ret.length = data[0].length;
    foreach(i, eleu; data)
    {
        foreach(j, elel; eleu)
        {
            ++ret[j].length;
            ret[j][i] = elel;
        }
    }

    return ret;
}

/// Template to get array from type
alias toArr(T) = T[];

/// Get unique indexes for merge
string[][] ensureUnique(int axis)(Index i1, Index i2, string lsuffix = "x_", string rsuffix = "y_")
{
    assert(i1.indexing[axis].codes.length == i2.indexing[axis].codes.length, "Index level mismatch for union");
    
    import std.algorithm: countUntil;
    string[][] unique;

    unique.length = i1.indexing[axis].codes[0].length + i1.indexing[axis].codes[0].length;

    foreach(i; 0 .. i1.indexing[axis].codes[0].length)
    {
        string[] indx;
        indx.length = i1.indexing[axis].codes.length;

        foreach(j; 0 .. i1.indexing[axis].codes.length)
        {
            if(i1.indexing[axis].index[j].length == 0)
            {
                import std.conv: to;
                indx[j] = to!string(i1.indexing[axis].codes[j][i]);
            }
            else
                indx[j] = i1.indexing[axis].index[j][i1.indexing[axis].codes[j][i]];
        }

        // Making an asuumption that there is no conflicting indexes within the same Index struct
        unique[i] = indx;
    }

    foreach(i; 0 .. i2.indexing[axis].codes[0].length)
    {
        string[] indx;
        indx.length = i2.indexing[axis].codes.length;

        foreach(j; 0 .. i2.indexing[axis].codes.length)
        {
            if(i2.indexing[axis].index[j].length == 0)
            {
                import std.conv: to;
                indx[j] = to!string(i2.indexing[axis].codes[j][i]);
            }
            else
                indx[j] = i2.indexing[axis].index[j][i2.indexing[axis].codes[j][i]];
        }

        // Making an asuumption that there is no conflicting indexes within the same Index struct
        const int p = cast(int)countUntil(unique, indx);
        if(p < 0)
            unique[i + i1.indexing[axis].codes[0].length] = indx;
        else
        {
            foreach(j; 0 .. unique[p].length)
                unique[p][j] = lsuffix ~ unique[p][j];

            foreach(j; 0 .. indx.length)
                indx[j] = rsuffix ~ indx[j];

            unique[i + i1.indexing[axis].codes[0].length] = indx;
        }
    }

    return unique;
}

/// Returns the union of indexes in the order they appear
string[][] indexUnion(int axis)(Index i1, Index i2)
{
    assert(i1.indexing[axis].codes.length == i2.indexing[axis].codes.length, "Index level mismatch for union");
    
    import std.algorithm: countUntil;
    string[][] unique;

    foreach(i; 0 .. i1.indexing[axis].codes[0].length)
    {
        string[] indx;
        indx.length = i1.indexing[axis].codes.length;

        foreach(j; 0 .. i1.indexing[axis].codes.length)
        {
            if(i1.indexing[axis].index[j].length == 0)
            {
                import std.conv: to;
                indx[j] = to!string(i1.indexing[axis].codes[j][i]);
            }
            else
                indx[j] = i1.indexing[axis].index[j][i1.indexing[axis].codes[j][i]];
        }

        // Making an asuumption that there is no conflicting indexes within the same Index struct
        unique ~= indx;
    }

    foreach(i; 0 .. i2.indexing[axis].codes[0].length)
    {
        string[] indx;
        indx.length = i2.indexing[axis].codes.length;

        foreach(j; 0 .. i2.indexing[axis].codes.length)
        {
            if(i2.indexing[axis].index[j].length == 0)
            {
                import std.conv: to;
                indx[j] = to!string(i2.indexing[axis].codes[j][i]);
            }
            else
                indx[j] = i2.indexing[axis].index[j][i2.indexing[axis].codes[j][i]];
        }

        // Making an asuumption that there is no conflicting indexes within the same Index struct
        const int p = cast(int)countUntil(unique, indx);
        if(p < 0)
            unique ~= indx;
    }

    return unique;
}

/// find set of indexes that occur in both structures
string[][] indexIntersection(int axis)(Index i1, Index i2)
{
    assert(i1.indexing[axis].codes.length == i2.indexing[axis].codes.length, "Index level mismatch for union");
    
    import std.algorithm: countUntil;
    string[][] intersect;

    foreach(i; 0 .. i1.indexing[axis].codes[0].length)
    {
        string[] indx;
        indx.length = i1.indexing[axis].codes.length;

        foreach(j; 0 .. i1.indexing[axis].codes.length)
        {
            if(i1.indexing[axis].index[j].length == 0)
            {
                import std.conv: to;
                indx[j] = to!string(i1.indexing[axis].codes[j][i]);
            }
            else
                indx[j] = i1.indexing[axis].index[j][i1.indexing[axis].codes[j][i]];
        }

        // Making an asuumption that there is no conflicting indexes within the same Index struct
        if(i2.getPosition!(axis)(indx) > -1)
            intersect ~= indx;
    }

    return intersect;
}

// Community suggested way ot intialize a DataFrame
unittest
{
    assert(is(getArgsList!(int[2]) == AliasSeq!(int, int)));
    assert(is(getArgsList!(int[2], double[3]) == AliasSeq!(int, int, double, double, double)));
    assert(is(getArgsList!(int[1], double[2], int, 2) == AliasSeq!(int, double, double, int, int)));
    assert(is(getArgsList!(int, int, double[2], int, 2) == AliasSeq!(int, int, double, double, int, int)));
}

// Sorting indexes and codes by keeping their effet position same
unittest
{
    string[] indx = ["b", "c", "a", "d"];
    int[] codes = [0, 1, 2, 3];
    sortIndex(indx, codes);
    assert(indx == ["a", "b", "c", "d"]);
    assert(codes == [1, 2, 0, 3]);
}

// Testing dropper
unittest
{
    assert(is(dropper!([1, 4], int, long, int, long, byte, float, bool) == AliasSeq!(int, int, long, float, bool)));
    assert(is(dropper!([0, 4], int, long, int, long, double) == AliasSeq!(long, int, long)));
    assert(is(dropper!([1, 3, 5], int, long, int, long, double, float, bool) == AliasSeq!(int, int, double, bool)));
    assert(is(dropper!([0, 2, 3, 5], int, uint, byte, ubyte, long, ulong, bool) == AliasSeq!(uint, long, bool)));
}

// Testing dropper for arrays
unittest
{
    assert(dropper([1, 4], [1, 2, 3, 4, 5, 6]) == [1, 3, 4, 6]);
    assert(dropper([0, 5], [1, 2, 3, 4, 5, 6]) == [2, 3, 4, 5]);
    assert(dropper([0, 3, 5], [1, 2, 3, 4, 5, 6]) == [2, 3, 5]);
}

// Vectorize test
unittest
{
    double[] arr = [1.2, 2.7, 1.2, 5.6, 1.2, 5.6];
    auto varr = vectorize(arr);
    assert(varr.length == arr.length + 1);
    assert(varr[0] == 3);
    assert(varr[1 .. $] == [0, 1, 0, 2, 0, 2]);

    string[] arrs = ["Hello", "Hi", "Hello", "Hi"];
    auto varrs = vectorize(arrs);
    assert(varrs.length == arrs.length + 1);
    assert(varrs[0] == 2);
    assert(varrs[1 .. $] == [0, 1, 0, 1]);

    int[][] arr2d = [[1, 2], [2, 4], [1, 2], [3, 6]];
    auto varr2d = vectorize(arr2d);
    assert(varr2d.length == arr2d.length + 1);
    assert(varr2d[0] == 3);
    assert(varr2d[1 .. $] == [0, 1, 0, 2]);
}

// Transposed test
unittest
{
    int[][] a = [[1, 2, 3], [4, 5, 6]];
    assert(transposed(a) == [[1, 4], [2, 5], [3, 6]]);

    int[][] b = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];
    assert(transposed(b) == [[1, 4, 7], [2, 5, 8], [3, 6, 9]]);
}

// Ensuring unque indexes on merge
unittest
{
    Index i1;
    Index i2;

    i1.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["1", "@"]);
    i2.setIndex([["Hey", "Yo"], ["Yo", "Hey"]], ["!", "2"]);

    assert(ensureUnique!0(i1, i2) == [["Hello", "Hi"], ["Hi", "Hello"], ["Hey", "Yo"], ["Yo", "Hey"]]);
    
    i2.setIndex([["Hey", "Hello"], ["Hi", "Hi"]], ["!", "@"]);
    assert(ensureUnique!0(i1, i2) == [["x_Hello", "x_Hi"], ["Hi", "Hello"], ["Hey", "Hi"], ["y_Hello", "y_Hi"]]);
}

// Index union
unittest
{
    Index i1;
    Index i2;

    i1.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["1", "@"]);
    i2.setIndex([["Hi", "Hello"], ["Yo", "Hi"]], ["!", "2"]);

    assert(indexUnion!0(i1, i2).length == 3);
    assert(indexUnion!0(i1, i2) == [["Hello", "Hi"], ["Hi", "Hello"], ["Hi", "Yo"]]);
}

// Index Intersection
unittest
{
    Index i1;
    Index i2;

    i1.setIndex([["Hello", "Hi"], ["Hi", "Hello"]], ["1", "@"]);
    i2.setIndex([["Hi", "Hello"], ["Yo", "Hi"]], ["!", "2"]);

    assert(indexIntersection!0(i1, i2) == [["Hello", "Hi"]]);
}

// Index Intersection
unittest
{
    Index i1;
    Index i2;

    i1.setIndex([["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hey"], ["Hey", "Hi", "Hello"]], ["1", "@"]);
    i2.setIndex([["Hi", "Hello"], ["Hello", "Hi"], ["Hi", "Hey"]], ["!", "2"]);

    assert(indexIntersection!0(i1, i2) == [["Hello", "Hi", "Hey"], ["Hi", "Hello", "Hi"]]);
}
