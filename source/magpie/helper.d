module magpie.helper;

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
int[][] transposed(int[][] data)
{
    int[][] ret;
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

unittest
{
    int[][] a = [[1, 2, 3], [4, 5, 6]];
    assert(transposed(a) == [[1, 4], [2, 5], [3, 6]]);

    int[][] b = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];
    assert(transposed(b) == [[1, 4, 7], [2, 5, 8], [3, 6, 9]]);
}
