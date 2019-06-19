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

/// Template to evaluate RowType when column is dropped
template dropper(int[] pos, int rem, Types...)
{
    static if(pos.length == 0)
    {
        alias dropper = Types;
    }
    else
    {
        alias dropper = AliasSeq!(Types[0 .. pos[0] - rem], dropper!(pos[1 .. $], pos[0] + 1 ,Types[pos[0] - rem + 1 .. $]));
    }
}

/// drops values at a set of position from given array
T[] dropper(T)(int[] pos, int rem, T[] values)
{
    if(pos.length == 0)
        return values;
    else
        return values[0 .. pos[0] - rem] ~ dropper(pos[1 .. $], pos[0] + 1, values[pos[0] - rem + 1 .. $]);
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
    assert(is(dropper!([1, 4], 0, int, long, int, long, byte, float, bool) == AliasSeq!(int, int, long, float, bool)));
    assert(is(dropper!([0, 4], 0, int, long, int, long, double) == AliasSeq!(long, int, long)));
    assert(is(dropper!([1, 3, 5], 0, int, long, int, long, double, float, bool) == AliasSeq!(int, int, double, bool)));
    assert(is(dropper!([0, 2, 3, 5], 0, int, uint, byte, ubyte, long, ulong, bool) == AliasSeq!(uint, long, bool)));
}

// Testing dropper for arrays
unittest
{
    assert(dropper([1, 4], 0, [1, 2, 3, 4, 5, 6]) == [1, 3, 4, 6]);
    assert(dropper([0, 5], 0, [1, 2, 3, 4, 5, 6]) == [2, 3, 4, 5]);
    assert(dropper([0, 3, 5], 0, [1, 2, 3, 4, 5, 6]) == [2, 3, 5]);
}
