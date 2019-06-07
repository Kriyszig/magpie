module magpie.helper;

import std.meta: AliasSeq, Repeat;

/// Build DataFrame argument list
template getArgsList(args...)
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

unittest
{
    string[] indx = ["b", "c", "a", "d"];
    int[] codes = [0, 1, 2, 3];
    sortIndex(indx, codes);
    assert(indx == ["a", "b", "c", "d"]);
    assert(codes == [1, 2, 0, 3]);
}