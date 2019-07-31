module magpie.ops;

import std.algorithm: map, reduce;

double count(T)(T[] arr)
{
    import std.algorithm: sum;
    static if(__traits(isArithmetic, T))
        return cast(double)sum(arr);
    else
        return double.init;
}

double max(T)(T[] arr)
{
    import std.algorithm: max;
    static if(__traits(isArithmetic, T))
        return cast(double)arr.reduce!max;
    else
        return double.init;
}

double min(T)(T[] arr)
{
    import std.algorithm: min;
    static if(__traits(isArithmetic, T))
        return cast(double)arr.reduce!min;
    else
        return double.init;
}

double mean(T)(T[] arr)
{
    import std.algorithm: mean;
    static if(__traits(isArithmetic, T))
        return cast(double)mean(arr);
    else
        return double.init;
}

double median(T)(T[] arr)
{
    import std.algorithm: sort;
    import std.array: array;
    static if(__traits(isArithmetic, T))
    {
        auto sortedarr = arr.map!((a) => a).array().sort();
        if(sortedarr.length % 2 == 0)
            return cast(double)(sortedarr[$ / 2] + sortedarr[$ / 2 - 1])/2.0;
        else
            return cast(double)sortedarr[sortedarr.length / 2];
    }
    else
        return double.init;
}

unittest
{
    int[] arr = [1, 2, 3, 4, 5, 6];
    assert(count(arr) == 21);
    assert(max(arr) == 6);
    assert(min(arr) == 1);
    assert(mean(arr) == 3.5);
    assert(median(arr) == 3.5);
}

unittest
{
    import std.math: approxEqual;
    double[] arr = [-3.2, -1.6, -0.7, 0.7, 1.6, 3.2];
    assert(count(arr) == 0);
    assert(max(arr) == 3.2);
    assert(min(arr) == -3.2);
    assert(approxEqual(mean(arr), 0, 1e-10));
    assert(median(arr) == 0);
}
