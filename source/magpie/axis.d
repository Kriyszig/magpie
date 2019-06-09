module magpie.axis;

/++
Structure to return an entire row or column of DataFrame.
The operations on this structure enables column/row binary operations.
+/
struct Axis(T...)
{
    static if(T.length == 1)
        alias AxisType = T[0];
    else
        alias AxisType = T;

    /// Data being returned by the DataFrame
    AxisType data;
}