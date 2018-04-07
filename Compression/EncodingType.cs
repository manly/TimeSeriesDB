using System;


namespace TimeSeriesDB
{
    public enum EncodingType {
        /// <summary>
        ///     No compression.
        /// </summary>
        None,
        /// <summary>
        ///     Stores the difference between the current value and the previous one.
        ///     
        ///     ex: [5,10,20,30] -> [5,5,10,10]
        /// </summary>
        Delta,
        /// <summary>
        ///     Stores the difference between the current value and the previous one minus common delta across 16 items frame.
        ///     This stores very efficiently cyclic timestamps.
        ///     Avoid using on signed data. If dealing with signed data, Delta encoding will fare better.
        ///     
        ///     ex: [50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65]      -> [1,50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        ///         [0,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150] -> [10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        ///         [5,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150] -> [5,5,0,5,5,5,5,5,5,5,5,5,5,5,5,5,5]
        /// </summary>
        DeltaDelta,
        /// <summary>
        ///     Stores the 'previous_value XOR current_value'.
        ///     Works well for windowed data.
        /// </summary>
        XOR,
        /// <summary>
        ///     Uses a 1st order DFCM predictor (Differential Finite Context Method).
        ///     This stores efficiently patterned and cyclic data.
        /// </summary>
        DFCM,
    }
}
