let
    // ===== CONFIGURATION =====
    SampleSize = 500,
    // Rows to sample per column for type detection
    // Could make sense for SampleSize to be some percentage of total rows? - 2025-11-05-134411
    Tolerance = 0.01,
    // Allow 1% non-conforming values (handles dirty data)
    // ===== DATA LOADING =====
    Source = Csv.Document(
        Web.Contents("https://fulcrumapp.io/share/aab9db8f4ed411a41ebb/csv"),
        [
            Delimiter = ",",
            Encoding = 65001,
            QuoteStyle = QuoteStyle.Csv
        ]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    // ===== NORMALIZE BLANKS =====
    // Convert empty strings to null so they don't interfere with type detection
    #"BlanksToNull" = Table.TransformColumns(
        #"Promoted Headers",
        List.Transform(
            Table.ColumnNames(#"Promoted Headers"),
            each {_, (v) => if v is text and Text.Trim(v) = "" then null else v, type nullable any}
        )
    ),
    // ===== TYPE DETECTION FUNCTION =====
    DetectType = (tbl as table, col as text) as type =>
        let
            vals = List.FirstN(Table.Column(tbl, col), SampleSize),
            nonNull = List.RemoveNulls(vals),
            total = List.Count(nonNull),
            // Try parsing as numbers
            nums = List.RemoveNulls(List.Transform(nonNull, each try Number.From(_) otherwise null)),
            numShare = if total = 0 then 0 else Number.From(List.Count(nums)) / total,
            isNumeric = numShare >= (1 - Tolerance),
            // Check if integers (whole numbers)
            intShare =
                if isNumeric then
                    Number.From(List.Count(List.Select(nums, each Number.Mod(_, 1) = 0))) / List.Count(nums)
                else
                    0,
            // Determine type
            outType =
                if total = 0 then
                    type text
                else if isNumeric and intShare >= (1 - Tolerance) then
                    Int64.Type
                else if isNumeric then
                    type number
                else
                    type text
        in
            outType,
    // ===== CLEAN DATA BEFORE TYPING =====
    Cols = Table.ColumnNames(#"BlanksToNull"),
    TypePairs = List.Transform(Cols, each {_, DetectType(#"BlanksToNull", _)}),
    // Convert non-numeric values to null for numeric columns
    #"Cleaned Data" = Table.TransformColumns(
        #"BlanksToNull",
        List.Transform(
            TypePairs,
            (pair) =>
                {
                    pair{0},
                    (value) =>
                        if pair{1} = Int64.Type or pair{1} = type number then
                            try Number.From(value) otherwise null
                        else
                            value
                }
        )
    ),
    // ===== APPLY TYPES =====
    #"Changed Type" = Table.TransformColumnTypes(#"Cleaned Data", TypePairs, "en-US"),
    #"Added _created_duration_minutes" = Table.AddColumn(
        #"Changed Type", "_created_duration_minutes", each [_created_duration] / 60
    ),
    #"Changed Type1" = Table.TransformColumnTypes(
        #"Added _created_duration_minutes", {{"_created_duration_minutes", type duration}}
    )
in
    #"Changed Type1"
