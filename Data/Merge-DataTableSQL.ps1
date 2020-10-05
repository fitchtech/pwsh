function Push-DataTableSQL ($SQLServer, $SQLDB, $Target, $Source) {

    if ($null -eq $Security) { $Security = "SSPI" }
    if ($null -eq $Timeout) { $Timeout = "60" }
    if ($null -eq $SQLServer -or $null -eq $SQLDB -or $null -eq $Target) {
        "Parameters cannot be null values"
    }
    if (($Source.GetType()).Name -ne "DataTable") {
        "Source must be a DataTable object"
    }
    else {
        #Create connection object to SQL instance in one isnt already open
        if ($SQLConnection.State -ne [Data.ConnectionState]::Open) {
            $SQLConnection = New-Object System.Data.SqlClient.SqlConnection
            $SQLConnection.ConnectionString = "Server=$SQLServer;Database=$SQLDB;Integrated Security=$Security;Connection Timeout=$Timeout"
            $SQLConnection.Open()
        }
        if ($SQLConnection.State -ne [Data.ConnectionState]::Open) { "Connection to SQL DB not open" }
        else {
            $SQLBulkCopy = New-Object ("System.Data.SqlClient.SqlBulkCopy") $SQLConnection
            $SQLBulkCopy.DestinationTableName = $Target
            $SQLBulkCopy.BatchSize = 5000
            $SQLBulkCopy.BulkCopyTimeout = 0
            foreach ($Column in $Source.columns.columnname) { [void]$SQLBulkCopy.ColumnMappings.Add($Column, $Column) }
            $SQLBulkCopy.WriteToServer($Source)
        }
        if ($SQLConnection.State -eq [Data.ConnectionState]::Open) { $SQLConnection.Close() }
    }
}

function Merge-SQLDataTable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$SQLServer,
        [Parameter(Mandatory = $true)][string]$SQLDB,
        [Parameter(Mandatory = $true)][string]$Target,
        [Parameter(Mandatory = $true)][System.Data.DataTable]$Source,
        [Parameter][String]$Filter = "*",
        [Parameter][String]$Security = "SSPI",
        [Parameter][Int]$Timeout = "60"
    )

    Begin {
        #Create connection object to SQL instance
        $SQLConnection = New-Object System.Data.SqlClient.SqlConnection
        $SQLConnection.ConnectionString = "Server=$SQLServer;Database=$SQLDB;Integrated Security=$Security;Connection Timeout=$Timeout"
        $SQLConnection.Open()
    }
    Process {
        if ($SQLConnection.State -ne [Data.ConnectionState]::Open) { "Connection to SQL DB not open" }
        else {

            #Get columns for table in SQL and compare to column in source DataTable
            $SQLColumns = New-Object System.Data.DataTable
            $SQLCommand = New-Object System.Data.SqlClient.SqlCommand
            $SQLCommand.Connection = $SQLConnection
            $SQLCommand.CommandText = "SELECT $($Filter) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_Name = '$(($Target.Split(".") | Select-Object -Index 1))'"
            $SQLAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
            $SQLAdapter.SelectCommand = $SQLCommand
            $SQLAdapter.Fill($SQLColumns) | Out-Null
            $Columns = $SQLColumns.COLUMN_NAME
            if ($Compare = ((Compare-Object $SQLColumns.COLUMN_NAME $DataTable.Columns.ColumnName -PassThru) -join ", ")) {
                "DataTable and SQL table contain different columns: $Compare"
            }
            else {

                #What is the primary key of the target table
                $PrimaryKey = New-Object System.Data.DataTable
                $SQLCommand.CommandText = "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_Name = '$($Target.Split(".") | Select-Object -Index 1)' AND CONSTRAINT_NAME LIKE 'PK_%'"
                $SQLAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
                $SQLAdapter.SelectCommand = $SQLCommand
                $SQLAdapter.Fill($PrimaryKey) | Out-Null
                if ($null -ne $PrimaryKey) {
                    $PrimaryKey = $PrimaryKey | Where-Object { $_.CONSTRAINT_NAME -like 'PK_*' } | Select-Object -ExpandProperty COLUMN_NAME
                }

                #Create temporary table for bulk insert
                $CreateColumns = ($CreateColumns = foreach ($Column in ($Columns | Where-Object { $_ -ne $PrimaryKey })) { "[" + $Column + "] [nvarchar] (max) NULL" }) -join ","
                $SQLQuery = "CREATE TABLE $($Target)_TEMP($PrimaryKey [nvarchar](255) NOT NULL PRIMARY KEY, $CreateColumns)"
                $SQLCommand = New-Object System.Data.SqlClient.SqlCommand
                $SQLCommand.Connection = $SQLConnection
                $SQLCommand.CommandText = $SQLQuery
                $Results = $SQLCommand.ExecuteNonQuery()
                if ($Results -ne -1) { "Unable to create temp table $($Target)_TEMP" }
                else {

                    #Bulk insert source DataTable into temporary SQL table
                    Push-DataTableSQL -SQLServer $SQLServer -SQLDB $SQLDB -Target "$($Target)_TEMP" -Source $DataTable 

                    #Build and execute SQL merge command
                    $Updates = (($Updates = foreach ($Column in $Columns -ne $PrimaryKey) {
                                "Target.[$($Column)]" + " = " + ("Source.[$($Column)]")
                            }) -join ",")
                    $InsertColumns = ($InsertColumns = foreach ($Column in $Columns) { "[$Column]" }) -join ","
                    $InsertValues = ($InsertValues = foreach ($Column in $Columns) { "Source.[$Column]" }) -join ","
                    $SQLQuery = @"
MERGE INTO $($Target) AS Target
USING $($Target)_TEMP AS Source
ON Target.$($PrimaryKey) = Source.$($PrimaryKey)
WHEN MATCHED THEN
    UPDATE SET $Updates
WHEN NOT MATCHED THEN
    INSERT ($InsertColumns) VALUES ($InsertValues);
"@
                    $SQLCommand.CommandText = $SQLQuery 
                    $Results = $SQLCommand.ExecuteNonQuery()
                    "$Results rows affected"

                    #Drop temporary table
                    $SQLCommand.CommandText = "DROP TABLE $($Target)_TEMP"
                    $Results = $SQLCommand.ExecuteNonQuery()
                    if ($Results -ne -1) {
                        "Unable to DROP TABLE $($Target)_TEMP"
                    }

                    #Close connection to SQL Server
                    if ($SQLConnection.State -eq [Data.ConnectionState]::Open) {
                        $SQLConnection.Close()
                    }
                    #End of create temporary table
                }
                #End of compare DataTable and SQL Table columns
            }
            #End of if SQL connection open
        }
    }
}