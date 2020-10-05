function Merge-DataTableToSQL {
    <#
    .EXAMPLE
    Merge-DataTableToSQL -SQLServer serverName -SQLDB dev -Target dbo.tableName -Source $DataTable
    .PARAMETER Source
    A DataTable that will be merged witht eh target tabled.
    .PARAMETER SQLServer
    Named instances serverName\instanceName | If it's a default instance only use server name, do not add \MSSQLSERVER.
    .PARAMETER SQLDB
    Name of the database where the target table is.
    .PARAMETER Target
    The Table in which you're targeting
    .PARAMETER Filter
    Default="*" Used in selecting the columns from the target.
    .PARAMETER Security
    Default="SSPI" Used in connection string to SQL DB.
    .PARAMETER Timeout
    Default="60" Used in connection string.
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$SQLServer,
        [Parameter(Mandatory = $true)][string]$SQLDB,
        [Parameter(Mandatory = $true)][string]$Target,
        [Parameter(Mandatory = $true)][System.Data.DataTable]$Source,
        [String]$Filter = "*",
        [String]$Security = "SSPI",
        [Int]$Timeout = "60"
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
            $SQLCommand = New-Object System.Data.SqlClient.SqlCommand
            $SQLCommand.Connection = $SQLConnection
            $SQLCommand.CommandText = "SELECT $($Filter) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_Name = '$(($Target.Split(".") | Select -Index 1))'"
            $SQLAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
            $SQLAdapter.SelectCommand = $SQLCommand
            $SQLColumns = New-Object System.Data.DataTable
            $SQLAdapter.Fill($SQLColumns) | Out-Null
            $Columns = $SQLColumns.COLUMN_NAME
            if ($Compare = ((Compare-Object $SQLColumns.COLUMN_NAME $Source.Columns.ColumnName -PassThru) -join ", ")) {
                "DataTable and SQL table contain different columns: $Compare"
            }
            else {

                #What is the primary key of the target table
                $PrimaryKey = New-Object System.Data.DataTable
                $SQLCommand.CommandText = "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_Name = '$($Target.Split(".") | Select -Index 1)' AND CONSTRAINT_NAME LIKE 'PK_%'"
                $SQLAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
                $SQLAdapter.SelectCommand = $SQLCommand
                $SQLAdapter.Fill($PrimaryKey) | Out-Null
                $PrimaryKey = $PrimaryKey | Where-Object { $_.CONSTRAINT_NAME -like 'PK_*' } | Select -ExpandProperty COLUMN_NAME -First 1
                if ($PrimaryKey -eq $null) {
                    "SQL Table does not have primary key"
                }
                else {
                    #Create temporary table for bulk insert
                    $TempTable = $Target + "_TEMP_" + (Get-Random)
                    $CreateColumns = ($CreateColumns = foreach ($Column in ($Columns | Where-Object { $_ -ne $PrimaryKey })) { "[" + $Column + "] [nvarchar] (max) NULL" }) -join ","
                    $SQLQuery = "CREATE TABLE $($TempTable)([$($PrimaryKey)] [nvarchar](255) NOT NULL PRIMARY KEY, $CreateColumns)"
                    $SQLCommand.CommandText = $SQLQuery
                    $Results = $SQLCommand.ExecuteNonQuery()
                    if ($Results -ne -1) { "Unable to create temp table $($TempTable)" }
                    else {

                        #Bulk insert source DataTable into temporary SQL table
                        $SQLBulkCopy = New-Object ("System.Data.SqlClient.SqlBulkCopy") $SQLConnection
                        $SQLBulkCopy.DestinationTableName = "$($TempTable)"
                        $SQLBulkCopy.BatchSize = 5000
                        $SQLBulkCopy.BulkCopyTimeout = 0
                        #Changed the following row from $Source.columns.columnname to $Columns
                        foreach ($Column in $Columns) { [void]$SQLBulkCopy.ColumnMappings.Add($Column, $Column) }
                        $SQLBulkCopy.WriteToServer($Source)

                        #Build and execute SQL merge command
                        $Updates = (($Updates = foreach ($Column in $Columns -ne $PrimaryKey) {
                                    "Target.[$($Column)]" + " = " + ("Source.[$($Column)]")
                                }) -join ",")
                        $InsertColumns = ($InsertColumns = foreach ($Column in $Columns) { "[$Column]" }) -join ","
                        $InsertValues = ($InsertValues = foreach ($Column in $Columns) { "Source.[$Column]" }) -join ","
                        $SQLQuery = @"
MERGE INTO $($Target) AS Target
USING $($TempTable) AS Source
ON Target.[$($PrimaryKey)] = Source.[$($PrimaryKey)]
WHEN MATCHED THEN
    UPDATE SET $Updates
WHEN NOT MATCHED THEN
    INSERT ($InsertColumns) VALUES ($InsertValues);
"@
                        $SQLCommand.CommandText = $SQLQuery 
                        $Results = $SQLCommand.ExecuteNonQuery()
                        "$Results rows affected"
                        #Drop temporary table
                        $SQLCommand.CommandText = "DROP TABLE $($TempTable)"
                        $Results = $SQLCommand.ExecuteNonQuery()
                        if ($Results -ne -1) {
                            "Unable to DROP TABLE $($TempTable)"
                        }
                        #End of create temporary table
                    }
                    #End of require primary key
                }
                #End of compare DataTable and SQL Table columns
            }
        }
    }
    end {
        #Cleanup temp table if not already dropped and close connection
        if ($SQLConnection.State -eq [Data.ConnectionState]::Closed) {
            $SQLConnection = New-Object System.Data.SqlClient.SqlConnection
            $SQLConnection.ConnectionString = "Server=$SQLServer;Database=$SQLDB;Integrated Security=$Security;Connection Timeout=$Timeout"
            $SQLConnection.Open()
        }
        $SQLCommand = New-Object System.Data.SqlClient.SqlCommand
        $SQLCommand.Connection = $SQLConnection
        $SQLCommand.CommandText = "Select * FROM $($TempTable)"
        try {
            $Results = $SQLCommand.ExecuteNonQuery()
            if ($Results -eq -1) {
                $SQLCommand.CommandText = "DROP TABLE $($TempTable)"
                $Results = $SQLCommand.ExecuteNonQuery()
                if ($Results -ne -1) {
                    "Unable to DROP TABLE $($TempTable)"
                }
            }
        }
        catch {}
        if ($SQLConnection.State -eq [Data.ConnectionState]::Open) {
            $SQLConnection.Close()
        }
    }
}