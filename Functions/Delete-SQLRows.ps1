function Remove-SQLRows { #($SQLServer, $SQLDB, $Target, $Source)
    <#
    .EXAMPLE
    Delete-SQLRows -SQLServer serverName -SQLDB dev -Target dbo.tableName -Where "WHERE [Collection Timestamp] <= '5/19/2015 8:50:07 AM'"
    .PARAMETER Source
    A DataTable that will be merged witht eh target tabled.
    .PARAMETER SQLServer
    Named instances SERVERNAME\INSTANCENAME | If it's a default instance only use server name, do not add \MSSQLSERVER.
    .PARAMETER SQLDB
    Name of the database where the target table is.
    .PARAMETER Target
    The Table in which you're targeting
    .PARAMETER Security
    Default="SSPI" Used in connection string to SQL DB.
    .PARAMETER Timeout
    Default="60" Used in connection string.
#>
    param(
        [Parameter(Mandatory = $true)][string]$SQLServer,
        [Parameter(Mandatory = $true)][string]$SQLDB,
        [Parameter(Mandatory = $true)][string]$Target,
        [String] $Where,
        [String]$Security = "SSPI",
        [Int]$Timeout = "60"
    )
    begin {
        #Create connection object to SQL instance in one isnt already open
        if ($SQLConnection.State -ne [Data.ConnectionState]::Open) {
            $SQLConnection = New-Object System.Data.SqlClient.SqlConnection
            $SQLConnection.ConnectionString = "Server=$SQLServer;Database=$SQLDB;Integrated Security=$Security;Connection Timeout=$Timeout"
            $SQLConnection.Open()
        }
    }
    Process {
        if ($Where -ne $null -and $Where -notlike "*WHERE*") {
            $Where = "WHERE " + $Where
        }
        if ($SQLConnection.State -ne [Data.ConnectionState]::Open) { throw "Connection to SQL DB not open" }
        else {
            $SQLCommand = New-Object System.Data.SqlClient.SqlCommand
            $SQLCommand.Connection = $SQLConnection
            $SQLCommand.CommandText = "DELETE FROM $Target $Where"
            $Results = $SQLCommand.ExecuteNonQuery()
            "$(Get-Date): $Results rows affected"
        }
    }
    end {
        if ($SQLConnection.State -eq [Data.ConnectionState]::Open) { $SQLConnection.Close() }
    }
}